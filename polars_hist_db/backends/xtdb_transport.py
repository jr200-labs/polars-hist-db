from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import math
import re
from typing import Any, Iterable, Iterator, Optional
from urllib.parse import quote

import polars as pl
import pyarrow as pa
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..config import TableConfig
from ..utils.arrow import require_unique_arrow_field_names
from .config import DbEngineConfig


_XTDB_LAST_SYSTEM_TIME_KEY = "polars_hist_db_xtdb_last_system_time"
_XTDB_RESERVED_IDENTIFIERS = {"flag", "timestamp"}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_flight_sql() -> Any:
    try:
        from adbc_driver_flightsql import dbapi as flight_sql
    except ImportError as exc:
        raise RuntimeError(
            "XTDB ADBC support requires the 'xtdb' extra "
            "(adbc-driver-flightsql). Install with polars-hist-db[xtdb]."
        ) from exc
    return flight_sql


def _create_xtdb_engine(config: DbEngineConfig, engine_factory: Any) -> Engine:
    database = config.database or "xtdb"
    auth = ""
    if config.username:
        auth = quote(config.username, safe="")
        if config.password:
            auth = f"{auth}:{quote(config.password, safe='')}"
        auth = f"{auth}@"
    url = f"postgresql+psycopg://{auth}{config.hostname}:{config.port}/{database}"
    engine = engine_factory(
        url,
        connect_args={"prepare_threshold": None},
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        use_native_hstore=False,
    )
    # XTDB pgwire currently trips SQLAlchemy's PostgreSQL dialect when it
    # probes SHOW standard_conforming_strings. XTDB uses standard strings,
    # so skip that PostgreSQL-specific initialization query.
    engine.dialect._set_backslash_escapes = lambda connection: setattr(  # type: ignore[attr-defined, method-assign]
        engine.dialect, "_backslash_escapes", False
    )
    return engine


@contextmanager
def _xtdb_connection_scope(engine: Engine) -> Iterator[Any]:
    """Open a scope for XTDB stores, which manage their own transactions."""
    with engine.connect() as connection:
        yield connection
        connection.commit()


def _xtdb_adbc_uri(config: DbEngineConfig) -> str:
    adbc_port = config.adbc_port or 9832
    return f"grpc://{config.hostname}:{adbc_port}"


def _create_xtdb_adbc_connection(config: DbEngineConfig, flight_loader: Any) -> Any:
    return flight_loader().connect(_xtdb_adbc_uri(config), autocommit=True)


def _close_xtdb_adbc_connection(connection: Any | None) -> None:
    if connection is not None:
        connection.close()


def __getattr__(name: str) -> Any:
    """Keep legacy private XTDB imports working during the split."""
    from . import xtdb_arrow, xtdb_dataframe, xtdb_query

    try:
        return getattr(xtdb_arrow, name)
    except AttributeError:
        try:
            return getattr(xtdb_query, name)
        except AttributeError:
            return getattr(xtdb_dataframe, name)


def _is_xtdb_adbc_ingest_unavailable(exc: Exception) -> bool:
    message = str(exc).lower()
    class_name = exc.__class__.__name__
    return (
        class_name in {"NotImplementedError", "NotSupportedError"}
        and "executeingest" in message
        and ("not implemented" in message or "not_implemented" in message)
    )


def _is_xtdb_table_not_found_error(exc: Exception) -> bool:
    return "Table not found:" in str(exc)


def _execute_xtdb_transaction(connection: Any, statements: Iterable[str]) -> None:
    """Submit one serialized XTDB DML transaction."""

    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        raise ValueError("XTDB transactions require a live DBAPI connection")
    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    driver_connection.execute("BEGIN READ WRITE")
    try:
        for statement in statements:
            driver_connection.execute(statement)
        driver_connection.execute("COMMIT")
    except Exception:
        driver_connection.execute("ROLLBACK")
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit


def _validate_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsupported XTDB identifier: {identifier!r}")
    return identifier


def _quote_identifier(identifier: str) -> str:
    if (
        _IDENTIFIER_RE.match(identifier)
        and identifier == identifier.lower()
        and identifier not in _XTDB_RESERVED_IDENTIFIERS
    ):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def _xtdb_column_identifier(column_name: str) -> str:
    from .xtdb_arrow import _xtdb_physical_column_name

    return _quote_identifier(_xtdb_physical_column_name(column_name))


def _xtdb_sql_literal(value: Any, cast_type: str) -> str:
    if value is None:
        return f"NULL::{cast_type}"
    if isinstance(value, bool):
        return f"{'TRUE' if value else 'FALSE'}::{cast_type}"
    if isinstance(value, int):
        return f"{value}::{cast_type}"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("XTDB insert does not support non-finite float values")
        return f"{value}::{cast_type}"
    if isinstance(value, Decimal):
        return f"{value}::{cast_type}"
    if isinstance(value, datetime):
        escaped = value.isoformat().replace("'", "''")
        return f"'{escaped}'::{cast_type}"
    if isinstance(value, date):
        escaped = value.isoformat().replace("'", "''")
        return f"'{escaped}'::{cast_type}"
    if isinstance(value, bytes):
        return f"X('{value.hex()}')::{cast_type}"

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'::{cast_type}"


def _qualified_table_name(table_schema: str, table_name: str) -> str:
    return f"{_validate_identifier(table_schema)}.{_validate_identifier(table_name)}"


def _driver_connection(connection: Any) -> Any | None:
    proxied_connection = getattr(connection, "connection", None)
    return getattr(proxied_connection, "driver_connection", None)


def _xtdb_timestamp_literal(value: datetime) -> str:
    escaped = value.isoformat().replace("'", "''")
    timestamp_type = "TIMESTAMP WITH TIME ZONE" if value.tzinfo else "TIMESTAMP"
    return f"{timestamp_type} '{escaped}'"


def _next_xtdb_system_time(connection: Any, system_time: datetime) -> datetime:
    info = getattr(connection, "info", None)
    if not isinstance(info, dict):
        return system_time

    last_system_time = info.get(_XTDB_LAST_SYSTEM_TIME_KEY)
    if isinstance(last_system_time, datetime) and system_time <= last_system_time:
        system_time = last_system_time + timedelta(microseconds=1)
    info[_XTDB_LAST_SYSTEM_TIME_KEY] = system_time
    return system_time


def _is_xtdb_invalid_system_time_error(exc: Exception) -> bool:
    message = str(exc)
    return "invalid-system-time" in message or "specified system-time older" in message


def _rollback_xtdb_connection(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _configure_xtdb_pgwire_parameter_adapters(driver_connection: Any) -> None:
    adapters = getattr(driver_connection, "adapters", None)
    register_dumper = getattr(adapters, "register_dumper", None)
    if not callable(register_dumper):
        return

    try:
        from psycopg.types.string import StrDumper
    except ModuleNotFoundError:
        return

    register_dumper(str, StrDumper)


def _xtdb_parameter_value(value: Any, cast_type: str) -> Any:
    if (
        isinstance(value, datetime)
        and cast_type.upper().startswith("TIMESTAMP")
        and value.tzinfo is not None
    ):
        value = value.astimezone(timezone.utc)
        if "WITH TIME ZONE" not in cast_type.upper():
            value = value.replace(tzinfo=None)
    return value


def _normalize_xtdb_timestamp_columns(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
) -> pl.DataFrame:
    from .xtdb_arrow import _xtdb_insert_casts

    casts = _xtdb_insert_casts(df, table_config)
    expressions = []
    for column, cast_type in zip(df.columns, casts, strict=True):
        dtype = df.schema[column]
        if (
            isinstance(dtype, pl.Datetime)
            and dtype.time_zone is not None
            and cast_type.upper().startswith("TIMESTAMP")
        ):
            expression = pl.col(column).dt.convert_time_zone("UTC")
            if "WITH TIME ZONE" not in cast_type.upper():
                expression = expression.dt.replace_time_zone(None)
            expressions.append(expression)
    if not expressions:
        return df
    return df.with_columns(expressions)


def _execute_xtdb_dml(
    connection: Any,
    sql: str,
    rows: list[tuple[Any, ...]] | None = None,
    *,
    system_time: Optional[datetime] = None,
) -> int:
    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        if rows is not None or system_time is not None:
            raise ValueError(
                "XTDB dataframe writes with rows or system-time require a live "
                "DBAPI connection"
            )
        connection.execute(text(sql))
        return 0

    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    begin_sql = "BEGIN READ WRITE"
    if system_time is not None:
        system_time = _next_xtdb_system_time(connection, system_time)
        begin_sql = (
            "BEGIN READ WRITE WITH "
            f"(SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
        )
    driver_connection.execute(begin_sql)
    try:
        if rows is None:
            driver_connection.execute(sql)
            row_count = 0
        else:
            _configure_xtdb_pgwire_parameter_adapters(driver_connection)
            cursor = driver_connection.cursor()
            try:
                cursor.executemany(sql, rows)
            finally:
                close = getattr(cursor, "close", None)
                if callable(close):
                    close()
            row_count = len(rows)
        driver_connection.execute("COMMIT")
    except Exception as exc:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        if system_time is not None and _is_xtdb_invalid_system_time_error(exc):
            return _execute_xtdb_dml(connection, sql, rows, system_time=None)
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit
    return row_count


def _execute_xtdb_arrow_copy(
    connection: Any,
    table_sql: str,
    df: pl.DataFrame,
    *,
    system_time: Optional[datetime] = None,
) -> None:
    from .xtdb_arrow import _normalize_xtdb_ingest_arrow

    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        raise ValueError("XTDB Arrow COPY requires a live DBAPI connection")

    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    begin_sql = "BEGIN READ WRITE"
    if system_time is not None:
        system_time = _next_xtdb_system_time(connection, system_time)
        begin_sql = (
            "BEGIN READ WRITE WITH "
            f"(SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
        )

    arrow_table = _normalize_xtdb_ingest_arrow(df.to_arrow())
    require_unique_arrow_field_names(
        arrow_table.schema, context="XTDB Arrow COPY schema"
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
        writer.write_table(arrow_table)

    driver_connection.execute(begin_sql)
    try:
        cursor = driver_connection.cursor()
        try:
            with cursor.copy(
                f"COPY {table_sql} FROM STDIN WITH (FORMAT 'arrow-stream')"
            ) as copy:
                copy.write(sink.getvalue().to_pybytes())
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()
        driver_connection.execute("COMMIT")
    except Exception as exc:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        if system_time is not None and _is_xtdb_invalid_system_time_error(exc):
            _execute_xtdb_arrow_copy(connection, table_sql, df, system_time=None)
            return
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit
