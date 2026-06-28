from .config import DbEngineConfig
from .mariadb import MariaDbBackend
from .xtdb import XtdbBackend


def get_backend(name: str = "mariadb"):
    normalized_name = name.lower()
    if normalized_name == "mariadb":
        return MariaDbBackend()
    if normalized_name == "xtdb":
        return XtdbBackend()
    if normalized_name == "mssql":
        raise NotImplementedError("MS SQL backend is reserved but not implemented yet")
    raise ValueError(f"Unsupported database backend '{name}'")


def backend_from_config(config: DbEngineConfig):
    return get_backend(config.backend)
