from pathlib import Path


def xtdb_container_args(*ports: int) -> list[str]:
    """Start test servers with explicit interfaces and ports across image updates."""
    config = Path(__file__).resolve().parents[1] / "_data" / "xtdb.yaml"
    return [
        "run",
        "--rm",
        "-d",
        *[arg for port in ports for arg in ("-p", str(port))],
        "-v",
        f"{config}:/etc/xtdb/test.yaml:ro",
        "ghcr.io/xtdb/xtdb:nightly",
        "-f",
        "/etc/xtdb/test.yaml",
    ]
