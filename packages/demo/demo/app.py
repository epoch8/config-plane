import time
import argparse
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config_plane.base import ConfigRepo
from config_plane.impl.sql import create_sql_config_repo, Base
# from config_plane.impl.git import create_git_config_repo  # If needed later
# from config_plane.impl.memory import create_memory_config_repo # If needed later


def debug_print(msg: str):
    print(f"[APP] {msg}", file=sys.stderr)


def get_feature_x_status(repo: ConfigRepo) -> str:
    val = repo.get("feature_x_enabled")
    if val == b"true":
        return "Enabled"
    return "Disabled"


def get_theme(repo: ConfigRepo) -> str:
    val = repo.get("theme")
    if val:
        return val.decode("utf-8").capitalize()
    return "Default"


def main():
    parser = argparse.ArgumentParser(description="Demo App")
    parser.add_argument("--repo-uri", required=True, help="URI/Path to config repo")
    parser.add_argument("--branch", default="master", help="Config branch to use")
    parser.add_argument(
        "--poll-interval", type=int, default=2, help="Poll interval in seconds"
    )
    parser.add_argument("--name", default="App", help="App Instance Name")

    args = parser.parse_args()

    # For SQL backend, repo-uri is database URL
    # We assume SQL backend for this demo as per requirements
    # Initialize infrastructure
    # Add timeout for concurrency
    engine = create_engine(args.repo_uri, connect_args={"timeout": 1})
    Base.metadata.create_all(engine)

    # Enable WAL mode for better concurrency (handled by scenario or ignored if fails)
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")

    SessionLocal = sessionmaker(bind=engine)

    # Ensure tables exist (app shouldn't technically do this, but for demo simplicity)
    # Base.metadata.create_all(engine) # This line is now active above

    debug_print(f"{args.name} starting on branch '{args.branch}'...")

    # Initialize repo once
    repo = create_sql_config_repo(SessionLocal, branch=args.branch)

    while True:
        try:
            # Reload to get fresh state.
            repo.reload()

            feat = get_feature_x_status(repo)
            theme = get_theme(repo)

            print(f"[{args.name}] Feature X: {feat}, Theme: {theme}")
            sys.stdout.flush()

            # repo.session.close() # Keep session open for reuse

        except Exception as e:
            debug_print(f"Error reading config: {e}")

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
