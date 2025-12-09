import time
import argparse
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config_plane.base import ConfigRepo
from config_plane.impl.git import create_git_config_repo
from config_plane.impl.sql import create_sql_config_repo, Base


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
    parser.add_argument(
        "--backend",
        choices=["sql", "git"],
        default="sql",
        help="Backend type (default: sql)",
    )

    parser.add_argument("--remote-url", default=None, help="Remote Git Configuration")

    args = parser.parse_args()

    if args.backend == "sql":
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

        # Initialize repo once
        debug_print(f"{args.name} starting on branch '{args.branch}' (SQL)...")
        repo = create_sql_config_repo(SessionLocal, branch=args.branch)
    else:
        # Git Backend
        debug_print(f"{args.name} starting on branch '{args.branch}' (Git)...")
        # For Git, repo_uri is the path to the repo directory
        if not args.remote_url:
            debug_print("ERROR: --remote-url required for git backend")
            sys.exit(1)
        repo = create_git_config_repo(
            args.repo_uri, remote_url=args.remote_url, branch=args.branch
        )

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
