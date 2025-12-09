import time
import subprocess
import sys
import argparse
import shutil
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config_plane.impl.sql import create_sql_config_repo, Base
from config_plane.impl.git import create_git_config_repo

# Setup paths that are safe to use
TMP_DIR = Path("tmp").absolute()
DB_PATH = TMP_DIR / "demo.db"
DB_URL = f"sqlite:///{DB_PATH}"

GIT_REPO_PATH = TMP_DIR / "demo-config-repo"


def run_step(step_num: int, title: str):
    print(f"\n=== Step {step_num}: {title} ===")
    time.sleep(1)


def print_info(msg: str):
    print(f"[INFO] {msg}")


def commit_repo(repo):
    repo.commit()


def main():
    parser = argparse.ArgumentParser(description="Demo Scenario")
    parser.add_argument(
        "--backend",
        choices=["sql", "git"],
        default="sql",
        help="Backend type (default: sql)",
    )
    args = parser.parse_args()

    # Ensure tmp dir exists
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Cleanup previous runs
    if DB_PATH.exists():
        DB_PATH.unlink()
    # Also cleanup WAL files to prevent IO errors if leftover
    for p in TMP_DIR.glob("demo.db*"):
        try:
            p.unlink()
        except OSError:
            pass

    if GIT_REPO_PATH.exists():
        shutil.rmtree(GIT_REPO_PATH)

    # Initialize Infrastructure & Define Repo Factory
    SessionLocal = None

    # Context info for the run
    prod_repo_uri = ""
    dev_repo_uri = ""

    if args.backend == "sql":
        repo_uri = DB_URL
        prod_repo_uri = repo_uri
        dev_repo_uri = repo_uri

        # Initialize infrastructure
        # Add timeout for concurrency
        engine = create_engine(DB_URL, connect_args={"timeout": 1})
        Base.metadata.create_all(engine)

        # Enable WAL mode for better concurrency
        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode=WAL")

        SessionLocal = sessionmaker(bind=engine)

        def create_repo(uri, branch="prod"):
            return create_sql_config_repo(SessionLocal, branch=branch)

        def sync_push(repo, branch):
            pass  # SQL is shared DB, no push

        def sync_pull(repo, branch):
            repo.reload()  # Just reload

    else:
        # Git Backend Setup
        # Structure:
        #   tmp/demo-repo-origin (Bare)
        #   tmp/demo-repo-prod (Clone 1) -> ProdApp
        #   tmp/demo-repo-dev (Clone 2) -> DevApp

        origin_path = TMP_DIR / "demo-repo-origin"
        prod_path = TMP_DIR / "demo-repo-prod"
        dev_path = TMP_DIR / "demo-repo-dev"

        # Cleanup
        for p in [origin_path, prod_path, dev_path]:
            if p.exists():
                shutil.rmtree(p)

        # 1. Init bare origin
        origin_path.mkdir()
        subprocess.run(
            ["git", "init", "--bare", "--initial-branch=master"],
            cwd=origin_path,
            check=True,
        )

        # 2. Clone Prod
        subprocess.run(["git", "clone", str(origin_path), str(prod_path)], check=True)
        # Configure user/email for commits
        subprocess.run(
            ["git", "config", "user.name", "Demo User"], cwd=prod_path, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "demo@example.com"],
            cwd=prod_path,
            check=True,
        )

        # 3. Clone Dev
        subprocess.run(["git", "clone", str(origin_path), str(dev_path)], check=True)
        subprocess.run(
            ["git", "config", "user.name", "Demo User"], cwd=dev_path, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "demo@example.com"],
            cwd=dev_path,
            check=True,
        )

        prod_repo_uri = str(prod_path)
        dev_repo_uri = str(dev_path)

        def create_repo(uri, branch="prod"):
            # uri is the path
            return create_git_config_repo(uri, branch=branch)

        def sync_push(repo, branch):
            # repo is GitConfigRepo wrapper
            path = repo.repo_path
            # Push current branch to origin
            subprocess.run(["git", "push", "origin", branch], cwd=path, check=True)

        def sync_pull(repo, branch):
            path = repo.repo_path
            subprocess.run(["git", "pull", "origin", branch], cwd=path, check=True)
            repo.reload()

    # 1. Initialize Repository (prod)
    run_step(1, "Initialize Repository")
    print_info(f"Creating {args.backend.upper()} Repo (Prod) at {prod_repo_uri}")

    # Create initial state in Prod Repo
    # Start with master to ensure we have a valid HEAD before creating prod
    repo = create_repo(prod_repo_uri, branch="master")

    # For Git, we need to make sure we are on 'prod'.
    if args.backend == "git":
        # We are on master (empty). create_branch needs a valid commit if branching from specific point,
        # or we just commit to master first.
        repo.set("feature_x_enabled", b"false")
        repo.set("theme", b"light")
        commit_repo(repo)

        # Now master exists. Create prod.
        repo.create_branch("prod", from_branch="master")
        repo.switch_branch("prod")
        sync_push(repo, "prod")  # Push prod
        # Also push master just in case
        sync_push(repo, "master")
    else:
        # SQL: Directly on prod
        repo = create_repo(prod_repo_uri, branch="prod")
        repo.set("feature_x_enabled", b"false")
        repo.set("theme", b"light")
        commit_repo(repo)

    print_info("Initialized 'prod' with Feature X: Disabled, Theme: Light")

    # 2. Start Production App
    run_step(2, "Start Production App")
    prod_app = subprocess.Popen(
        [
            "uv",
            "run",
            "python",
            "-m",
            "demo.app",
            "--repo-uri",
            prod_repo_uri,
            "--branch",
            "prod",
            "--name",
            "ProdApp",
            "--poll-interval",
            "5",
            "--backend",
            args.backend,
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,  # Forward debug prints
        cwd=Path(__file__).parents[3],
    )
    # Give it a moment to start
    time.sleep(3)

    # 3. Start Development Session (Create dev branch)
    run_step(3, "Start Development Session")
    print_info("Creating 'dev' branch from 'prod'")

    if args.backend == "sql":
        # SQL: dev_repo connects to same DB
        dev_repo = create_repo(dev_repo_uri, branch="prod")
        dev_repo.create_branch("dev", from_branch="prod")
    else:
        # Git: We act in the Dev Clone
        dev_inst = create_repo(dev_repo_uri, branch="master")
        # Pull latest prod from origin to base dev off it
        sync_pull(dev_inst, "prod")
        dev_inst.switch_branch("prod")
        dev_inst.create_branch("dev", from_branch="prod")
        dev_inst.switch_branch("dev")  # GitConfigRepo switch checkouts

    run_step(3, "Start Development App (on dev branch)")
    dev_app = subprocess.Popen(
        [
            "uv",
            "run",
            "python",
            "-m",
            "demo.app",
            "--repo-uri",
            dev_repo_uri,
            "--branch",
            "dev",
            "--name",
            "DevApp ",
            "--poll-interval",
            "5",
            "--backend",
            args.backend,
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
        cwd=Path(__file__).parents[3],
    )
    time.sleep(3)

    # 4. Modify Configuration in Dev
    run_step(4, "Modify Configuration (in Dev)")
    print_info("Enabling Feature X and changing Theme to Dark in 'dev'")

    # Use a fresh repo instance to be sure (or reuse)
    dev_repo = create_repo(dev_repo_uri, branch="dev")
    dev_repo.set("feature_x_enabled", b"true")
    dev_repo.set("theme", b"dark")
    if dev_repo.is_dirty():
        print_info("Changes staged...")
    commit_repo(dev_repo)
    # Push dev changes so they are available (optional for local dev app test, but needed for merge later)
    sync_push(dev_repo, "dev")
    print_info("Changes committed to 'dev'")

    print_info("Observing apps...")
    time.sleep(10)

    # 5. Verify Isolation
    run_step(5, "Verify Isolation")
    print_info("Production App should still show old config.")
    # (Visual check from output)

    # 6. Promote to Production
    run_step(6, "Promote to Production")
    print_info("Merging 'dev' into 'prod'")

    # Merge logic
    if args.backend == "sql":
        repo_prod = create_repo(prod_repo_uri, branch="prod")
        repo_dev_read = create_repo(dev_repo_uri, branch="dev")
        val_feat = repo_dev_read.get("feature_x_enabled")
        val_theme = repo_dev_read.get("theme")
        repo_prod.set("feature_x_enabled", val_feat)
        repo_prod.set("theme", val_theme)
        commit_repo(repo_prod)
    else:
        # Git Merge:
        # In Prod Clone: Pull prod (ensure latest) -> Merge dev -> Push prod
        # We need to fetch dev first
        repo_prod = create_repo(prod_repo_uri, branch="prod")
        path = repo_prod.repo_path
        subprocess.run(["git", "fetch", "origin"], cwd=path, check=True)
        # We merge origin/dev into prod
        subprocess.run(
            ["git", "merge", "origin/dev", "-m", "Merge dev"], cwd=path, check=True
        )
        sync_push(repo_prod, "prod")

    print_info("Promoted changes to 'prod'")

    # 7. Production Update
    run_step(7, "Production Update")
    print_info("Production App should pick up new config...")

    if args.backend == "git":
        # Prod App needs to PULL the changes we just pushed to origin!
        # But ProdApp is just running `demo.app` loop which calls `repo.reload()`.
        # `repo.reload()` in GitConfigRepo does NOT pull from remote. It just reloads from local disk/HEAD.
        # Since running in a subprocess, we can't easily force it to pull.
        # However, `demo.app` is running in `prod_repo_uri`.
        # We just updated `prod_repo_uri` (Prod Clone) in the block above (Merge step).
        # So the local disk of Prod Clone IS updated.
        # So `repo.reload()` should pick it up.
        pass

    time.sleep(10)

    print_info("Demo Complete. Terminating apps.")
    prod_app.terminate()
    dev_app.terminate()

    prod_app.wait()
    dev_app.wait()


if __name__ == "__main__":
    main()
