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
        # Create a dummy clone to push initial commit so master exists
        init_path = TMP_DIR / "init-temp"
        if init_path.exists():
            shutil.rmtree(init_path)
        subprocess.run(["git", "clone", str(origin_path), str(init_path)], check=True)
        (init_path / "README").write_text("Init")
        subprocess.run(["git", "add", "."], cwd=init_path, check=True)
        subprocess.run(
            ["git", "commit", "-m", "Initial commit"], cwd=init_path, check=True
        )
        subprocess.run(["git", "push", "origin", "master"], cwd=init_path, check=True)
        shutil.rmtree(init_path)

        prod_repo_uri = str(prod_path)
        dev_repo_uri = str(dev_path)

        def create_repo(uri, branch="prod"):
            # uri is the work_path, we pass explicit remote_url
            return create_git_config_repo(
                uri, remote_url=str(origin_path), branch=branch
            )

        def sync_push(repo, branch):
            # Implemented in commit()
            pass

        def sync_pull(repo, branch):
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
        # In GitConfigRepo, commit() now pushes!
        # However, for the very first push of master?
        # Standard push origin master works.
        commit_repo(repo)

        # Now master exists on remote. Create prod using local helper.
        # But we want to create prod branch.
        repo.create_branch("prod", from_branch="master")
        repo.switch_branch("prod")
        # Ensure we push the new branch?
        # create_branch might not push.
        # But commit() pushes current branch.
        # If we just created it and made no changes, we might need manual push?
        # Or better: make a change on prod and commit.
        repo.set("feature_x_enabled", b"false")  # trigger dirty check if any?
        # Actually set again to be sure or set something new.
        # Currently dirty check compares to stage.
        # If I switched branch, stage is reloaded.
        # Let's just touch something.
        repo.set("meta", b"init-prod")
        commit_repo(repo)

        # Also ensure master is pushed if it wasn't?
        # The previous commit on master pushed it.
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
            "--remote-url",
            str(origin_path) if args.backend == "git" else "dummy",
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
        # Clone happens automatically in init
        dev_inst = create_repo(dev_repo_uri, branch="prod")
        # Logic:
        # dev_inst init -> clone from origin (master) -> checkout prod (pull from origin).
        # We want to create 'dev' from 'prod'.
        dev_inst.create_branch("dev", from_branch="prod")
        # create_branch locally.
        dev_inst.switch_branch("dev")

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
            "--remote-url",
            str(origin_path) if args.backend == "git" else "dummy",
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
    # commit() pushes to origin automatically now
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
        # We need to merge origin/dev into prod.
        # We can do this in our prod_repo clone.
        # 1. Update our view of remote
        # GitConfigRepo doesn't expose fetch explicitly, but reload() pulls current branch.
        # But we need to merge a different branch.

        # We need to run git merge. GitConfigRepo doesn't have `merge` method yet.
        # We can use subprocess here just for the merge step as it's a "management" operation.
        # OR we add merge support to ConfigRepo?
        # Let's keep subprocess for the specific merge logic as it's complex logic (PR flow etc).

        repo_prod = create_repo(prod_repo_uri, branch="prod")
        path = repo_prod.work_path

        # Fetch origin to get latest dev
        subprocess.run(["git", "fetch", "origin"], cwd=path, check=True)
        # Merge origin/dev
        try:
            subprocess.run(
                ["git", "merge", "origin/dev", "-m", "Merge dev"], cwd=path, check=True
            )
        except subprocess.CalledProcessError:
            print("[WARN] Merge failed? Maybe nothing to merge?")

        # Push prod (using commit() or manual push?)
        # Since we modified the repo via subprocess, usage of repo object might be stale?
        # But if we just merged, we just need to push.
        # repo.commit() only commits if dirty stage. Merge creates a commit usually.
        # So we just need to push.
        subprocess.run(["git", "push", "origin", "prod"], cwd=path, check=True)

    print_info("Promoted changes to 'prod'")

    # 7. Production Update
    run_step(7, "Production Update")
    print_info("Production App should pick up new config...")

    if args.backend == "git":
        # Prod App (running in subprocess) loops reload().
        # reload() now does `git pull`.
        # So it SHOULD pick up the changes we just pushed to origin!
        pass

    time.sleep(10)

    print_info("Demo Complete. Terminating apps.")
    prod_app.terminate()
    dev_app.terminate()

    prod_app.wait()
    dev_app.wait()


if __name__ == "__main__":
    main()
