import time
import subprocess
import sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config_plane.impl.sql import create_sql_config_repo, Base

# Setup paths that are safe to use
DB_PATH = Path("demo.db").absolute()
DB_URL = f"sqlite:///{DB_PATH}"


def run_step(step_num: int, title: str):
    print(f"\n=== Step {step_num}: {title} ===")
    time.sleep(1)


def print_info(msg: str):
    print(f"[INFO] {msg}")


def create_repo(session_local, branch="prod"):
    return create_sql_config_repo(session_local, branch=branch)


def commit_repo(repo):
    repo.commit()


def main():
    if DB_PATH.exists():
        DB_PATH.unlink()
    # Also cleanup WAL files to prevent IO errors if leftover
    for p in Path(".").glob("demo.db*"):
        try:
            p.unlink()
        except OSError:
            pass

    # Initialize infrastructure
    # Add timeout for concurrency
    engine = create_engine(DB_URL, connect_args={"timeout": 1})
    Base.metadata.create_all(engine)

    # Enable WAL mode for better concurrency
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")

    SessionLocal = sessionmaker(bind=engine)

    # 1. Initialize Repository (prod)
    run_step(1, "Initialize Repository")
    print_info(f"Creating SQL Repo at {DB_URL}")
    repo = create_repo(SessionLocal, branch="prod")
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
            DB_URL,
            "--branch",
            "prod",
            "--name",
            "ProdApp",
            "--poll-interval",
            "5",
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
    # We use a new repo instance to simulate a dev user
    dev_repo = create_repo(SessionLocal, branch="prod")
    dev_repo.create_branch("dev", from_branch="prod")

    run_step(3, "Start Development App (on dev branch)")
    dev_app = subprocess.Popen(
        [
            "uv",
            "run",
            "python",
            "-m",
            "demo.app",
            "--repo-uri",
            DB_URL,
            "--branch",
            "dev",
            "--name",
            "DevApp ",
            "--poll-interval",
            "5",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
        cwd=Path(__file__).parents[3],
    )
    time.sleep(3)

    # 4. Modify Configuration in Dev
    run_step(4, "Modify Configuration (in Dev)")
    print_info("Enabling Feature X and changing Theme to Dark in 'dev'")

    # Be sure to switch/use dev branch
    dev_repo.switch_branch("dev")
    dev_repo.set("feature_x_enabled", b"true")
    dev_repo.set("theme", b"dark")
    if dev_repo.is_dirty():
        print_info("Changes staged...")
    commit_repo(dev_repo)
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

    # Merge logic:
    # 1. Checkout prod
    # 2. Read dev snapshot
    # 3. Apply changes (In this simple case, we can just point prod branch to dev snapshot, or 'fast-forward')
    # Since config-plane history is linear DAG of snapshots?
    # Our simple implementation:
    # dev_repo is on dev.
    # To merge:
    # repo_prod = create_sql_config_repo(SessionLocal, branch="prod")
    # repo_prod.merge(dev) -- we don't have merge API yet.
    # Manual merge for demo:
    # Read all from dev, write to prod.

    repo_prod = create_repo(SessionLocal, branch="prod")

    # Get dev snapshot
    # We can inspect dev repo to see what's there?
    # Or just simpler:
    # "Fast forward" logic: Update prod branch pointer to point to same snapshot as dev?
    # BranchModel update.
    # But let's do it via Repo API if possible? No generic set_branch_pointer method.
    # So we'll do: read from dev, write to prod.

    # Re-instantiate to be clean
    repo_dev_read = create_repo(SessionLocal, branch="dev")
    val_feat = repo_dev_read.get("feature_x_enabled")
    val_theme = repo_dev_read.get("theme")

    repo_prod.set("feature_x_enabled", val_feat)
    repo_prod.set("theme", val_theme)
    commit_repo(repo_prod)
    print_info("Promoted changes to 'prod'")

    # 7. Production Update
    run_step(7, "Production Update")
    print_info("Production App should pick up new config...")
    time.sleep(10)

    print_info("Demo Complete. Terminating apps.")
    prod_app.terminate()
    dev_app.terminate()

    prod_app.wait()
    dev_app.wait()


if __name__ == "__main__":
    main()
