"""
End-to-end pipeline runner for personal finance project.

This script automates the complete workflow:
1. Load Nordea CSV data into DuckDB
2. Run dbt seed and models
3. Launch Streamlit dashboard

Usage:
    python run.py [--skip-load] [--skip-dbt]
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str, cwd: Path | None = None) -> None:
    """Run a command and handle errors."""
    print(f"\n{'=' * 60}")
    print(f"📍 {description}")
    print(f"{'=' * 60}\n")

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            check=True,
            text=True,
            capture_output=False,
        )
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error: {description} failed with exit code {e.returncode}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"\n❌ Error: Command not found: {cmd[0]}")
        print("Make sure all dependencies are installed (run: uv sync)")
        sys.exit(1)


def main() -> None:
    """Run the complete pipeline."""
    parser = argparse.ArgumentParser(
        description="Run the complete finance data pipeline"
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip loading CSV data (use existing data)",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt transformations (use existing models)",
    )
    args = parser.parse_args()

    root_dir = Path(__file__).parent
    dbt_dir = root_dir / "dbt_project"
    data_dir = root_dir / "data"
    db_path = data_dir / "db" / "finance.duckdb"

    print("🚀 Finance Pipeline Runner")
    print(f"Working directory: {root_dir}")

    # Step 1: Load CSV data
    if not args.skip_load:
        csv_dir = data_dir / "raw" / "nordea"
        if csv_dir.exists() and list(csv_dir.glob("*.csv")):
            run_command(
                [sys.executable, "scripts/load_nordea.py"],
                "Step 1: Loading Nordea CSV data into DuckDB",
                cwd=root_dir,
            )
        else:
            print(f"\n⚠️  Warning: No CSV files found in {csv_dir}")
            print("Skipping data load step. Using existing data if available.")
    else:
        print("\n⏭️  Skipping data load (--skip-load flag)")

    # Check if database exists
    if not db_path.exists():
        print(f"\n❌ Error: Database not found at {db_path}")
        print("Please ensure CSV data is loaded first.")
        sys.exit(1)

    # Step 2: Run dbt
    if not args.skip_dbt:
        # Check if dbt is available
        try:
            subprocess.run(
                ["dbt", "--version"],
                capture_output=True,
                check=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("\n❌ Error: dbt not found")
            print("Please install dependencies: uv sync")
            sys.exit(1)

        # Run dbt seed
        run_command(
            ["dbt", "seed"],
            "Step 2a: Loading dbt seed data (category mappings)",
            cwd=dbt_dir,
        )

        # Run dbt models
        run_command(
            ["dbt", "run"],
            "Step 2b: Running dbt models (transformations)",
            cwd=dbt_dir,
        )

        # Run dbt tests
        run_command(
            ["dbt", "test"],
            "Step 2c: Running dbt tests",
            cwd=dbt_dir,
        )
    else:
        print("\n⏭️  Skipping dbt transformations (--skip-dbt flag)")

    # Step 3: Launch Streamlit dashboard
    print(f"\n{'=' * 60}")
    print("📊 Step 3: Launching Streamlit dashboard")
    print(f"{'=' * 60}\n")
    print("Dashboard will open in your browser at http://localhost:8501")
    print("Press Ctrl+C to stop the dashboard\n")

    try:
        subprocess.run(
            ["streamlit", "run", "scripts/dashboard.py"],
            cwd=root_dir,
            check=True,
        )
    except KeyboardInterrupt:
        print("\n\n👋 Dashboard stopped. Goodbye!")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error: Dashboard failed with exit code {e.returncode}")
        sys.exit(1)
    except FileNotFoundError:
        print("\n❌ Error: streamlit command not found")
        print("Please install dependencies: uv sync")
        sys.exit(1)


if __name__ == "__main__":
    main()
