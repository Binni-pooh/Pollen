import os
from pathlib import Path

# 1. Find the project root (one level up from pipeline/) and load the .env file
project_root = Path(__file__).resolve().parent

# 2. Retrieve the path from environment variables (with a sensible default fallback)
DATA_DIR = Path(project_root) / "data"