import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# app.main creates the analytics database at import time; point it at a
# throwaway file so importing the app never touches odds_database/.
os.environ.setdefault("ANALYTICS_DB_PATH", str(Path(tempfile.mkdtemp()) / "analytics.db"))
