import sys
from pathlib import Path

# Tests run against the source tree, so an editable install is not required for
# `uv run pytest` to work in a fresh checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
