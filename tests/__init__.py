import sys
from pathlib import Path

# Add src/ to the pythonpath so pytest can find our modules
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
