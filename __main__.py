"""Allow running RosterIQ as: python -m rosteriq"""

import sys
from rosteriq.cli import main

sys.exit(main() or 0)
