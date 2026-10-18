"""python -m bot [--once]"""
from __future__ import annotations

import sys

from bot.runner import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
