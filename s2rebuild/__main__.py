"""s2rebuild __main__: entry point for python -m s2rebuild.

Author: John Bergamini
"""
import sys

from .cli import main

if __name__ == '__main__':
    sys.exit(main())
