"""One command runs the whole pipeline: python -m src.run_pipeline --config config/config.yaml.

Must read the config file named on the command line (see src/common/config.py),
run bronze ingestion for all twelve input files, build silver, then build gold,
in that order, on a clean checkout with no other setup beyond
`pip install -r requirements.txt` and a working Java installation.
"""

from __future__ import annotations


def main() -> None:
    raise NotImplementedError


if __name__ == "__main__":
    main()
