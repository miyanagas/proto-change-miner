from __future__ import annotations

from pathlib import Path
import csv

from lib.io_utils import ensure_dir


def merge_repo_csvs(pairs_dir: Path, out_csv: Path) -> None:
    """
    pairs/repo=<repo>/pairs.csv を 1つの all_pairs.csv に結合
    """
    ensure_dir(out_csv.parent)

    csv_files = sorted(pairs_dir.glob("repo=*/pairs.csv"))
    if not csv_files:
        return

    # ヘッダは最初のファイルから
    with csv_files[0].open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    with out_csv.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(header)

        for fp in csv_files:
            with fp.open("r", encoding="utf-8") as f:
                reader = csv.reader(f)
                _ = next(reader, None)  # skip header
                for row in reader:
                    w.writerow(row)
