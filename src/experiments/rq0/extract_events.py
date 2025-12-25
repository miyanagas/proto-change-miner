from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from lib.config import RQ0Config
from lib.categorize import categorize, should_skip
from lib.git_backend import changed_files_in_commit, list_proto_change_commits
from lib.io_utils import ensure_dir


def extract_events_for_repo(repo_path: Path, cfg: RQ0Config, out_events_jsonl: Path) -> dict[str, int]:
    """
    1行 = 1 proto変更コミット
    repoごとに events/<repo>.jsonl を作る（追記競合を避ける）
    """
    ensure_dir(out_events_jsonl.parent)

    repo = repo_path.name
    proto_commits = list_proto_change_commits(repo_path)

    n_written = 0
    with out_events_jsonl.open("w", encoding="utf-8") as f:
        for c in proto_commits:
            files = changed_files_in_commit(repo_path, c)
            files = [
                p for p in files
                if not should_skip(p, skip_prefixes=cfg.skip_prefixes, skip_suffixes=cfg.skip_suffixes)
            ]

            protobuf_files = [p for p in files if p.lower().endswith(".proto")]
            if not protobuf_files:
                continue

            cochange_files = [p for p in files if not p.lower().endswith(".proto")]
            cochange_objs = [{"path": p, "cat": categorize(p)} for p in cochange_files if categorize(p) != "proto"]

            row = {
                "repo": repo,
                "commit": c,
                "protobuf_files": protobuf_files,
                "cochange_files": cochange_objs,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_written += 1

    return {"repo": repo, "total_proto_tx_any": n_written}
