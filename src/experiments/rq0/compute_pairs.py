from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from lib.config import RQ0Config
from lib.categorize import categorize, should_skip
from lib.git_backend import (
    changed_files_in_commit,
    list_all_non_merge_commits,
    list_proto_change_commits,
)


@dataclass(frozen=True)
class RepoPairResult:
    repo: str
    n_all_tx: int
    n_proto_tx_any: int
    n_unique_proto_files: int
    n_unique_cochange_files: int
    n_pairs: int
    rows: list[dict]


def _filtered_changed_files(repo_path: Path, commit: str, cfg: RQ0Config) -> list[str]:
    files = changed_files_in_commit(repo_path, commit)
    return [
        p for p in files
        if not should_skip(p, skip_prefixes=cfg.skip_prefixes, skip_suffixes=cfg.skip_suffixes, skip_patterns=cfg.skip_patterns)
    ]


def compute_pairs_for_repo(repo_path: Path, cfg: RQ0Config) -> RepoPairResult:
    """
    (proto_file, cochange_file) 全ペアについて、support/confidence/lift を算出する（repo単位）。

    注意:
      - confidence = P(file | proto_file) = n_both_tx / n_proto_tx_for_proto
      - baseline   = P(file)            = n_file_tx / n_all_tx
      - lift       = confidence / baseline
    """
    repo = repo_path.name

    # --- Phase 1: proto変更コミットを走査して、候補と共起を作る ---
    proto_commits = list_proto_change_commits(repo_path)
    n_proto_tx_any = len(proto_commits)

    n_proto_tx_for_proto = Counter()              # proto_file -> #commits(proto_file changed)
    n_both_tx = Counter()                         # (proto_file, file) -> #commits both appear
    candidate_files: set[str] = set()             # 共変更ファイル（baseline計算対象）
    candidate_protos: set[str] = set()

    for c in proto_commits:
        files = _filtered_changed_files(repo_path, c, cfg)

        protos = [p for p in files if p.lower().endswith(".proto")]
        if not protos:
            continue

        cochange = [p for p in files if not p.lower().endswith(".proto")]
        cochange = [p for p in cochange if categorize(p) != "proto"]

        # commit内の重複を排除（「出現したかどうか」をカウントしたい）
        protos_set = set(protos)
        cochange_set = set(cochange)

        for p in protos_set:
            candidate_protos.add(p)
            n_proto_tx_for_proto[p] += 1

        for f in cochange_set:
            candidate_files.add(f)

        for p in protos_set:
            for f in cochange_set:
                n_both_tx[(p, f)] += 1

    # --- Phase 2: baseline P(file) のために全コミットを走査（候補ファイルだけ数える） ---
    all_commits = list_all_non_merge_commits(repo_path, max_commits=cfg.max_all_commits)
    n_all_tx = len(all_commits)

    n_file_tx = Counter()  # file -> #commits(file appears)
    if candidate_files:
        cand = candidate_files
        for c in all_commits:
            files = _filtered_changed_files(repo_path, c, cfg)
            files_set = set(files)
            hit = files_set.intersection(cand)
            for f in hit:
                n_file_tx[f] += 1

    # --- Build rows ---
    rows: list[dict] = []
    for (p, f), both in n_both_tx.items():
        proto_tx = n_proto_tx_for_proto[p]
        file_tx = n_file_tx.get(f, 0)

        support = (both / n_all_tx) if n_all_tx else 0.0
        confidence = (both / proto_tx) if proto_tx else 0.0
        baseline = (file_tx / n_all_tx) if n_all_tx else 0.0
        lift = (confidence / baseline) if baseline > 0 else 0.0

        rows.append({
            "repo": repo,
            "proto_file": p,
            "file": f,
            "file_category": categorize(f),
            "n_all_tx": n_all_tx,
            "n_proto_tx_for_proto": proto_tx,
            "n_file_tx": file_tx,
            "n_both_tx": both,
            "support": support,
            "confidence": confidence,
            "baseline": baseline,
            "lift": lift,
        })

    return RepoPairResult(
        repo=repo,
        n_all_tx=n_all_tx,
        n_proto_tx_any=n_proto_tx_any,
        n_unique_proto_files=len(candidate_protos),
        n_unique_cochange_files=len(candidate_files),
        n_pairs=len(rows),
        rows=rows,
    )
