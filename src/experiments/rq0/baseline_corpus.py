"""RQ0: Baseline corpus statistics

全repoの全非mergeコミットを対象に、拡張子別・カテゴリ別のtransaction-occurrence集計を行う。
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Tuple

from lib.categorize import categorize, should_skip
from lib.config import RQ0Config
from lib.git_backend import changed_files_in_commit, list_all_non_merge_commits, list_local_repos, has_proto
from lib.io_utils import ensure_dir, write_csv


# --------------------------------------------------
# データ構造
# --------------------------------------------------
@dataclass
class RepoBaselineResult:
    """repo単位のbaseline集計結果"""
    repo: str
    n_all_tx: int
    cat_tx_counts: Counter = field(default_factory=Counter)
    ext_tx_counts: Counter = field(default_factory=Counter)


def _get_extension(path: str) -> str:
    """ファイルパスから拡張子を取得（なければ 'no_ext'）"""
    p = Path(path)
    ext = p.suffix.lower()
    return ext if ext else "no_ext"


# --------------------------------------------------
# 1コミット処理関数
# --------------------------------------------------
def process_commit(
    repo_path: Path,
    commit: str,
    cfg: RQ0Config,
) -> Tuple[set, set]:
    """
    1コミット内で変更されたファイルの カテゴリ集合 と 拡張子集合 を返す。

    Returns:
        (cats, exts): カテゴリ集合、拡張子集合（重複排除済み）
    """
    files = changed_files_in_commit(repo_path, commit)
    files = [
        f for f in files
        if not should_skip(
            f,
            skip_prefixes=cfg.skip_prefixes,
            skip_suffixes=cfg.skip_suffixes,
            skip_patterns=cfg.skip_patterns,
        )
    ]

    files_set = set(files)
    cats = {categorize(f) for f in files_set}
    exts = {_get_extension(f) for f in files_set}

    return cats, exts


# --------------------------------------------------
# repo単位集計関数
# --------------------------------------------------
def compute_baseline_for_repo(repo_path: Path, cfg: RQ0Config) -> RepoBaselineResult:
    """
    repo単位でbaseline統計を集計する。

    Returns:
        RepoBaselineResult: n_all_tx, cat_tx_counts, ext_tx_counts
    """
    repo_name = repo_path.name
    all_commits = list_all_non_merge_commits(repo_path, max_commits=cfg.max_all_commits)
    n_all_tx = len(all_commits)

    cat_tx_counts: Counter = Counter()
    ext_tx_counts: Counter = Counter()

    for commit in all_commits:
        cats, exts = process_commit(repo_path, commit, cfg)
        # transaction-occurrence: そのコミットに出現した各カテゴリ/拡張子を+1
        cat_tx_counts.update(cats)
        ext_tx_counts.update(exts)

    return RepoBaselineResult(
        repo=repo_name,
        n_all_tx=n_all_tx,
        cat_tx_counts=cat_tx_counts,
        ext_tx_counts=ext_tx_counts,
    )


def _worker(args: Tuple[Path, RQ0Config]) -> RepoBaselineResult:
    """ProcessPoolExecutor用ワーカー"""
    repo_path, cfg = args
    return compute_baseline_for_repo(repo_path, cfg)


# --------------------------------------------------
# 全repo統合 & CSV出力
# --------------------------------------------------
def run_baseline_corpus(cfg: RQ0Config) -> None:
    """
    全repoを処理し、baseline CSVを出力する。

    出力ファイル:
        - repo_baseline.csv
        - category_baseline.csv
        - ext_baseline.csv
        - category_by_repo.csv
        - ext_by_repo.csv
    """
    repos_dir = Path(cfg.repos_dir)
    out_dir = Path(cfg.out_dir) / "rq0" / "baseline"
    ensure_dir(out_dir)

    repos = [r for r in list_local_repos(repos_dir) if has_proto(r)]
    print(f"[baseline_corpus] Found {len(repos)} repos")

    # 並列処理でrepo単位集計
    results: list[RepoBaselineResult] = []
    with ProcessPoolExecutor(max_workers=cfg.workers) as executor:
        futures = {executor.submit(_worker, (repo, cfg)): repo for repo in repos}
        for future in as_completed(futures):
            repo = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"  [done] {result.repo}: {result.n_all_tx} commits")
            except Exception as e:
                print(f"  [error] {repo.name}: {e}")

    if not results:
        print("[baseline_corpus] No results to write")
        return

    # --- 全体集計 ---
    total_cat_counts: Counter = Counter()
    total_ext_counts: Counter = Counter()
    total_n_tx = 0

    for r in results:
        total_cat_counts.update(r.cat_tx_counts)
        total_ext_counts.update(r.ext_tx_counts)
        total_n_tx += r.n_all_tx

    # --- CSV出力 ---

    # 1. repo_baseline.csv
    repo_rows = [
        {
            "repo": r.repo,
            "n_all_tx": r.n_all_tx,
        }
        for r in sorted(results, key=lambda x: x.repo)
    ]
    write_csv(out_dir / "repo_baseline.csv", repo_rows)

    # 2. category_baseline.csv
    cat_rows = [
        {
            "category": cat,
            "tx_count": cnt,
            "tx_ratio": cnt / total_n_tx if total_n_tx > 0 else 0.0,
        }
        for cat, cnt in total_cat_counts.most_common()
    ]
    write_csv(out_dir / "category_baseline.csv", cat_rows)

    # 3. ext_baseline.csv
    ext_rows = [
        {
            "ext": ext,
            "tx_count": cnt,
            "tx_ratio": cnt / total_n_tx if total_n_tx > 0 else 0.0,
        }
        for ext, cnt in total_ext_counts.most_common()
    ]
    write_csv(out_dir / "ext_baseline.csv", ext_rows)

    # 4. category_by_repo.csv
    all_cats = sorted(total_cat_counts.keys())
    cat_by_repo_rows = []
    for r in sorted(results, key=lambda x: x.repo):
        row = {"repo": r.repo, "n_all_tx": r.n_all_tx}
        for cat in all_cats:
            row[f"cat_{cat}"] = r.cat_tx_counts.get(cat, 0)
        cat_by_repo_rows.append(row)
    write_csv(out_dir / "category_by_repo.csv", cat_by_repo_rows)

    # 5. ext_by_repo.csv
    all_exts = sorted(total_ext_counts.keys())
    ext_by_repo_rows = []
    for r in sorted(results, key=lambda x: x.repo):
        row = {"repo": r.repo, "n_all_tx": r.n_all_tx}
        for ext in all_exts:
            row[f"ext_{ext}"] = r.ext_tx_counts.get(ext, 0)
        ext_by_repo_rows.append(row)
    write_csv(out_dir / "ext_by_repo.csv", ext_by_repo_rows)

    print(f"[baseline_corpus] Wrote {len(results)} repos, {total_n_tx} total commits")
    print(f"[baseline_corpus] Output: {out_dir}")


# --------------------------------------------------
# CLI
# --------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="RQ0: Baseline corpus statistics")
    parser.add_argument("--repos_dir", type=str, required=True, help="Path to repos directory")
    parser.add_argument("--out_dir", type=str, required=True, help="Path to output directory")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--max_all_commits", type=int, default=None, help="Max commits per repo (for testing)")
    args = parser.parse_args()

    cfg = RQ0Config(
        repos_dir=args.repos_dir,
        out_dir=args.out_dir,
        workers=args.workers,
        max_all_commits=args.max_all_commits,
    )

    run_baseline_corpus(cfg)


if __name__ == "__main__":
    main()
