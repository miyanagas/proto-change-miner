from __future__ import annotations

import argparse
from dataclasses import asdict
from multiprocessing import Pool, cpu_count
from pathlib import Path

from lib.config import RQ0Config
from lib.git_backend import list_local_repos, has_proto
from lib.io_utils import write_csv, ensure_dir
from experiments.rq0.extract_events import extract_events_for_repo
from experiments.rq0.compute_pairs import compute_pairs_for_repo, RepoPairResult
from experiments.rq0.merge_outputs import merge_repo_csvs
from experiments.rq0.schema import REPO_SUMMARY_COLUMNS


def _process_one(repo_path_str: str, cfg_dict: dict, out_dir_str: str) -> dict:
    repo_path = Path(repo_path_str)
    cfg = RQ0Config(**cfg_dict)
    out_dir = Path(out_dir_str)

    repo = repo_path.name

    # 出力先（repoごと）
    events_path = out_dir / "rq0" / "events" / f"{repo}.jsonl"
    pairs_dir = out_dir / "rq0" / "pairs" / f"repo={repo}"
    pairs_csv = pairs_dir / "pairs.csv"

    ensure_dir(pairs_dir)

    # events（材料）
    extract_events_for_repo(repo_path, cfg, events_path)

    # pairs（探索結果）
    res: RepoPairResult = compute_pairs_for_repo(repo_path, cfg)
    write_csv(pairs_csv, res.rows)

    return {
        "repo": repo,
        "n_all_tx": res.n_all_tx,
        "n_proto_tx_any": res.n_proto_tx_any,
        "n_unique_proto_files": res.n_unique_proto_files,
        "n_unique_cochange_files": res.n_unique_cochange_files,
        "n_pairs": res.n_pairs,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos_dir", type=Path, required=True)
    ap.add_argument("--out_dir", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=min(cpu_count(), 8))
    ap.add_argument("--max_all_commits", type=int, default=None)  # デバッグ用
    ap.add_argument("--merge_all", action="store_true", help="all_pairs.csv を作る")
    args = ap.parse_args()

    cfg = RQ0Config(
        repos_dir=str(args.repos_dir),
        out_dir=str(args.out_dir),
        workers=args.workers,
        max_all_commits=args.max_all_commits,
    )

    repos = [r for r in list_local_repos(args.repos_dir) if has_proto(r)]
    ensure_dir(args.out_dir / "rq0" / "events")
    ensure_dir(args.out_dir / "rq0" / "pairs")

    tasks = [(str(r), cfg.__dict__, str(args.out_dir)) for r in repos]

    summaries: list[dict] = []
    errors: list[dict] = []

    with Pool(processes=cfg.workers) as pool:
        for summary in pool.starmap(_process_one, tasks):
            summaries.append(summary)

    # repo summary
    summary_csv = args.out_dir / "rq0" / "repo_summary.csv"
    write_csv(summary_csv, summaries)

    # optional merge
    if args.merge_all:
        all_pairs_csv = args.out_dir / "rq0" / "all_pairs.csv"
        merge_repo_csvs(args.out_dir / "rq0" / "pairs", all_pairs_csv)
        print(f"[OK] merged: {all_pairs_csv}")

    print(f"[OK] wrote: {summary_csv}")
    print(f"[OK] events dir: {args.out_dir / 'rq0' / 'events'}")
    print(f"[OK] pairs dir: {args.out_dir / 'rq0' / 'pairs'}")


if __name__ == "__main__":
    main()
