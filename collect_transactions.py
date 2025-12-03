from __future__ import annotations

from pathlib import Path
from multiprocessing import Pool, cpu_count
from typing import Any

from git import Repo, GitCommandError
import pandas as pd
from tqdm import tqdm


CSV_PATH = "repos_with_protobuf_flag.csv"
REPOS_DIR = Path("repos")
OUTPUT_JSONL = "transactions.jsonl"


def get_no_merge_commits(repo: Repo) -> list[str]:
    commits = list(repo.iter_commits("--all", no_merges=True))
    return [c.hexsha for c in commits]


def get_changed_files_in_commit(repo: Repo, commit_hash: str) -> list[str]:
    commit = repo.commit(commit_hash)

    if not commit.parents:
        paths = sorted(
            {item.path for item in commit.tree.traverse() if getattr(item, "type", None) == "blob"}
        )
        return paths

    parent = commit.parents[0]
    diffs = parent.diff(commit)

    paths: set[str] = set()
    for d in diffs:
        if d.a_path:
            paths.add(d.a_path)
        if d.b_path:
            paths.add(d.b_path)

    return sorted(paths)


def get_commits_touching_file(repo: Repo, path: str) -> list[str]:
    commits = repo.iter_commits("--all", paths=path)
    return [c.hexsha for c in commits]


def collect_transactions(repo_path: Path) -> list[list[str]]:
    repo = Repo(repo_path)
    transactions: list[list[str]] = []

    hashes = get_no_merge_commits(repo)
    for h in hashes:
        changed_files = get_changed_files_in_commit(repo=repo, commit_hash=h)
        if not changed_files:
            continue

        if any(f.endswith(".proto") for f in changed_files):
            transactions.append(changed_files)
    return transactions


def collect_for_repo(repo_url: str) -> list[dict[str, list[str]]]:
    repo_name = repo_url.rstrip("/").split("/")[-1]
    repo_path = REPOS_DIR / repo_name

    if not repo_path.exists():
        print(f"[skip] {repo_name}: {repo_path} does not exist.")
        return []

    try:
        tx_list = collect_transactions(repo_path)
    except GitCommandError as e:
        print(f"[error] {repo_name}: {e}")
        return []

    return [
        {
            "repo": repo_name,
            "transaction": tx,
        }
        for tx in tx_list
    ]


def main():
    df = pd.read_csv(CSV_PATH, sep=",")
    urls = df.loc[df["uses_protobuf"] == True, "URL"].tolist()

    max_procs = cpu_count()
    n_procs = max(1, min(len(urls), max_procs - 2))
    print(f"Detected repo count: {len(urls)}, using {n_procs} processes.")

    all_rows: list[dict[str, list[str]]] = []

    with Pool(processes=n_procs) as pool:
        for row in tqdm(pool.imap_unordered(collect_for_repo, urls), total=len(urls)):
            all_rows.extend(row)

    print(f"Collected transaction count: {len(all_rows)}")

    if not all_rows:
        print("No transactions collected.")
        return

    # pandasに保存（後でマイニングに利用）
    tx_df = pd.DataFrame(all_rows)
    tx_df.to_json(OUTPUT_JSONL, orient="records", lines=True)
    print(f"Completely saved: {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()