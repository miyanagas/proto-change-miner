from __future__ import annotations

import subprocess
from pathlib import Path
from typing import List


class GitError(RuntimeError):
    pass


def run_git(repo: Path, args: list[str]) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo), *args],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as e:
        raise GitError(f"git failed: repo={repo}, args={args}") from e


def is_git_repo(path: Path) -> bool:
    return path.is_dir() and (path / ".git").exists()


def list_local_repos(repos_dir: Path) -> list[Path]:
    repos = []
    for child in repos_dir.iterdir():
        if is_git_repo(child):
            repos.append(child)
    return sorted(repos)


def has_proto(repo: Path) -> bool:
    try:
        out = run_git(repo, ["ls-files", "*.proto"])
        return bool(out.strip())
    except Exception:
        return False


def list_all_non_merge_commits(repo: Path, max_commits: int | None = None) -> list[str]:
    args = ["rev-list", "--no-merges", "--all"]
    if max_commits is not None:
        args.extend(["--max-count", str(max_commits)])
    out = run_git(repo, args)
    return [c.strip() for c in out.splitlines() if c.strip()]


def list_proto_change_commits(repo: Path) -> list[str]:
    out = run_git(repo, ["rev-list", "--no-merges", "--all", "--", "*.proto"])
    return [c.strip() for c in out.splitlines() if c.strip()]


def changed_files_in_commit(repo: Path, commit: str) -> list[str]:
    out = run_git(repo, ["diff-tree", "--no-commit-id", "--name-only", "-r", commit])
    return [f.strip() for f in out.splitlines() if f.strip()]
