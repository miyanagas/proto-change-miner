from __future__ import annotations

from pathlib import Path
from multiprocessing import Pool, cpu_count
from typing import Any

from git import Repo, GitCommandError, InvalidGitRepositoryError
from pydriller import Repository
import pandas as pd
from tqdm import tqdm
import shutil

from utils.logging_setup import setup_logger

REPOS_DIR = Path("/workspace/repos")
REPOS_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_DIR = Path("/workspace/results")
CSV_PATH = RESULTS_DIR / "repos_with_protobuf_flag.csv"
OUT_JSONL = RESULTS_DIR / "transactions.jsonl"

logger = setup_logger(
    name="collect_transactions",
    log_file= Path("/workspace/logs") / "collect_transactions.log",
    console_level=20, # logging.INFO
    file_level=10,    # logging.DEBUG
)


def collect_transactions(repo_path: Path) -> list[list[str]]:
    """
    1 つの Git リポジトリから「.proto を含むコミットごとのトランザクション」を収集する。

    トランザクションの定義:
        - あるコミットで変更されたファイルの集合 (パスのリスト)
        - ただし、そのコミット内に .proto ファイルの変更が含まれるものに限定する

    具体的には:
        - PyDriller を用いて merge でないコミットのみを走査
        - 各コミットについて、変更ファイル (modified_files) の new_path / old_path を収集
        - その中に .proto 拡張子のファイルがあれば、
          そのコミットで変更されたすべてのファイルパスの集合を 1 トランザクションとする

    Parameters
    ----------
    repo_path : Path
        解析対象の Git リポジトリのローカルパス

    Returns
    -------
    list[list[str]]
        トランザクションのリスト。
        各要素は、そのコミットで変更されたファイルパスのリスト（ソート済み）
    """
    transactions: list[list[str]] = []

    # only_no_merge=True でマージコミットを除外して traverse
    for commit in Repository(str(repo_path), only_no_merge=True).traverse_commits():
        paths: set[str] = set()
        has_proto = False

        # commit.modified_files はそのコミットで変更されたファイル群
        for mf in commit.modified_files:
            # new_path / old_path の両方をチェック
            for p in (mf.new_path, mf.old_path):
                if p is None:
                    continue
                paths.add(p)
                # .proto ファイルが 1 つでもあればフラグを立てる
                if p.endswith(".proto"):
                    has_proto = True

        # .proto を含むコミットだけトランザクションとして追加
        if has_proto and paths:
            transactions.append(sorted(paths))

    return transactions


def ensure_full_repo(repo_url: str) -> Path | None:
    """
    対象のリポジトリが「完全な履歴を持つ clone」としてローカルに存在することを保証する

    挙動:
        1. REPOS_DIR 配下に存在しなければ full clone を実行
        2. 既に存在する場合:
            - .git/shallow があれば shallow clone とみなし、`git fetch --unshallow` で full 化を試みる
            - InvalidGitRepositoryError や unshallow 失敗時は、ディレクトリを削除して clone し直す

    Parameters
    ----------
    repo_url : str
        Git リポジトリ URL (例: "https://github.com/owner/repo.git")

    Returns
    -------
    Path | None
        full なリポジトリが存在するローカルパス
        clone や unshallow に失敗した場合は None を返す
    """
    name = repo_url.rstrip("/").split("/")[-1]
    repo_path = REPOS_DIR / name

    # まだ存在しない場合 → full clone
    if not repo_path.exists():
        logger.info(f"[clone-full] {repo_url} -> {repo_path}")
        try:
            Repo.clone_from(repo_url, repo_path)
        except GitCommandError as e:
            logger.error(f"[clone error] {repo_url}: {e}")
            return None
        return repo_path

    # すでに存在する場合 → shallow かどうか確認
    shallow_file = repo_path / ".git" / "shallow"
    if shallow_file.exists():
        logger.info(f"[unshallow] {name} at {repo_path}")
        try:
            repo = Repo(repo_path)
        except InvalidGitRepositoryError as e:
            logger.error(f"[invalid repo] {repo_path}: {e} -> re-cloning")
            # 壊れているので削除して clone し直す
            shutil.rmtree(repo_path, ignore_errors=True)
            try:
                Repo.clone_from(repo_url, repo_path)
            except GitCommandError as e2:
                logger.error(f"[re-clone error] {repo_url}: {e2}")
                return None
            return repo_path

        # shallow → full に昇格
        try:
            repo.git.fetch("--unshallow")
            logger.info(f"[unshallow done] {name}")
        except GitCommandError as e:
            logger.warning(f"[unshallow failed] {name}: {e} -> re-cloning")
            shutil.rmtree(repo_path, ignore_errors=True)
            try:
                Repo.clone_from(repo_url, repo_path)
            except GitCommandError as e2:
                logger.error(f"[re-clone error] {repo_url}: {e2}")
                return None
            return repo_path

    else:
        # shallow_file がない = すでに full history を持っているとみなす
        logger.debug(f"[full repo exists] {name} at {repo_path}")

    return repo_path


def collect_for_repo(repo_url: str) -> list[dict[str, list[str]]]:
    """
    1つのリポジトリを対象に、トランザクション収集までを一括で行う

    フロー:
        1. ensure_full_repo で full clone をローカルに用意
        2. collect_transactions で .proto を含むコミットからトランザクションを収集
        3. "repo" 名と "transaction" を持つ dict のリストに整形して返す

    Parameters
    ----------
    repo_url : str
        対象リポジトリの URL

    Returns
    -------
    list[dict[str, list[str]]]
        収集したトランザクション情報のリスト
        各要素は {"repo": <リポジトリ名>, "transaction": <ファイルパスのリスト>} の形
        エラーなどで何も収集できなかった場合は空リストを返す
    """
    repo_path = ensure_full_repo(repo_url)
    if repo_path is None:
        return []

    repo_name = repo_path.name

    logger.info(f"[start] {repo_url}")
    try:
        tx_list = collect_transactions(repo_path)
    except Exception as e:
        logger.error(f"[error] {repo_name}: {e}")
        return []

    logger.info(f"[done] {repo_name}: collected {len(tx_list)} transactions")

    return [
        {
            "repo": repo_name,
            "transaction": tx,
        }
        for tx in tx_list
    ]


def main() -> None:
    """
    全体のエントリポイント

    - repos_with_protobuf_flag.csv を読み込む
    - uses_protobuf == True のリポジトリ URL のみを抽出
    - 各リポジトリに対して並列でトランザクション収集を行う
    - 収集結果を JSON Lines 形式 (1 行 1 レコード) で OUT_JSONL に保存する

    Raises
    ------
    FileNotFoundError
        入力の CSV_PATH が存在しない場合
    """
    if not CSV_PATH.exists():
        logger.error(f"CSV not found: {CSV_PATH}")
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    # Protobuf 有りリポジトリ一覧を含む CSV を読み込み
    df = pd.read_csv(CSV_PATH, sep=",")
    urls = df.loc[df["uses_protobuf"] == True, "URL"].tolist()

    max_procs = cpu_count()
    n_procs = min(len(urls), max(1, max_procs - 2))
    logger.info(f"Detected repo count: {len(urls)}, using {n_procs} processes.")

    all_rows: list[dict[str, list[str]]] = []

    with Pool(processes=n_procs) as pool:
        # imap_unordered により、処理が終わった順に結果を受け取る
        for row in tqdm(pool.imap_unordered(collect_for_repo, urls), total=len(urls)):
            all_rows.extend(row)

    logger.info(f"Collected transaction count: {len(all_rows)}")

    if not all_rows:
        logger.info("No transactions collected.")
        return

    # DataFrame に変換して JSON Lines として保存
    tx_df = pd.DataFrame(all_rows)
    tx_df.to_json(OUT_JSONL, orient="records", lines=True)
    logger.info(f"Completely saved: {OUT_JSONL}")


if __name__ == "__main__":
    main()