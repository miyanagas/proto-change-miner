import subprocess
from pathlib import Path
from multiprocessing import Pool, cpu_count
from typing import Tuple, List

import pandas as pd

from utils.logging_setup import setup_logger

# --- パス設定 --------------------------------------------------------------

CSV_PATH = (
    Path("/workspace/data")
    / "P.U_merged_filtered - Final_merged_only_not_excluded_yes_ms_unarchived_commit_hash v2.0.csv"
)

# リポジトリを clone するベースディレクトリ
BASE_DIR = Path("/workspace/repos")
BASE_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_DIR = Path("/workspace/results")
OUTPUT_CSV = RESULTS_DIR / "repos_with_protobuf_flag.csv"
OUTPUT_MD = RESULTS_DIR / "repos_using_protobuf.md"

# 入力 CSV 内でリポジトリ URL が入っているカラム名
REPO_COL = "URL"

logger = setup_logger(
    name="detect_protobuf",
    log_file= Path("/workspace/logs") / "detect_protobuf.log",
    console_level=20, # logging.INFO
    file_level=10,    # logging.DEBUG
)

def clone_repo_if_needed(url: str) -> Path:
    """
    指定されたリポジトリを clone (--depth=1) する

    Parameters
    ----------
    url : str
        Git リポジトリの URL (例: https://github.com/owner/repo)

    Returns
    -------
    Path
        clone 先のローカルディレクトリパス
        既に存在している場合はそのディレクトリを返す
    """
    name = url.rstrip("/").split("/")[-1]
    repo_dir = BASE_DIR / name

    if not repo_dir.exists():
        logger.info(f"[clone] {url} -> {repo_dir}")
        # --depth=1 で浅い clone
        result = subprocess.run(
            ["git", "clone", "--depth", "1", url, str(repo_dir)],
            check=False,
        )
        if result.returncode != 0:
            # clone に失敗した場合はログに記録しておく
            logger.error(f"[clone] failed to clone {url}, returncode={result.returncode}")
    else:
        logger.debug(f"[skip] already exists: {repo_dir}")

    return repo_dir


def detect_protobuf(repo_dir: Path) -> Tuple[bool, str]:
    """
    指定されたリポジトリディレクトリ内で、protobuf の利用有無を判定する

    判定ロジック:
    1. .proto ファイルが 1 つ以上存在するかどうか
    2. それが無ければ、テキストファイルを走査して protobuf 関連の文字列パターンを検索

    Parameters
    ----------
    repo_dir : Path
        ローカル上の Git リポジトリディレクトリ

    Returns
    -------
    Tuple[bool, str]
        - 第 1 要素: Protobuf を利用していると判定したかどうか
        - 第 2 要素: 判定に至った理由（例: "found_proto_files: 3", "pattern:google.protobuf in path/to/file"）
    """
    # 1. .proto ファイル確認
    proto_files = list(repo_dir.rglob("*.proto"))
    if proto_files:
        return True, f"found_proto_files: {len(proto_files)}"

    # 2. テキスト検索
    # Protobuf 利用の痕跡として検索する文字列パターン
    PATTERNS = [
        "com.google.protobuf",
        "io.grpc.protobuf",
        "google.golang.org/protobuf",
        "github.com/golang/protobuf",
        "google.protobuf",
        "protobuf-java",
        "grpc-protobuf",
        "protobufjs",
        "google-protobuf",
        "pip install protobuf",
    ]

    # テキスト探索から除外するバイナリ／アーカイブ系の拡張子
    SKIP_SUFFIXES = {
        ".png", ".jpg", ".jpeg", ".gif",
        ".pdf", ".zip", ".jar", ".war",
        ".class", ".bin", ".exe",
    }

    for path in repo_dir.rglob("*"):
        if not path.is_file():
            continue
        # 明らかにバイナリ・アーカイブ系の拡張子はスキップ
        if path.suffix.lower() in SKIP_SUFFIXES:
            continue

        try:
            # 文字化けしても強制的に読み進めるため errors="ignore"
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            # 権限エラーなどで読めないファイルはスキップ
            continue

        for pat in PATTERNS:
            if pat in text:
                rel = path.relative_to(repo_dir)
                return True, f"pattern:{pat} in {rel}"

    return False, ""


def process_one(args: Tuple[int, pd.Series]) -> Tuple[bool, str]:
    """
    DataFrame の 1 行に対して、リポジトリ clone と protobuf 判定を行う

    Parameters
    ----------
    args : Tuple[int, pandas.Series]
        (行インデックス, 行データ) のタプル
        行データには REPO_COL (URL) カラムが含まれている前提

    Returns
    -------
    Tuple[bool, str]
        detect_protobuf の戻り値と同様:
        - uses_protobuf: bool
        - reason: str (検出理由 or エラー理由)
    """
    idx, row = args
    url = row[REPO_COL]

    if not isinstance(url, str) or not url.startswith("http"):
        logger.warning(f"[warn] skip row {idx}, invalid url: {url}")
        return False, "invalid_url"

    repo_dir = clone_repo_if_needed(url)
    uses, reason = detect_protobuf(repo_dir)
    logger.info(f"[result] {url} -> uses_protobuf={uses}")
    logger.debug(f"  reason={reason}")
    return uses, reason


def main() -> None:
    """
    スクリプト全体のエントリポイント

    - 入力 CSV の存在チェック
    - 既存結果 CSV (OUT_CSV) があればそれを再利用
    - 無ければ並列で各リポジトリを解析し、結果を OUT_CSV に保存
    - uses_protobuf=True の行のみ Markdown テーブルで OUT_MD に保存
    - uses_protobuf の集計結果をログに出力
    """
    if not CSV_PATH.exists():
        logger.error(f"CSV not found: {CSV_PATH}")
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    if OUTPUT_CSV.exists():
        # 既に判定済みならマークダウンだけ更新
        logger.info(f"{OUTPUT_CSV} already exists, reuse it.")
        df = pd.read_csv(OUTPUT_CSV)
    else:
        df = pd.read_csv(CSV_PATH, sep=";")

        # 並列実行
        n_proc = max(1, cpu_count() - 2)
        logger.info(f"Using {n_proc} processes")

        with Pool(processes=n_proc) as pool:
            results: List[Tuple[bool, str]] = pool.map(
                process_one,
                list(df.iterrows()),
            )

        df["uses_protobuf"] = [u for (u, r) in results]
        df["protobuf_reason"] = [r for (u, r) in results]
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"saved: {OUTPUT_CSV}")

    # Protobuf を使っているリポジトリだけの Markdown を出力
    df_true = df[df["uses_protobuf"] == True][["Identifier", "URL", "protobuf_reason"]]
    md = df_true.to_markdown(index=False)
    OUTPUT_MD.write_text(md, encoding="utf-8")
    logger.info(f"saved: {OUTPUT_MD}")

    # uses_protobuf の True / False の件数をまとめてログ出力
    summary = (
        df["uses_protobuf"]
        .value_counts()
        .rename_axis("uses_protobuf")
        .reset_index(name="count")
    )
    logger.info("Summary of uses_protobuf:\n%s", summary.to_string(index=False))


if __name__ == "__main__":
    main()