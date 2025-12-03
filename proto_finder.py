import os
import subprocess
from pathlib import Path

import pandas as pd

CSV_PATH = "P.U_merged_filtered - Final_merged_only_not_excluded_yes_ms_unarchived_commit_hash v2.0.csv"
BASE_DIR = Path("repos")  # ここに全リポジトリをcloneする

BASE_DIR.mkdir(exist_ok=True)

# 1. CSV読み込み
df = pd.read_csv(CSV_PATH, sep=";")

# ここでは 'repo_url' 列に GitHub のURLが入っていると仮定
# 実際の列名に合わせて変更してください
REPO_COL = "URL"


def clone_repo_if_needed(url: str) -> Path:
    """
    GitHub URL を受け取り、repos/ 以下に clone する。
    すでにディレクトリが存在する場合は clone しない。
    """
    name = url.rstrip("/").split("/")[-1]  # URL最後の部分をrepo名として利用
    repo_dir = BASE_DIR / name

    if not repo_dir.exists():
        print(f"[clone] {url} -> {repo_dir}")
        subprocess.run(
            ["git", "clone", "--depth", "1", url, str(repo_dir)],
            check=False,
        )
    else:
        print(f"[skip] already exists: {repo_dir}")

    return repo_dir


def detect_protobuf(repo_dir: Path) -> tuple[bool, str]:
    """
    リポジトリディレクトリ以下を走査して、
    protobuf を使っているかどうかと、その根拠(理由)文字列を返す。
    """

    # 1. .proto ファイルの有無チェック
    proto_files = list(repo_dir.rglob("*.proto"))
    if proto_files:
        return True, f"found_proto_files: {len(proto_files)}"

    # 2. テキスト検索で protobuf 関連の文字列を探す
    #   （必要に応じてパターンを増やしてください）
    PATTERNS = [
        "com.google.protobuf",          # Java
        "io.grpc.protobuf",             # Java
        "google.golang.org/protobuf",   # Go
        "github.com/golang/protobuf",   # Go
        "google.protobuf",              # Python / others
        "protobuf-java",                # Maven dep
        "grpc-protobuf",                # Maven dep
        "protobufjs",                   # JS
        "google-protobuf",              # JS
        "pip install protobuf",         # ドキュメント中など
    ]

    # バイナリ・大きなファイルはスキップした方が良い
    SKIP_SUFFIXES = {
        ".png", ".jpg", ".jpeg", ".gif",
        ".pdf", ".zip", ".jar", ".war",
        ".class", ".bin", ".exe",
    }

    for path in repo_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() in SKIP_SUFFIXES:
            continue

        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        for pat in PATTERNS:
            if pat in text:
                rel = path.relative_to(repo_dir)
                return True, f"pattern:{pat} in {rel}"

    # 何も見つからなかった
    return False, ""

OUT_CSV = "repos_with_protobuf_flag.csv"
if not Path(OUT_CSV).exists():
    results = []

    for idx, row in df.iterrows():
        url = row[REPO_COL]
        if not isinstance(url, str) or not url.startswith("http"):
            print(f"[warn] skip row {idx}, invalid url: {url}")
            results.append((False, "invalid_url"))
            continue

        repo_dir = clone_repo_if_needed(url)
        uses, reason = detect_protobuf(repo_dir)
        results.append((uses, reason))
        print(f"[result] {url} -> uses_protobuf={uses}, reason={reason}")

    # 4. CSVに結果を追加して保存
    df["uses_protobuf"] = [u for (u, r) in results]
    df["protobuf_reason"] = [r for (u, r) in results]

    df.to_csv(OUT_CSV, index=False)
    print(f"saved: {OUT_CSV}")
else:
    df = pd.read_csv(OUT_CSV, sep=",")

df_true = df[df['uses_protobuf'] == True][['Identifier', 'URL', 'protobuf_reason']]
md = df_true.to_markdown(index=False)
with open("repos_using_protobuf.md", "w") as f:
    f.write(md)
print("saved: repos_using_protobuf.md")

summary = df["uses_protobuf"].value_counts().rename_axis("uses_protobuf").reset_index(name="count")
print(summary)