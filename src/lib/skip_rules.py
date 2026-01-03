"""自動生成ファイル除外ルール v1

Protobuf/gRPC生成物、よくある生成ディレクトリ、Go生成ファイルを除外
"""
from __future__ import annotations

from typing import Tuple

# --------------------------------------------------
# 解析対象外（ディレクトリプレフィックス）
# --------------------------------------------------
SKIP_PREFIXES: Tuple[str, ...] = (
    # ドキュメント・CI設定
    "docs/",
    ".github/",
    ".gitlab/",
    # 依存ライブラリ
    "vendor/",
    "third_party/",
    "node_modules/",
    # よくある生成ディレクトリ
    "generated/",
    "gen/",
    "dist/",
    "build/",
    "target/",
    "out/",
)

# --------------------------------------------------
# 解析対象外（拡張子・サフィックス）
# --------------------------------------------------
SKIP_SUFFIXES: Tuple[str, ...] = (
    # 画像・ドキュメント
    ".md", ".png", ".jpg", ".jpeg", ".gif", ".svg",
    # ロックファイル
    ".lock",
    ".sum",
    # Protobuf / gRPC 生成物
    ".pb.go",
    ".pb.cc",
    ".pb.h",
    ".pb.c",
    ".pb.java",
    ".pb.kt",
    ".pb.py",
    ".proto.lock",
    # Go generated
    "_generated.go",
    "_gen.go",
    ".generated.go",
)

# --------------------------------------------------
# 解析対象外（ファイル名パターン - fnmatch形式）
# --------------------------------------------------
SKIP_PATTERNS: Tuple[str, ...] = (
    # Protobuf / gRPC 生成物
    "*_pb.go",
    "*_grpc.pb.go",
    "*_grpc.pb.*",
    "*_pb2.py",
    "*_pb2_grpc.py",
    # Go generated
    "zz_generated.*",
    "bindata.go",
    "mock_*.go",
)
