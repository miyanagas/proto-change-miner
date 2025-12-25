from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Tuple

TEST_PAT = re.compile(r"(^|/)(test|tests|testing)(/|$)", re.IGNORECASE)
CI_PAT = re.compile(r"(^|/)(ci|pipeline|workflows)(/|$)", re.IGNORECASE)

# 拡張子 → 粗いカテゴリ（最終は6-7分類に寄せる）
EXT_IMPL = {".go", ".java", ".py", ".js", ".ts", ".tsx", ".jsx", ".kt", ".rb", ".rs", ".php", ".cs"}
EXT_CONFIG = {".yml", ".yaml", ".json", ".toml", ".ini", ".cfg", ".env"}
EXT_BUILD = {".gradle", ".xml"}  # Mavenのpom.xml等は xml判定でbuildへ寄せる
EXT_INFRA = {".tf"}


def should_skip(path: str, *, skip_prefixes: Tuple[str, ...], skip_suffixes: Tuple[str, ...]) -> bool:
    lower = path.lower()
    for pre in skip_prefixes:
        if lower.startswith(pre):
            return True
    for suf in skip_suffixes:
        if lower.endswith(suf):
            return True
    return False


def categorize(path: str) -> str:
    p = path.lower()
    name = Path(p).name

    # 役割（優先）
    if TEST_PAT.search(p) or name.endswith(("_test.go", "test.java")):
        return "test"
    if CI_PAT.search(p) or p.startswith((".github/", ".gitlab/")):
        return "ci"

    # infraっぽいディレクトリ
    if p.startswith(("docker/", "k8s/", "helm/", "charts/")) or "docker-compose" in p:
        return "infra"
    if name == "dockerfile":
        return "infra"
    if name in ("makefile",):
        return "build"
    if name in ("pom.xml", "build.gradle", "settings.gradle"):
        return "build"

    # 拡張子
    ext = Path(p).suffix
    if ext == ".proto":
        return "proto"
    if ext in EXT_IMPL:
        return "impl"
    if ext in EXT_CONFIG:
        return "config"
    if ext in EXT_INFRA:
        return "infra"
    if ext in EXT_BUILD:
        return "build"

    # shellなど
    if ext in (".sh", ".bash", ".zsh"):
        return "build"

    return "other"
