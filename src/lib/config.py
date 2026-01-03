from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from lib.skip_rules import SKIP_PREFIXES, SKIP_SUFFIXES, SKIP_PATTERNS


@dataclass(frozen=True)
class BaseConfig:
    repos_dir: str
    out_dir: str
    workers: int = 4

    # 解析対象外ルール（skip_rules.py から読み込み）
    skip_prefixes: Tuple[str, ...] = SKIP_PREFIXES
    skip_suffixes: Tuple[str, ...] = SKIP_SUFFIXES
    skip_patterns: Tuple[str, ...] = SKIP_PATTERNS

    # デバッグ・速度調整（Noneなら全件）
    max_all_commits: int | None = None


@dataclass(frozen=True)
class RQ0Config(BaseConfig):
    # RQ0: pairs探索出力の制御
    merge_all: bool = False

    # まだ閾値は決めない方針だが、将来のために置いておく
    min_n_both: int = 1          # デフォルトはフィルタしないのと同義
    top_k_per_proto: int | None = None  # Noneなら全出力


@dataclass(frozen=True)
class RQ1Config(BaseConfig):
    # RQ1: カテゴリ集約など
    categories: Tuple[str, ...] = ("impl", "test", "build", "config", "infra", "ci", "other")
    min_proto_tx_for_main: int = 5
