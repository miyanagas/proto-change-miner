from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str,
    log_file: Path,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """
    共通ロガー設定:
      - file handler: file_level (デフォルト DEBUG) でログファイルに全出力
      - console handler: console_level (デフォルト INFO) で標準出力に最低限だけ出力
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # ハンドラ側で出力レベルを調整

    # 同じロガーにハンドラを重複追加しないように
    if logger.handlers:
        return logger

    # フォーマット
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ファイルハンドラ（詳細）
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(file_level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # コンソールハンドラ（最低限）
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # 外に伝播させない（親ロガーに二重出力されるのを防ぐ）
    logger.propagate = False

    return logger