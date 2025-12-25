from __future__ import annotations

PAIR_COLUMNS = [
    "repo",
    "proto_file",
    "file",
    "file_category",
    "n_all_tx",
    "n_proto_tx_for_proto",
    "n_file_tx",
    "n_both_tx",
    "support",
    "confidence",  # P(file | proto_file)
    "baseline",    # P(file)
    "lift",
]

REPO_SUMMARY_COLUMNS = [
    "repo",
    "n_all_tx",
    "n_proto_tx_any",
    "n_unique_proto_files",
    "n_unique_cochange_files",
    "n_pairs",
]
