from __future__ import annotations

from pathlib import Path
from collections import Counter
import pandas as pd

TRANSACTION_PATH = Path("transactions.jsonl")
OUTPUT_CSV = Path("proto_file_cochanges.csv")

MIN_SUPPORT = 0.005      # 全コミットの 0.5% 以上
MIN_CONFIDENCE = 0.3     # P(proto -> file) >= 0.3
MIN_LIFT = 1.0           # 正の関連だけ見る


def is_proto(path: str) -> bool:
    return path.endswith(".proto")


def main() -> None:
    # ---- 1. カウント用の変数 ----
    total_tx = 0                   # トランザクション総数
    count_file = Counter()         # 各ファイルの出現回数
    co_count = Counter()           # (proto, other_file) の共起回数

    # ---- 2. transactions.jsonl を1行ずつ読む ----
    # DataFrame に一気に読み込まず、ストリーム的に処理してもOK
    for obj in pd.read_json(TRANSACTION_PATH, lines=True, chunksize=1000):
        # chunksize を使うとメモリ効率が良くなります（任意）
        for tx in obj["transaction"]:
            # tx: List[str] を想定
            # 念のため集合にして重複を除く
            files = list(set(tx))
            total_tx += 1

            # 各ファイルの出現回数をカウント
            for f in files:
                count_file[f] += 1

            # proto とそれ以外に分ける
            protos = [f for f in files if is_proto(f)]
            others = [f for f in files if not is_proto(f)]

            # 各 proto と他ファイルのペア共起をカウント
            for p in protos:
                for o in others:
                    co_count[(p, o)] += 1

    print(f"総トランザクション数: {total_tx}")
    print(f"ユニークファイル数: {len(count_file)}")
    print(f"proto-ファイルのペア数: {len(co_count)}")

    # ---- 3. support / confidence / lift を計算 ----
    rows = []

    for (proto, file), n_co in co_count.items():
        supp_pf = n_co / total_tx            # support(proto, file)
        supp_p = count_file[proto] / total_tx
        supp_f = count_file[file] / total_tx

        # ゼロ割防止
        if supp_p == 0 or supp_f == 0:
            continue

        conf = supp_pf / supp_p             # P(file | proto)
        lift = conf / supp_f

        # 閾値でフィルタ
        if supp_pf < MIN_SUPPORT:
            continue
        if conf < MIN_CONFIDENCE:
            continue
        if lift < MIN_LIFT:
            continue

        rows.append({
            "proto": proto,
            "file": file,
            "support": supp_pf,
            "confidence": conf,
            "lift": lift,
            "count_proto_and_file": n_co,
            "count_proto": count_file[proto],
            "count_file": count_file[file],
        })

    # ---- 4. DataFrame にして CSV 出力 ----
    if not rows:
        print("条件を満たすペアがありませんでした。閾値を下げて再実行してみてください。")
        return

    df = pd.DataFrame(rows)

    # 重要そうな順にソート
    df = df.sort_values(
        by=["lift", "confidence", "support"],
        ascending=[False, False, False],
    )

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"保存完了: {OUTPUT_CSV} に {len(df)} 行出力しました。")

    md = df.to_markdown(index=False)
    with open("proto_file_cochanges.md", "w") as f:
        f.write(md)
    print("保存完了: proto_file_cochanges.md にマークダウン形式で出力しました。")

if __name__ == "__main__":
    main()