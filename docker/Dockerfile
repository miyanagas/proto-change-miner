# Dockerfile

FROM python:3.13.10-slim

# ---- OS レベルの準備 ----
# curl と git を入れる（uv インストールと GitPython 用）
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl git && \
    rm -rf /var/lib/apt/lists/*

# ---- uv をインストール ----
# /root/.local/bin に入るので PATH に通す
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /workspace

# ---- 依存関係だけ先にコピーしてインストール（Docker キャッシュ効かせる） ----
COPY pyproject.toml uv.lock ./

# .venv を作って依存関係をインストール
# --frozen で uv.lock に書かれたバージョンを厳密に再現
RUN uv sync --frozen --no-dev

# コンテナ起動時に .venv の Python を優先して使う
ENV PATH="/workspace/.venv/bin:${PATH}"

# ---- アプリケーションコードをコピー ----
# 必要に応じて他のディレクトリも追加
COPY src ./src

# デフォルトの作業ディレクトリ
WORKDIR /workspace/src

# デフォルト CMD は解析スクリプトのヘルプを表示（compose で上書き可能）
CMD ["python", "analyze_proto.py", "--help"]
