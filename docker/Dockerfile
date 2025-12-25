# Dockerfile

FROM python:3.13.10-slim

# ---- OS レベルの準備 ----
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# ---- uv を公式イメージからコピー（curl installより安定） ----
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /workspace

# ---- 依存関係だけ先にコピーしてインストール（Docker キャッシュ効かせる） ----
COPY pyproject.toml uv.lock ./

# .venv を作って依存関係をインストール
# --frozen で uv.lock に書かれたバージョンを厳密に再現
RUN uv sync --frozen --no-dev

# コンテナ起動時に .venv の Python を優先して使う
ENV PATH="/workspace/.venv/bin:${PATH}"
ENV PYTHONPATH="/workspace/src"

ENTRYPOINT ["bash", "-lc"]
CMD ["echo 'Use: docker compose run --rm runner <command>'"]
