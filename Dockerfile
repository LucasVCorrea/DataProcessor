FROM astral/uv:python3.14-alpine AS airflow_base
LABEL maintainer="Xero-Hige <Gaston.martinez.90@gmail.com>"

# Set uv environment variables for performance
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Copy dependency files.
COPY pyproject.toml uv.lock /opt/

# Copy source files
COPY airflow_base /opt/airflow_base

# Install dependencies to /opt/venv (outside the source mount path)
WORKDIR /opt
RUN uv sync --frozen --package airflow-base

ENTRYPOINT ["uv", "run", "--package","airflow-base","airflow"]

FROM airflow_base AS worker

ARG WORKER_NAME=aset-worker

COPY workers/${WORKER_NAME} /opt/workers/${WORKER_NAME}

ENV WORKER_NAME=$WORKER_NAME
RUN uv sync --frozen --package ${WORKER_NAME}

ENTRYPOINT ["sh","-c", "uv run --package $WORKER_NAME airflow \"$@\"", "--"]
