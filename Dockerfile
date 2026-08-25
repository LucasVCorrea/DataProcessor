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

ARG WORKER_NAME=worker

COPY projects/${PROJECT_NAME}/${PIPELINE_NAME}/pyproject.toml /opt/projects/${PROJECT_NAME}/${PIPELINE_NAME}/pyproject.toml

WORKDIR /opt/projects/${PROJECT_NAME}/${PIPELINE_NAME}
RUN uv sync --frozen --project .

ENTRYPOINT ["uv","run","--project",".","airflow"]

#Specific workers

FROM airflow_base AS g-worker-base

COPY base_workers/google-worker /opt/base_workers/google-worker

WORKDIR /opt/base_workers/google-worker

RUN uv sync --frozen --project .

ENTRYPOINT ["uv","run","--project",".","airflow"]


FROM g-worker-base AS g-worker

ARG WORKER_NAME=g-worker

COPY projects/${PROJECT_NAME}/${PIPELINE_NAME}/pyproject.toml /opt/projects/${PROJECT_NAME}/${PIPELINE_NAME}/pyproject.toml

WORKDIR /opt/projects/${PROJECT_NAME}/${PIPELINE_NAME}
RUN uv sync --frozen --project .

ENTRYPOINT ["uv","run","--project",".","airflow"]

#---------------------------------------------
