FROM astral/uv:python3.14-alpine AS base
LABEL maintainer="Xero-Hige <Gaston.martinez.90@gmail.com>"

# Set uv environment variables for performance
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Copy dependency files.
COPY pyproject.toml uv.lock /opt/

# Copy source files
COPY projects/base_project /opt/projects/base_project

# Install dependencies to /opt/venv (outside the source mount path)
WORKDIR /opt
RUN uv sync --no-install-project --no-dev

FROM base AS project_a

COPY projects/project_a /opt/projects/project_a

RUN uv sync --no-install-project --no-dev

ENTRYPOINT ["uv", "run", "--package","project_a","start"]
