FROM ghcr.io/agent-infra/sandbox:latest

# Switch to root for installations
USER root

# 1. Consolidate system dependencies into a single layer
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    python3-venv \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# 2. Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip -q /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install --update && \
    rm -rf /tmp/awscliv2.zip /tmp/aws

# 3. Install uv for rapid Python package resolution
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="/usr/local/bin" sh

# 4. Set up workspace and permissions
RUN mkdir -p /home/gem/workspace && chown -R 1000:1000 /home/gem

# 5. Create virtual environment
RUN /usr/local/bin/uv venv /home/gem/workspace/.venv

# 6. Copy requirements and install
# Note: Docker COPY uses relative paths from your build context. 
# See the build instructions below on how to handle /home/ubuntu/requirements.txt.
COPY requirements.txt /tmp/reqs.txt
RUN . /home/gem/workspace/.venv/bin/activate && \
    uv pip install -r /tmp/reqs.txt \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    --index-strategy unsafe-best-match
# 7. Final permission sweep for the gem user
RUN chown -R 1000:1000 /home/gem

# 8. Set the entrypoint
ENTRYPOINT ["sh", "-c", "chown -R 1000:1000 /home/gem || true; exec /opt/gem/run.sh"]
