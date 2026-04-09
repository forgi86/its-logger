FROM pytorch/pytorch

ENV PIP_ROOT_USER_ACTION=ignore

# Install system packages
RUN apt-get update && apt-get install -yq \
    git \
    vim \
    wget \
    unzip \
    tmux \
    && rm -rf /var/lib/apt/lists/*

ARG uid
ARG gid
ARG user
ARG group

RUN groupadd -g ${gid} ${group} && \
    useradd -u ${uid} -g ${gid} -s /bin/bash -m ${user}

# Create a non-root user without fixed UID/GID
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages
USER ${uid}:${gid}