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

# Create a non-root user without fixed UID/GID
ARG USERNAME=vscode
RUN useradd -m -s /bin/bash ${USERNAME}

USER ${USERNAME}
WORKDIR /home/${USERNAME}