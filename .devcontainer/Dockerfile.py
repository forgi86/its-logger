FROM pytorch/pytorch

ENV PIP_ROOT_USER_ACTION=ignore

# Install system packages 
RUN apt-get update && apt-get install -yq wget vim git unzip

RUN apt-get update && apt-get install -yq wget tmux

ARG user=mforgione
ARG group=mforgione
ARG uid=1005
ARG gid=1005
RUN groupadd -g ${gid} ${group}
RUN useradd -u ${uid} -g ${gid} -s /bin/bash -m ${user}
USER ${uid}:${gid}