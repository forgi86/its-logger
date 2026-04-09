docker build -t logger .
    

docker run -dit \
  --name logger-runner \
  --user $(id -u):$(id -g) \
  --mount type=bind,source="$(pwd)",target=/workspace \
  -w /workspace \
  logger:latest \
  sleep infinity

docker exec -it logger-runner /bin/bash

export HOME=/workspace
pip install --user -r requirements.txt


scp -r mforgione@hyperion.idsia.ch:~/projects/its-logger/data/ .
