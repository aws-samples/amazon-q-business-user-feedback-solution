#!/bin/bash -x


# https://aws.amazon.com/premiumsupport/knowledge-center/lambda-layer-simulated-docker/

set -e

# 3.11
docker run -v "$PWD":/var/task "public.ecr.aws/sam/build-python3.11" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.11/site-packages/; exit"
zip -r boto_python_layer.zip python > /dev/null

sudo rm -rf python