set dotenv-load

list:
    @just --list

run-registry:
    #!/usr/bin/env bash
    docker run -p $DOCKER_PORT:$DOCKER_PORT --network=host \
        -v /tmp/registry-cache:/var/lib/registry \
        -e REGISTRY_HTTP_ADDR=localhost:$DOCKER_PORT \
        -e REGISTRY_STORAGE=filesystem \
        -e REGISTRY_HEALTH_STORAGEDRIVER_ENABLED=false \
        -e REGISTRY_LOG_LEVEL=debug \
        registry:2

set-permission:
    sudo setfacl -R -m u:ubuntu:rwx /var/lib/docker/overlay2

test-push:
    #!/usr/bin/env bash
    docker pull alpine
    docker tag alpine localhost:$DOCKER_PORT/alpine
    cargo run -- push localhost:$DOCKER_PORT/alpine

test-push-workload:
    #!/usr/bin/env bash
    set -ex
    docker build -t localhost:$DOCKER_PORT/workload -f workloads/Dockerfile workloads
    cargo run -- push localhost:$DOCKER_PORT/workload

get-manifest:
    #!/usr/bin/env bash
    curl -H "Accept: application/vnd.oci.image.manifest.v1+json" localhost:$DOCKER_PORT/v2/workload/manifests/latest | jq .

get-config:
    #!/usr/bin/env bash
    set -ex
    config_digest=$(curl -H "Accept: application/vnd.oci.image.manifest.v1+json" localhost:$DOCKER_PORT/v2/workload/manifests/latest | jq -r .config.digest)
    curl localhost:$DOCKER_PORT/v2/workload/blobs/$config_digest | jq .

test-docker-pull:
    docker pull localhost:$DOCKER_PORT/workload
    docker run --rm localhost:$DOCKER_PORT/workload ls -l

test-real-push-workload:
    docker tag localhost:$DOCKER_PORT/workload simonmok/workload
    cargo run -- push simonmok/workload

test-real-push-alpine:
    docker pull alpine
    docker tag alpine simonmok/alpine
    cargo run -- push simonmok/alpine

test-pull-local:
    cargo run -- --jobs 1 pull localhost:$DOCKER_PORT/workload

test-pull-hub:
    cargo build
    sudo env "PATH=$PATH" ./target/debug/conex --jobs 1  pull simonmok/workload

test-pull-release-vllm:
    cargo build --release
    sudo env "PATH=$PATH" ./target/release/conex --jobs 16 pull simonmok/vllm:demo

reset-all:
    cargo build
    sudo env "PATH=$PATH" ./target/debug/conex clean
    docker system prune --all --force

test-docker-nginx:
    docker pull nginx
    docker run --rm nginx

run-snapshotter:
    cargo build
    sudo env "PATH=$PATH" ./target/debug/conex clean
    docker system prune --all --force
    sudo env "PATH=$PATH" ./target/debug/conex snapshotter

run-snapshotter-release:
    cargo build --release
    sudo env "PATH=$PATH" ./target/release/conex clean
    docker system prune --all --force
    sudo env "PATH=$PATH" ./target/release/conex snapshotter


build-vllm:
    # docker pull nvcr.io/nvidia/pytorch:23.09-py3
    docker build -t vllm:latest -f workloads/vllm.Dockerfile workloads

push-vllm:
    cargo build --release
    sudo env "PATH=$PATH" ./target/release/conex push simonmok/vllm:raw


demo-reset-world:
    #!/usr/bin/env bash

    # first reset docker
    docker system prune --all --force
    sudo bash -c 'rm -rf /var/lib/docker/*'
    sudo systemctl daemon-reload
    sudo systemctl restart docker
    sudo systemctl restart containerd

    # then reset conex
    sudo env "PATH=$PATH" ./target/release/conex clean

demo-reload-docker:
    #!/usr/bin/env bash
    sudo systemctl daemon-reload
    sudo systemctl restart docker

demo-run-snapshotter:
    sudo env "PATH=$PATH" ./target/release/conex snapshotter

# simonmok/vllm:demo