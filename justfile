list:
    @just --list

run-registry:
    #!/usr/bin/env bash
    docker run -p 5000:5000 --network=host \
        -v /tmp/registry-cache:/var/lib/registry \
        -e REGISTRY_STORAGE=filesystem \
        -e REGISTRY_HEALTH_STORAGEDRIVER_ENABLED=false \
        -e REGISTRY_LOG_LEVEL=debug \
        registry:2

set-permission:
    sudo setfacl -R -m u:ubuntu:rwx /var/lib/docker/overlay2

test-push:
    #!/usr/bin/env bash
    docker pull alpine
    docker tag alpine localhost:5000/alpine
    cargo run -- push localhost:5000/alpine

test-push-workload:
    #!/usr/bin/env bash
    set -ex
    docker build -t localhost:5000/workload -f workloads/Dockerfile workloads
    cargo run -- push localhost:5000/workload

