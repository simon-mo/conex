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

get-manifest:
    #!/usr/bin/env bash
    curl -H "Accept: application/vnd.oci.image.manifest.v1+json" localhost:5000/v2/workload/manifests/latest | jq .

get-config:
    #!/usr/bin/env bash
    set -ex
    config_digest=$(curl -H "Accept: application/vnd.oci.image.manifest.v1+json" localhost:5000/v2/workload/manifests/latest | jq -r .config.digest)
    curl localhost:5000/v2/workload/blobs/$config_digest | jq .

test-docker-pull:
    docker pull localhost:5000/workload
    docker run --rm localhost:5000/workload ls -l