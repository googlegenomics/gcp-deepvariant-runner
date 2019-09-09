# A docker image with helper binaries for running DeepVariant at scale using
# the Google Cloud Platform.

# Install pipelines tool.
FROM golang:1.11
RUN go get github.com/googlegenomics/pipelines-tools/pipelines

FROM google/cloud-sdk:alpine

# Copy pipelines tool from the previous step.
COPY --from=0 /go/bin/pipelines /usr/bin

RUN apk add --update python3 python3-dev build-base && \
    ln -sf python3 /usr/bin/python && \
    pip3 install --upgrade pip enum34 retrying google-api-core google-cloud-storage && \
    mkdir -p /opt/deepvariant_runner/bin && \
    mkdir -p /opt/deepvariant_runner/src

ADD LICENSE /
ADD gcp_deepvariant_runner.py /opt/deepvariant_runner/src/
ADD gke_cluster.py /opt/deepvariant_runner/src/
ADD process_util.py /opt/deepvariant_runner/src/
ADD run_and_verify.sh /opt/deepvariant_runner/bin/
ADD cancel /opt/deepvariant_runner/bin/

# Create shell wrappers for python files for easier use.
RUN \
    BASH_HEADER='#!/bin/bash' && \
    printf "%s\n%s\n" \
        "${BASH_HEADER}" \
        'python /opt/deepvariant_runner/src/gcp_deepvariant_runner.py "$@"' > \
        /opt/deepvariant_runner/bin/gcp_deepvariant_runner && \
    chmod +x /opt/deepvariant_runner/bin/gcp_deepvariant_runner
