# Configure shell
SHELL = bash -e -o pipefail
PWD = $(shell pwd)

# Variables
VERSION                  ?= $(shell cat ./VERSION)
SERVICE_NAME             ?= fake-gcs-server

## Docker related
DOCKER_REGISTRY          ?=
DOCKER_REPOSITORY        ?= fsouza/
DOCKER_TAG               ?= ${VERSION}
DOCKER_IMAGENAME         := ${DOCKER_REGISTRY}${DOCKER_REPOSITORY}${SERVICE_NAME}:${DOCKER_TAG}

all: build

build:
	docker build -t ${DOCKER_IMAGENAME} .

push:
	docker push ${DOCKER_IMAGENAME}

run:
	docker run -d --name ${SERVICE_NAME} -p 4443:4443 -v ${PWD}/examples/data:/data ${DOCKER_IMAGENAME}

stop:
	docker rm -f ${SERVICE_NAME}
