#!/bin/sh -e

# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

IMAGE_NAME=fsouza/fake-gcs-server

function pick_tag() {
	tag=latest
	if [ "${GITHUB_HEAD_REF##refs/tags/}" != "${GITHUB_HEAD_REF}" ]; then
		tag=${GITHUB_HEAD_REF##refs/tags}
	fi
	echo $tag
}

function additional_tags() {
	original_tag=$1
	if echo "$original_tag" | grep -q '^v\d\+\.\d\+\.\d\+$'; then
		filtered=${original_tag#v}
		tags="${filtered} ${filtered%.*} ${filtered%%.*}"

		for tag in $tags; do
			docker tag ${IMAGE_NAME}:${original_tag} ${IMAGE_NAME}:${tag}
		done
	fi
}

tag=$(pick_tag)
docker build -t "${IMAGE_NAME}:${tag}" -f ci/Dockerfile .
additional_tags "${tag}"

if [ "${GITHUB_EVENT_NAME}" = "push" ]; then
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}"
	docker push ${IMAGE_NAME}
fi

docker system prune -af

# sanity check
docker run "${IMAGE_NAME}:${tag}" -h
