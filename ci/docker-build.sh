#!/bin/sh -e

# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

function pick_tag() {
	tag=latest
	if [ "${GITHUB_HEAD_REF##refs/tags/}" != "${GITHUB_HEAD_REF}" ]; then
		tag=${GITHUB_HEAD_REF##refs/tags}
	fi
	echo $tag
}

docker build -t fsouza/fake-gcs-server:$(pick_tag) -f ci/Dockerfile .

if [ -z "${DRY_RUN}" ]; then
	docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}"
	docker push fsouza/fake-gcs-server
fi

docker system prune -af
