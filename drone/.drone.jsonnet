// the first version is used to build the binary that gets shipped to Docker Hub.
local go_versions = ['1.12.7', '1.11.12', '1.13beta1'];

local test_dockerfile = {
  name: 'test-dockerfile',
  image: 'plugins/docker',
  settings: {
    repo: 'fsouza/fake-gcs-server',
    dry_run: true,
  },
  when: {
    event: ['push', 'pull_request'],
  },
  depends_on: ['clone'],
};

local push_to_dockerhub = {
  name: 'build-and-push-to-dockerhub',
  image: 'plugins/docker',
  settings: {
    repo: 'fsouza/fake-gcs-server',
    auto_tag: true,
    dockerfile: 'drone/Dockerfile',
    username: { from_secret: 'docker_username' },
    password: { from_secret: 'docker_password' },
  },
  when: {
    ref: [
      'refs/tags/*',
      'refs/heads/master',
    ],
  },
  depends_on: ['test', 'lint', 'build'],
};

local docker_sanity_check(name, image_version, refs) = {
  name: 'docker-sanity-check-%(name)s' % { name: name },
  image: 'fsouza/fake-gcs-server:%(image_version)s' % { image_version: image_version },
  pull: 'always',
  commands: ['fake-gcs-server -h'],
  when: {
    ref: refs,
  },
  depends_on: ['build-and-push-to-dockerhub'],
};

local goreleaser = {
  name: 'goreleaser',
  image: 'goreleaser/goreleaser',
  commands: [
    'git fetch --tags',
    'goreleaser release',
  ],
  environment: {
    GITHUB_TOKEN: {
      from_secret: 'github_token',
    },
  },
  depends_on: ['test', 'lint'],
  when: {
    event: ['tag'],
  },
};

local goreleaser_test = {
  name: 'test-goreleaser',
  image: 'goreleaser/goreleaser',
  commands: [
    'goreleaser release --snapshot',
  ],
  depends_on: ['clone'],
  when: {
    event: ['push', 'pull_request'],
  },
};

local release_steps = [
  test_dockerfile,
  push_to_dockerhub,
  docker_sanity_check('push', 'latest', ['refs/heads/master']),
  docker_sanity_check('tag', '${DRONE_TAG}', ['refs/tags/*']),
  goreleaser_test,
  goreleaser,
];

local mod_download(go_version) = {
  name: 'mod-download',
  image: 'golang:%(go_version)s' % { go_version: go_version },
  commands: ['go mod download'],
  environment: { GOPROXY: 'https://proxy.golang.org' },
  depends_on: ['clone'],
};

local tests(go_version) = {
  name: 'test',
  image: 'golang:%(go_version)s' % { go_version: go_version },
  commands: ['go test -race -vet all -mod readonly ./...'],
  depends_on: ['mod-download'],
};

local lint = {
  name: 'lint',
  image: 'golangci/golangci-lint',
  pull: 'always',
  commands: ['golangci-lint run --enable-all -D errcheck -D lll -D dupl -D gochecknoglobals -D unparam --deadline 5m ./...'],
  depends_on: ['mod-download'],
};

local build(go_version) = {
  name: 'build',
  image: 'golang:%(go_version)s' % { go_version: go_version },
  commands: ['go build -o fake-gcs-server -mod readonly'],
  environment: { CGO_ENABLED: '0' },
  depends_on: ['mod-download'],
};

local sanity_check = {
  name: 'sanity-check',
  image: 'alpine',
  commands: ['./fake-gcs-server -h'],
  depends_on: ['build'],
};

local test_ci_dockerfile = {
  name: 'test-ci-dockerfile',
  image: 'plugins/docker',
  settings: {
    repo: 'fsouza/fake-gcs-server',
    dockerfile: 'drone/Dockerfile',
    dry_run: true,
  },
  when: {
    event: ['pull_request'],
  },
  depends_on: ['build'],
};

local pipeline(go_version) = {
  kind: 'pipeline',
  name: 'go-%(go_version)s' % { go_version: go_version },
  workspace: {
    base: '/go',
    path: 'fake-gcs-server',
  },
  steps: [
    mod_download(go_version),
    tests(go_version),
    lint,
    build(go_version),
    sanity_check,
    test_ci_dockerfile,
  ] + if go_version == go_versions[0] then release_steps else [],
};

std.map(pipeline, go_versions)
