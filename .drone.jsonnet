// the first version is used to build the binary that gets shipped to Docker Hub.
local go_versions = ['1.12.7', '1.11.12', '1.13beta1'];

local test_dockerfile = {
  name: 'test-dockerfile',
  image: 'plugins/docker',
  settings: {
    repo: 'fsouza/gcs-emulator',
    dry_run: true,
  },
  when: {
    event: ['pull_request'],
  },
  depends_on: ['clone'],
};

local push_to_dockerhub = {
  name: 'build-and-push-to-dockerhub',
  image: 'plugins/docker',
  settings: {
    repo: 'fsouza/gcs-emulator',
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

local release_steps = [
  test_dockerfile,
  push_to_dockerhub,
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
  commands: ['go build -o gcs-emulator -mod readonly ./cmd/gcs-emulator'],
  depends_on: ['mod-download'],
};

local test_ci_dockerfile = {
  name: 'test-ci-dockerfile',
  image: 'plugins/docker',
  settings: {
    repo: 'fsouza/gcs-emulator',
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
  name: 'build_go_%(go_version)s' % { go_version: go_version },
  workspace: {
    base: '/go',
    path: 'gcs-emulator',
  },
  steps: [
    mod_download(go_version),
    tests(go_version),
    lint,
    build(go_version),
    test_ci_dockerfile,
  ] + if go_version == go_versions[0] then release_steps else [],
};

std.map(pipeline, go_versions)
