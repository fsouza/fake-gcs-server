local Pipeline(go_version) = {
  kind: "pipeline",
  steps: [
    {
      name: "setup",
      image: "golang:" + go_version,
      commands: [
        "go get github.com/alecthomas/gometalinter github.com/golang/dep/cmd/dep",
        "gometalinter --install",
        "dep ensure -v",
        "go install ./...",
      ],
    },
    {
      name: "lint",
      image: "golang:" + go_version,
      commands: [
        "gometalinter --enable-gc --enable=gofmt --enable=goimports --disable=errcheck --disable=gas --disable=gosec --deadline=10m --vendor --tests ./...",
      ],
    },
    {
      name: "test",
      image: "golang:" + go_version,
      commands: [
        "go test -race ./...",
      ],
    },
  ],
};

[
  Pipeline("1.10"),
  Pipeline("1.11"),
]
