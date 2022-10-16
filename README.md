## Mysync

## Tests

Run all tests:
`cd tests ; make test`

In order to run particular test (e.g. tests/features/zk_failure.feature):
`cd tests ; GODOG_FEATURE="zk_failure" make test`


### MacOS
Cross-compilation from MacOS to linux:

`ya package pkg.json --target-platform=default-linux-x86_64`

### F.A.Q.

Q: Catch error like `docker: Error response from daemon: pull access denied for mysync-test-base, repository does not exist or may require 'docker login': denied: requested access to the resource is denied.
See 'docker run --help'`

A: You should get access to IAM role Docker-registry dbaas/
