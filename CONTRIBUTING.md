# Contributing to Boom

## Tests

We are currently working on adding tests to the codebase. You can run the tests with:
```bash
cargo test
```

Tests currently require the kafka, valkey, and mongo Docker containers to be running as described above.

*When running the tests, the config file found in `tests/config.test.yaml` will be used.*

The test suite also runs automagically on every push to the repository, and on every pull request.
You can check the status of the tests in the "Actions" tab of the GitHub repository.
