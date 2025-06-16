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

## Errors, instrumentation, and logging

Here are some conventions we try to adhere to.
It's important to emphasize that none of this is set in stone.
We evolve as our needs change and as we discover better ways of doing things.

### Errors

Panicking via `Result::unwrap`, `Result::expect`, etc. is almost always discouraged.

* Panicking is should be reserved for truly unrecoverable errors.

* Unrecoverable errors include things that go wrong during startup.

All other errors are recoverable errors and should be expressed using custom error types (usually enums) that implement `std::error::Error`.
These errors should be propagated to the caller using the `?` operator.

* Using `thiserror` to help define custom errors is recommended, but not required.

* Whether to reuse an existing error type in boom versus create a new one isn't always clear, but can be discussed during review.

* (We may at some point shift to using the "accumulator" pattern with types like `anyhow::Error` or `eyre::Report`, reducing the need for as many custom error types).

Each error is propagated until it's *handled*, i.e., when a caller intercepts the error value to either use it for control flow or log it.

### Instrumentation

We use `tracing::Span`s as the foundation for instrumenting boom.
Briefly, a span represents a duration, e.g., the execution of a function.
Each span has a name and optionally a set values recorded as fields.
Spans can be nested.
When a `tracing::Event` is created, e.g., using `tracing::info!` or `tracing::error!`, it comes with the list of its active parent spans and their fields.
This provides structured context that helps make events easier to understand.

* The `tracing::instrument` attribute is a preferred way to define a span for a function or method.
  This is not the only way to define a span, and spans are not required to be tied to functions, but using `instrument` is very convenient.

* `instrument` automatically records each function argument as a field, so we need to be mindful about what gets collected, skipping anything that has a large repr or that doesn't provide useful context.
  A notable example is `self` in methods, which isn't useful to track, so we skip it by instrumenting methods with `#[instrument(skip(self))]`.

As for deciding *what* to instrument, we should consider instrumenting 1) anything that helps provide context for events (particularly ERROR events) and/or 2) anything that we're interested in profiling for performance.

* Subscribers can be configured to consume "span close" events, which include execution time details (see BOOM_SPAN_EVENTS in the readme).

* The `tracing_flame` crate uses spans to generate flamegraphs.

* Having instrumented functions also unlocks further observability capabilities that we could add in the future, such as metrics.

### Logging

Logging is done by issuing `tracing::Event`s, as with `tracing::info!`, `tracing::error!`, and other similar macros. These events are picked up by a subscriber, which can do various things with them, such as print them to the console or write them to a file. Currently we just log to the console.

Strive to logs things at the appropriate level:

* INFO level events should be reserved for reporting major stages in the app lifecycle.

* WARN is for minor issues or unexpected things that aren't quite errors but are more severe than INFO.

* ERROR is of course for actual errors.
  In general, we should only log errors where they're handled.
  A function doesn't need to log an error if it propagates the error back to the caller via `?`.

* DEBUG and TRACE are for when we need to rerun boom, usually locally, to get more information.

When we log an error we want as much context as possible, including the error itself, the cause chain, and any additional context that can be provided.

* The `log_error` and `as_error` macros help standardize how we report errors as ERROR events.
  See the `o11y` module for details

* Some errors occur more than once in a given function.
  `log_error`/`as_error` can be used to give each occurrence a unique event callsite and therefore a clear origin when logged, e.g., `foo().inspect_err(as_error!("an optional message for additional context"))?;`

## Generating flame graphs

Boom supports generating a flame graph to visualize performance bottlenecks:

1. Make sure you have `inferno` installed:
   ```bash
   cargo install inferno
   ```

2. Run boom with the `BOOM_FLAME_FILE` environment variable.
   This instructs boom to generate a flame graph and save the output at the given path:
   ```bash
   BOOM_FLAME_FILE=./tracing.folded cargo run --bin scheduler -- ztf
   ```

   Terminate boom when you're done profiling.

3. Turn the raw output into an interactive SVG with `inferno-flamegraph`:
   ```bash
   inferno-flamegraph <tracing.folded >tracing-flamegraph.svg
   ```

   The SVG file can be viewed in your browser.
