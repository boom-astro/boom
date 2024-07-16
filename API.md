#### BOOM API

This is a WIP API for the BOOM project. It is a RESTful API that allows users to interact with the BOOM's MongoDB database with a simplified astro-like syntax.

Here we use Actix-web as the web framework and MongoDB as the database. For now, we only implement the following query_type (as in Kowalski):
- `find`
- `sample`
- `info`
- `cone_search`

`sample` is a query_type that is not found in Kowalski, but it is a simple query that returns a random sample of up to 100 documents in a given collection. It's technically a `find` query but does not expect a filter or a projection, and only takes an optional size parameter (default is 1, max is 100). This is useful for quickly checking the contents of a collection, and gives the query_type a more intuitive name.

To run the API in development mode (where changes are automatically reloaded), simply install cargo-watch with `cargo install cargo-watch` and run `cargo watch -x run`. This will start the API on `localhost:4000`. To run the API in production mode, run `cargo run --release`.

PS: The initial build takes some time, but subsequent builds are much faster. The API is currently in development and is not yet ready for production use.