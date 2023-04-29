# Relational GraphQL

Relational GraphQL is an ORM designed to efficiently translate GraphQL queries into queries against
a relational database.

## Features
* Easy setup: simply derive `Resource` on your Rust types and you're good to go!
* Powerful: give your API users safe access to a powerful, expressive query language that combines
  the best features of SQL and GraphQL
* Efficient: automatically leverage the performance of SQL indexes and joins
* Safe: the generated object types enjoy strong free theorems which prevent many classes of bugs in
  this library at compile time, thanks to a frankly ill-advised amount of type-level metaprogramming

### Near-term Roadmap
* Predicates on relations
* Enums
* Connection and edge fields
* One-to-one relationships (optional on both ends)
* Multiple references to the same resource, using aliases
* Efficient paging
* Sorting

### Coming in Future Versions
* Modular: easily add a powerful SQL data source to your existing GraphQL application, and compose
  it with other data sources
* Incremental: easily add GraphQL queries to your existing SQL database
* Flexible: don't like Postgres? Write your own backend to use a different relational database!
  More built-in backends to come in the future.

## Development

### Get the code

```bash
git clone git@github.com:jbearer/relational-graphql.git
cd relational-graphql
```

## Install Nix

This project uses [Nix](https://nixos.org/) as a one-stop shop for managing dependencies. While it
is possible to develop on this project without Nix, by manually installing
[Cargo](https://doc.rust-lang.org/cargo/) and related dependencies, we highly recommend using Nix to
get everything in one shot and be sure you are using the same versions as other developers.

If you don't already have Nix on your system, install it by following the instructions
[here](https://nixos.org/download.html). Once installed, you can enter a shell with up-to-date
versions of all the necessary dependencies in scope simply by running `nix shell` from the project
root.

Optionally, you can also install [direnv](https://direnv.net/), which will automatically drop you into the Nix
shell whenever you enter the project directory. Use `direnv allow` to enable this convenience after
installing. Once in the Nix shell, you can use `direnv deny` if you ever need to drop out of it.

## Build and Test

The whole project is a Cargo workspace, so to build everything you can simply run

```bash
cargo build --workspace
```

Use `cargo clippy` to run the linter and `cargo test` to run unit tests.

Note that some of the Cargo tests require a test PostgreSQL server to be running. You can use
`bin/start-test-db` to start a test server. You may need to use `sudo`. Once the server is
running, set `POSTGRES_TESTS=1` to enable these tests, and then run `cargo test` as usual.
`POSTGRES_TESTS_PORT` can be used to change the port that the PostgreSQL server runs on (the default
is 5432, the default for Postgres in general). You can use `POSTGRES_TESTS_PASSWORD` to change the
password for the test database (teh default is `password`).
