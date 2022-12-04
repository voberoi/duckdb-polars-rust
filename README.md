# duckdb-polars-rust

This repo shows an end-to-end example of how to go from a DuckDB query
to a Polars dataframe in Rust using DuckDB's Arrow query interface.

It's best paired with this blog post: [not published yet].

## Note

The example here uses Rust's bindings for DuckDB's C API,  [libduckdb-sys](https://lib.rs/crates/libduckdb-sys). A friendlier API exists in [duckdb-rs](https://crates.io/crates/duckdb),
but as of publishing this code it does does not support DuckDB's nested types (lists, structs, maps).

If you don't need those, you may be better off simply using [duckdb-rs](https://crates.io/crates/duckdb). The code to use Polars with results from that API will be different.

Credit to `kylebarron` on the Polars Discord for the following info if you use `duckdb-rs` instead:

> ... there are two different arrow implementations in rust, arrow and arrow2. The duckdb crate uses arrow while polars uses arrow2, so you can't automatically pass a polars chunk (i.e. an arrow2 record batch) to the duckdb crate. But you should be able to use the Arrow C data interface to pass data from arrow2 to arrow without a copy. So (I think) you should be able to do polars dataframe -> arrow2 chunks -> arrow ffi -> arrow record batches -> duckdb crate. And then not need to do any manual pointer work or any of your own unsafe code

## Contributing

Did you find an error? Is this example not up-to-date? Want to improve this code? Feel free to open a PR.

* The code uses a large unsafe block that can be reduced in size.
* A good next step might be to create an easier-to-use iterator over these query results.

## Questions?

Open an issue or say hi on [Twitter](https//www.twitter.com).
