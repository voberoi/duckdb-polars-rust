use arrow2::array::{Array, Float64Array, Int32Array, StructArray};

use libduckdb_sys::*;
use polars::prelude::*;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::{mem, ptr};

fn main() {
    let mut db: duckdb_database = ptr::null_mut();
    let mut conn: duckdb_connection = ptr::null_mut();

    unsafe {
        // Open a DuckDB connection in memory.
        if duckdb_open(ptr::null_mut(), &mut db) != duckdb_state_DuckDBSuccess {
            panic!("duckdb_open error");
        }
        if duckdb_connect(db, &mut conn) != duckdb_state_DuckDBSuccess {
            panic!("duckdb_connect error");
        }

        // Querying parquet files requires loading the parquet extension.
        execute_statement(conn, "INSTALL parquet");
        execute_statement(conn, "LOAD parquet");

        let sql = "
        SELECT date_trunc('day', tpep_pickup_datetime) AS pickup_date, \
        SUM(passenger_count) AS total_passenger_count, \
        SUM(trip_distance) AS total_trip_distance, \
        SUM(tip_amount) AS total_tip_amount, \
        SUM(tolls_amount) AS total_tolls_amount, \
        SUM(improvement_surcharge) AS total_improvement_surcharge, \
        SUM(total_amount) AS total_amount \
        FROM 'yellow_tripdata_2022-01.parquet' \
        GROUP BY 1";

        let sql = CString::new(sql).unwrap();

        // This executes the query and prepares a data structure we use to fetch
        // batches of results in Arrow arrays. `duckdb_arrow` is an alias for
        // `void *` in DuckDB's C API. I don't know what is stored at this
        // address once we execute `duckdb_query_arrow`, but we use it to
        // consume results in the loop below.
        let mut result: duckdb_arrow = ptr::null_mut();
        let state = duckdb_query_arrow(conn, sql.as_ptr(), &mut result);
        if state == duckdb_state_DuckDBError {
            let error_message: *const c_char = duckdb_query_arrow_error(result);
            let error_message = CStr::from_ptr(error_message).to_str().unwrap();
            panic!("{}", error_message);
        }

        // Time to consume the results of the query and do something with it
        // using polars. Here we're going to:
        //
        // 1. Fetch a batch of results into an Arrow array. This is a C struct.
        // 2. Convert that Arrow array into a safer and easier-to-use Rust arrow2::Array.
        // 3. Construct a Polars dataframe from that arrow2::Array.
        // 4. Do some computation over the batch of results.
        //
        // We need to keep track of the result count so we break when all results
        // have been consumed.

        let mut record_count = 0;
        loop {
            if record_count == duckdb_arrow_row_count(result).try_into().unwrap() {
                break;
            }

            ///////////////////////////////////////////////////////////////////
            //               1. Fetch a batch of arrow results.              //
            ///////////////////////////////////////////////////////////////////

            // arrow2::ffi::{ArrowArray, ArrowSchema} are representations of
            // these structs:
            //
            // https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
            let mut ffi_arrow_array: arrow2::ffi::ArrowArray = arrow2::ffi::ArrowArray::empty();
            let state = duckdb_query_arrow_array(
                result,
                &mut &mut ffi_arrow_array as *mut _ as *mut *mut c_void, // Help me understand this!! I got it from duckdb-rs.
            );

            if state != duckdb_state_DuckDBSuccess {
                panic!("duckdb_query_arrow_array error");
            }

            let mut schema = &arrow2::ffi::ArrowSchema::empty();
            let schema = &mut schema;
            let state = duckdb_query_arrow_schema(result, schema as *mut _ as *mut *mut c_void);
            if state != duckdb_state_DuckDBSuccess {
                panic!("duckdb_query_arrow_schema error");
            }

            ///////////////////////////////////////////////////////////////////
            //      2. Convert the C Arrow array into an arrow2::Array.      //
            ///////////////////////////////////////////////////////////////////

            let field = arrow2::ffi::import_field_from_c(schema).unwrap();
            let arrow_array =
                arrow2::ffi::import_array_from_c(ffi_arrow_array, field.data_type).expect("ok");

            ///////////////////////////////////////////////////////////////////
            //     3. Construct a polars dataframe from an arrow::Array.     //
            ///////////////////////////////////////////////////////////////////

            // We know our query is going to return a timestamp followed by 6
            // floats. Each of these columns will be a series in our dataframe.
            //
            // DuckDB materializes its results in a StructArray:
            // https://docs.rs/arrow2/latest/arrow2/array/struct.StructArray.html
            //
            // StructArrays just represent multiple arrays with the same number
            // of rows. We need to take each array in the StructArray and turn
            // it into a polars Series.

            let struct_array = arrow_array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("This Arrow Array should be a StructArray.");

            /*
             * If you want to find out what Arrow data types are being returned
             * from your query it's helpful to print out the StructArray's fields.
             *
             * println!("{:?}", struct_array.fields());
             *
             * Each array in the StructArray will need to be downcast to the
             * proper type.
             *
             * All Arrow datatypes are here:
             * https://docs.rs/arrow2/latest/arrow2/datatypes/enum.DataType.html
             *
             * You can use all this info to do this dynamically. The example below
             * is specific to the query we run above.
             */

            // A DataFrame is a vector of Series.
            let mut df_series: Vec<Series> = vec![];

            // The Arrow DataType for dates is Date32, which are are signed
            // 32-bit integers.
            let pickup_date_series = Series::try_from((
                "pickup_date",
                struct_array.values()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .to_boxed(),
            ))
            .unwrap();

            df_series.push(pickup_date_series);

            let series_names = vec![
                (1, "daily_passenger_count"),
                (2, "daily_trip_distance"),
                (3, "daily_tip_amount"),
                (4, "daily_tolls_amount"),
                (5, "daily_improvement_surcharge"),
                (6, "daily_total"),
            ];

            for (idx, name) in series_names {
                let series = Series::try_from((
                    name,
                    struct_array.values()[idx]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .to_boxed(),
                ))
                .unwrap();
                df_series.push(series);
            }

            let df = DataFrame::new(df_series).unwrap();

            ///////////////////////////////////////////////////////////////////
            //           4. Do some computation over the dataframe.          //
            ///////////////////////////////////////////////////////////////////

            let out = df.sum();
            println!("{}", out);

            record_count += arrow_array.len();
        }

        // I think we have actually have to call duckdb_query_arrow one more time.
        // We don't care about the result -- it cleans up/frees the previous results
        // it returns.
        //
        // See: https://duckdb.org/docs/api/c/api#duckdb_query_arrow_array
        //
        // The docs don't state this situation specifically, but if that call frees
        // the previous `out_array`, then presumably we'd have a memory leak if
        // we didn't do this.
        //
        // I might be wrong about this. This program doesn't crash, though -- I
        // think that is a good sign.
        let mut ffi_arrow_array: arrow2::ffi::ArrowArray = arrow2::ffi::ArrowArray::empty();
        let state = duckdb_query_arrow_array(
            result,
            &mut &mut ffi_arrow_array as *mut _ as *mut *mut c_void, // Help me understand this!! I got it from duckdb-rs.
        );
        if state == duckdb_state_DuckDBError {
            let error_message: *const c_char = duckdb_query_arrow_error(result);
            let error_message = CStr::from_ptr(error_message).to_str().unwrap();
            panic!("{}", error_message);
        }

        // Destroy the result struct. We're done with it.
        duckdb_destroy_arrow(&mut result);
    }
}

/*
 * Executes a statement without fetching any results.
 */
unsafe fn execute_statement(conn: duckdb_connection, statement: &str) {
    let statement = CString::new(statement).unwrap();

    // DuckDB's C API has two query functions:
    //
    // 1. duckdb_query (demonstrated below)
    // 2. duckdb_query_arrow (demonstrated in main())
    //
    // This is an example of using duckdb_query to execute a statement.
    let mut result: duckdb_result = mem::zeroed();
    let state = duckdb_query(conn, statement.as_ptr(), &mut result);

    if state == duckdb_state_DuckDBError {
        let error_message: *const c_char = duckdb_result_error(&mut result);
        let error_message = CStr::from_ptr(error_message).to_str().unwrap();
        panic!("{}", error_message);
    }

    duckdb_destroy_result(&mut result);
}
