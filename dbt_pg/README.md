# Personal docs for dbt project

## Models

bronze: views for the staging schema that comes from MongoDB after some preprocessing
with polars (python).

silver: exploded/flattened and filtered data from bronze by semantic layers.

gold: star schema modeled tables from silver table to be transferred to or
queried via federation using DuckDB.

## Running CLI

`dbt deps` to install needed packages
`dbt run` to run all queries for model creations
`dbt test` to run all tests for models
`dbt docs generate` to generate the documentations for dbt project
`dbt docs serve` to serve the html of dbt project documentations (depends on `dbt docs generate`)
    - use --port 8580 because 8080 is used by Airflow
`dbt clean` to reset the working environment: dbt_packages/ target/ folders