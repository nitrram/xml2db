# Prerequisities
`sqlx-cli` is needed to cache the database to be worked with.

First `cargo install sqlx-cli` if it's not present at your environment. Then it's good to set the `DATABSE_URL` environmental variable, which you can do via creating `.env` file in the root of the project.

Then just trigger `cargo sqlx prepare` to cache a db. 

# Usage
`cargo xml run <input_file.xml> $DATABASE_URL`
