# wrc-timing
Data grabber for WRC timing / classification data

```
Usage: wrc_rallies [OPTIONS]

  Show available rallies.

Options:
  --year INTEGER  Year results are required for (defaults to current year)
  --help          Show this message and exit.
```

```
Usage: wrc_championship_data [OPTIONS] COMMAND

  Get championship details.

Options:
  --year INTEGER  Year results are required for (defaults to current year)
  --dbname TEXT   SQLite database name
  --help          Show this message and exit.

```

```
Usage: wrc_get_stage [OPTIONS] NAME [STAGES]...

  Get one or more stages.

Options:
  --year INTEGER  Year results are required for (defaults to current year)
  --dbname TEXT   SQLite database name
  --help          Show this message and exit.
  ```
 
  
  ```
  Usage: wrc_get_all [OPTIONS] NAME

  Get all stages for a given rally.

Options:
  --year INTEGER  Year results are required for (defaults to current year)
  --dbname TEXT   SQLite database name
  --help          Show this message and exit.
  ```
  
  
  ```
  Usage: wrc_full_run [OPTIONS] NAME

  Get all data for all rallies in a year.

Options:
  --year INTEGER  Year results are required for (defaults to current year)
  --dbname TEXT   SQLite database name
  --help          Show this message and exit.
  ```
  
  ```
  Usage: wrc_set_metadata [OPTIONS]

  Refresh WRC metadata in database.

Options:
  --help  Show this message and exit.
  ```
