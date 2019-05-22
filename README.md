# wrc-timing
Data grabber for WRC timing / classification data


__THE FOLLOWING MAY BE WRONG... CHECK THE CODE TO BE SURE!__

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
Usage: wrc_get [OPTIONS] NAME [STAGES]...

  Get stages for a given rally.

Options:
  --year INTEGER         Year results are required for (defaults to current
                         year)
  --dbname TEXT          SQLite database name
  --running              Only grab stages that are running
  --default-stages TEXT  If no stages specified, which do we grab?
  --help                 Show this message and exit.
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
