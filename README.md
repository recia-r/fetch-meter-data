# Overview

Fetches meter data from watersignal api and writes it to a snowflake table. Supports optionsl local ASCII export of data in addition to transferring to Snowflake.

# Configuration

The configuration file `config.json` contains all of the configuration options and should be supplied to the script.

# Running

First, you must have java installed.

From the command line:

`java -jar meterdata.jar -s <YYYY-MM-DD-hh-mm> -e <YYYY-MM-DD-hh-mm> <CONFIG_PATH>`

For example:

`java -jar meterdata.jar -s 2022-09-01-00-00 -e 2022-09-30-23-00 config.json`

will return data for all the days in Sept 2022, and it will use the configuration in the `config.json` file.

## Default Rate Range

If omitted `sdate` and `edate` default to the 7 day period ending 23:59 yesterday (not including current day)

For example: running `java -jar meterdata.jar config.json` at any time on `2022-09-08` would fetch data from `2022-09-01-00-00` to `2022-09-07-23-00`.


## Options

-s, --sdate DATE  
&nbsp;&nbsp; Earliest date of date range, ISO 8601 format, (YYYY-MM-DD-HH-mm)  
-e, --edate DATE      
&nbsp;&nbsp; Latest date of date range, ISO 8601 format, (YYYY-MM-DD-HH-mm)  
-c, --compound Y/N    
&nbsp;&nbsp; WaterSignal to calculate compound usage before returning data, String, "Y" or "N" (Default: "N")  
-S, --Silent        
&nbsp;&nbsp; Supress output to stdout, but still generate log.
-x, --xport  
&nbsp;&nbsp; Local ASCII export of data in addition to transferring to Snowflake. Export directory can be specified on config file - defaults to `export`.  
-h, --help   
&nbsp;&nbsp; Print help and exit.

# Logs

Log directory can be specified in the config file - defaults to `logs`.# Overview

Fetches meter data from watersignal api and writes it to a snowflake table. Supports optionsl local ASCII export of data in addition to transferring to Snowflake.

# Configuration

The configuration file `config.json` contains all of the configuration options and should be supplied to the script.

# Running

First, you must have java installed.

From the command line:

`java -jar meterdata.jar -s <YYYY-MM-DD-hh-mm> -e <YYYY-MM-DD-hh-mm> <CONFIG_PATH>`

For example:

`java -jar meterdata.jar -s 2022-09-01-00-00 -e 2022-09-30-23-00 config.json`

will return data for all the days in Sept 2022, and it will use the configuration in the `config.json` file.

## Default Rate Range

If omitted `sdate` and `edate` default to the 7 day period ending 23:59 yesterday (not including current day)

For example: running `java -jar meterdata.jar config.json` at any time on `2022-09-08` would fetch data from `2022-09-01-00-00` to `2022-09-07-23-00`.


## Options

-s, --sdate DATE  
&nbsp;&nbsp; Earliest date of date range, ISO 8601 format, (YYYY-MM-DD-HH-mm)  
-e, --edate DATE      
&nbsp;&nbsp; Latest date of date range, ISO 8601 format, (YYYY-MM-DD-HH-mm)  
-c, --compound Y/N    
&nbsp;&nbsp; WaterSignal to calculate compound usage before returning data, String, "Y" or "N" (Default: "N")  
-S, --Silent        
&nbsp;&nbsp; Supress output to stdout, but still generate log.
-x, --xport  
&nbsp;&nbsp; Local ASCII export of data in addition to transferring to Snowflake. Export directory can be specified on config file - defaults to `export`.  
-h, --help   
&nbsp;&nbsp; Print help and exit.

# Logs

Log directory can be specified in the config file - defaults to `logs`.