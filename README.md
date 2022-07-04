### Command fred2ch

This is a simple command that pulls a single series from the St Louis Federal Reserve database
Fred II then creates and populates a ClickHouse table for it.

Required command line arguments:

    -series  Fred II series id
    -table   destination ClickHouse table.
    -api     Fred II API key

Optional command line arguments:

    -host           IP of ClickHouse database. Default: 127.0.0.1
    -user           ClickHouse user. Default: "default"
    -password       ClickHouse password. Default: ""

The table created has these fields:

     seriesId    String     series ID requested
     date        Date       date of metric value
     value       Float32    value of metric

All months available for the series are loaded.

Series names are case-insensitive.