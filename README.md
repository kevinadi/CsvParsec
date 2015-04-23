# CsvParsec

Usage: 
------
CsvParser [-d|input_file] -t[table_name] [-sql] 1>[cleaned_output] 2>[sql_output]

* input_file: Name of the input CSV file. If the argument "-d" is supplied, input is taken from stdin instead.
* table_name: SQL table name to be used in the CREATE TABLE statement. **Note:** there is no space between "-t" and the table name.
* -sql      : Dump everything as SQL INSERT statements instead of CSV (currently not used).
* cleaned_output : Cleaned CSV output, delimited using | and ~ (writes output to stdout).
* sql_output     : CREATE TABLE output & any rows that cannot be cleaned automatically (writes output to stderr).


Example:
--------
File to be processed: ACT.csv

### Step 1 - Run command
Command: CsvParser ACT.csv -tACT_UNICODE 1>ACT_UNICODE.csv 2>ACT_UNICODE.sql

The result of the above command will be two files: ACT_UNICODE.csv (cleaned CSV file with ~ and | as delimiters), and ACT_UNICODE.sql

### Step 2 - Verify that the SQL file is generated correctly
- The CREATE TABLE statement should create a table called ACT_UNICODE (as per the -t setting).
- See if there is any SQL INSERT statement in the file. This is the data rows that are either:
    - Have character count larger than 3000 characters (which will reliably crash MSSQL import tool).
    - Have some form of unrecoverable error, so must be manually inspected.
- Verify that the [UniqueID] column is specified as NVARCHAR(255) so it can be indexed.

### Step 3 - Verify that the CSV file is generated correctly
- The generated CSV file should have NO HEADER.
- Take note of the total number of lines. This will correspond to the number of imported rows in the MSSQL import tool.


Note:
-----
- Running CsvParser without the 1>cleaned_output and 2>sql_output parameters will just dump the outputs into screen.
- Dumping into screen may mix up the cleaned output and SQL output, depending on the shell used and how much data is present in each stream.
- The symbol 1> means redirect stdout to file.
- The symbol 2> means redirect stderr to file.


To-Do:
------
- The CSV output is hardcoded to use "|" and "~" as column separator & field delimiter, respectively. This should not be hardcoded.
- The SQL output format is hardcoded to use MSSQL syntax.

