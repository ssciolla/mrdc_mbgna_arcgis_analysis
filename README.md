# ArcGIS to MySQL Transformation Script

#### Script Author(s): Sam Sciolla
#### Created as part of the Migrating Research Data Collections project

## Purpose

This repository contains a Python script designed to convert the table and relationship JSON data about an ArcGIS database (fetched using a REST API) into a MySQL script with CREATE statements for the tables and layers. By using software such as [MySQL Workbench](https://www.mysql.com/products/workbench/), this script can be used to simply create an enhanced entity-relationship diagram.

## Use

To run this script, type the following at your command prompt of choice.

`python transform_arcgis_json_to_sql.py`

## Input

This script has no file inputs. By default, the script will collect data from the URL for the Matthaei-Nichols Living Collections Database, a live ArcGIS database. To use the script on another database, simply change the values for `arcgis_database_base_url` and `new_database_name` under Main Program (at the end of the script). The URL should point to a Services Directory for the database. The [ArcGIS REST API](https://developers.arcgis.com/rest/services-reference/using-the-services-directory.htm) provides documentation on Services Directories and their use. `new_database_name` should contain the name of a MySQL database that the SQL output might be executed against.

## Output

This script will produce three files: `arcgis_response.json`, `relationship_matches.json`, and `database_tables.sql`. `arcgis_response.json` contains the JSON response received from the ArcGIS REST API (keep in mind responses are cached; delete the cache file to collect new data). `relationship_matches.json` stores reconfigured data about table relationships, resulting in a record for each relationship that includes the names of both tables and cardinalities. `database_tables.sql` lays out the tables described in JSON as CREATE TABLE statements, ordered so that tables with foreign keys follow the tables they reference.

## Dependencies

This script uses the [requests](https://2.python-requests.org/en/master/) library to interact with the ArcGIS REST API. It also uses the `json` module, a standard library.

## Rights

...
