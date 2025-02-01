-- Create a catalog for the Snowflake data source
CREATE FOREIGN CATALOG snowflake_catalog USING CONNECTION `snowflake-connection` OPTIONS (database 'HOUSE_PRICES');
