# This is a list of table 'names' that will cause the generator to throw an error if encountered in a set of input data.
# These are typically Snowflake keywords, and if they are present it is an indicating that the input data has not been
# cleaned correctly.
# This should NOT be used a method for excluding actual tables. The content of the input file should solely drive
# what tables are processed by the generator.

invalid_table_names: list[str] = [
    "APPLICABLE_ROLES",
    "COLUMNS",
    "DATABASES",
    "ENABLED_ROLES",
    "APPLICABLE_ROLES",
    "COLUMNS",
    "DATABASES",
    "ENABLED_ROLES",
    "EXTERNAL_TABLES",
    "FILE_FORMATS",
    "FUNCTIONS",
    "INFORMATION_SCHEMA_CATALOG_NAME",
    "LOAD_HISTORY",
    "OBJECT_PRIVILEGES",
    "PACKAGES",
    "PIPES",
    "PROCEDURES",
    "REFERENTIAL_CONSTRAINTS",
    "REPLICATION_DATABASES",
    "SCHEMATA",
    "SEQUENCES",
    "STAGES",
    "TABLE_CONSTRAINTS",
    "TABLE_PRIVILEGES",
    "TABLE_STORAGE_METRICS",
    "TABLES",
    "USAGE_PRIVILEGES",
    "VIEWS",
]
