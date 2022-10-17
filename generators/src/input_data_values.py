from enum import Enum


class ColumnTypes(Enum):
    unique_key = "unique_key"
    updated_date = "updated_date"


class CuratedDBTType(Enum):
    scd2 = "scd2"
