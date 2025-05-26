from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field
import sqlalchemy


class ColumnTypes(str, Enum):
    ARRAY = 'ARRAY'
    BIGINT = 'BIGINT'
    BINARY = 'BINARY'
    BLOB = 'BLOB'
    BOOLEAN = 'BOOLEAN'
    CHAR = 'CHAR'
    CLOB = 'CLOB'
    DATE = 'DATE'
    DATETIME = 'DATETIME'
    DECIMAL = 'DECIMAL'
    DOUBLE = 'DOUBLE'
    FLOAT = 'FLOAT'
    INT = 'INT'
    INTEGER = 'INTEGER'
    JSON = 'JSON'
    NCHAR = 'NCHAR'
    NULLTYPE = 'NULLTYPE'
    NUMERIC = 'NUMERIC'
    NVARCHAR = 'NVARCHAR'
    REAL = 'REAL'
    SMALLINT = 'SMALLINT'
    STRINGTYPE = 'STRINGTYPE'
    TEXT = 'TEXT'
    TIME = 'TIME'
    TIMESTAMP = 'TIMESTAMP'
    UUID = 'UUID'
    VARBINARY = 'VARBINARY'
    VARCHAR = 'VARCHAR'
    DOUBLE_PRECISION = 'DOUBLE_PRECISION'

    @classmethod
    def get(cls, type) -> 'ColumnTypes':
        return cls._member_map_[str(type).rsplit('.', 1)[-1].split("'", 1)[0].upper()]


class Column(BaseModel):
    name: str
    nullable: bool = Field(default=False)
    primary_key: bool = Field(default=False)
    type: ColumnTypes

    def get_sqlaclhemy_type(self) -> str:
        return getattr(sqlalchemy, self.type)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Column):
            return NotImplemented
        return self.name == other.name and self.nullable == other.nullable and self.primary_key == other.primary_key and self.type == other.type


class TableSchema(BaseModel):
    title: str
    columns: List[Column]


class TableData(TableSchema):
    data: List[List[str]] = Field(default=[])
    page: int = Field(default=0)
    count: int = Field(default=0)


class SchemaListData(BaseModel):
    items: List[TableSchema]


class Answer(BaseModel):
    ok: bool
    data_type: Optional[str] = Field(default=None)
    data: TableSchema | TableData | SchemaListData = Field(default=None)
    error_message: Optional[str] = Field(default=None)
