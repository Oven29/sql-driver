from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field
import sqlalchemy


class ColumnTypes(str, Enum):
    REAL = 'REAL'
    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE'
    DOUBLE_PRECISION = 'DOUBLE_PRECISION'
    NUMERIC = 'NUMERIC'
    DECIMAL = 'DECIMAL'
    INTEGER = 'INTEGER'
    SMALLINT = 'SMALLINT'
    BIGINT = 'BIGINT'
    TIMESTAMP = 'TIMESTAMP'
    DATETIME = 'DATETIME'
    DATE = 'DATE'
    TIME = 'TIME'
    TEXT = 'TEXT'
    CLOB = 'CLOB'
    VARCHAR = 'VARCHAR'
    NVARCHAR = 'NVARCHAR'
    CHAR = 'CHAR'
    NCHAR = 'NCHAR'
    BLOB = 'BLOB'
    BINARY = 'BINARY'
    VARBINARY = 'VARBINARY'
    BOOLEAN = 'BOOLEAN'
    UUID = 'UUID'

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
