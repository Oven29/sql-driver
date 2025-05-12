from abc import ABC, abstractmethod
from typing import Any, List

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import select
import sqlalchemy

from src.schemas import Column, ColumnTypes, SchemaListData, TableData, TableSchema
from src.utils import cast_sql_value


class Interface(ABC):
    """
    Interface for drivers
    """
    _PAGE_SIZE = 100

    def __init__(self, **kwargs: str) -> None:
        if not 'db' in kwargs:
            raise AttributeError("db is required")

        if kwargs['db'] == 'sqlite':
            if not 'path' in kwargs:
                raise AttributeError("path is required for sqlite")

            db_url = f'sqlite:////{kwargs.pop("path")}'

        elif kwargs['db'] in ('mysql', 'postgresql'):            
            if not 'host' in kwargs:
                raise AttributeError(f"host is required for {kwargs['db']}")
            if not 'user' in kwargs:
                raise AttributeError(f"user is required for {kwargs['db']}")
            if not 'password' in kwargs:
                raise AttributeError(f"password is required for {kwargs['db']}")
            if not 'database' in kwargs:
                raise AttributeError(f"database is required for {kwargs['db']}")
        
            address = kwargs.pop("host")
            if 'port' in kwargs:
                address += f':{kwargs.pop("port")}'

            if kwargs['db'] == 'postgresql':
                db_url = f'postgresql+psycopg2://{kwargs.pop("user")}:{kwargs.pop("password")}@{address}/{kwargs.pop("database")}'
            else:
                db_url = f'mysql+pymysql://{kwargs.pop("user")}:{kwargs.pop("password")}@{address}/{kwargs.pop("database")}'

        else:
            raise AttributeError("db must be one of [sqlite, mysql, postgresql]")

        self._engine = sqlalchemy.engine.create_engine(
            db_url, echo=bool(kwargs.get('echo', False)), pool_timeout=int(kwargs.get('timeout', 15)))
        self.kwargs = kwargs
        self._debug = bool(kwargs.get('debug', False))

        self._available_commands = [
            'connect',
            'get',
            'create_table',
            'alter_table',
            'drop_table',
            'add_row',
            'add_column',
        ]

    @property
    def available_commands(self) -> List[str]:
        return self._available_commands

    @property
    def debug(self) -> TableSchema:
        return self._debug

    def execute_command(self, command: str, *args: str) -> Any:
        if not command in self._available_commands:
            raise AttributeError(f"Unknown command {command}")

        if command in ('create_table', 'alter_table'):
            if len(args) != 1:
                raise AttributeError(f"{command=} must have 1 arguments")
            return getattr(self, command)(TableSchema.model_validate_json(args[0]))

        elif command == 'add_column':
            if len(args) != 2:
                raise AttributeError(f"{command=} must have 2 arguments")
            return self.add_column(args[0], Column.model_validate_json(args[1]))

        else:
            return getattr(self, command)(*args)

    @abstractmethod
    def connect(self) -> SchemaListData:
        raise NotImplementedError

    @abstractmethod
    def get(self, tablename: str, page: int = 1) -> TableData:
        raise NotImplementedError

    @abstractmethod
    def create_table(self, data: TableSchema) -> TableSchema:
        raise NotImplementedError

    @abstractmethod
    def drop_table(self, tablename: str) -> TableSchema:
        raise NotImplementedError

    @abstractmethod
    def alter_table(self, tablename: str, data: TableSchema) -> TableSchema:
        raise NotImplementedError

    @abstractmethod
    def add_row(self, tablename: str, *data: str) -> TableSchema:
        raise NotImplementedError

    @abstractmethod
    def add_column(self, tablename: str, data: Column) -> TableSchema:
        raise NotImplementedError


class Driver(Interface):
    def _get_table_model(self, tablename: str) -> sqlalchemy.Table:
        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=self._engine)
        if not tablename in metadata.tables:
            raise ValueError(f"Table {tablename=} not found")

        return metadata.tables[tablename]

    def _get_db_schema(self) -> List[TableSchema]:
        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=self._engine)
        tables = []

        for tn, t in metadata.tables.items():
            tables.append(TableSchema(
                title=tn,
                columns=[Column(
                    name=c.name,
                    nullable=c.nullable,
                    primary_key=c.primary_key,
                    type=ColumnTypes.get(type(c.type)),
                ) for c in t.columns]
            ))

        return tables

    def _get_table_schema(self, tablename: str) -> TableSchema:
        tables = self._get_db_schema()

        for table in tables:
            if table.title == tablename:
                return table

        raise ValueError(f"Table {tablename=} not found")

    def connect(self) -> SchemaListData:
        return SchemaListData(items=self._get_db_schema())

    def get(self, tablename: str, page: int = 1) -> TableData:
        with self._engine.connect() as con:
            page = int(self.kwargs.get('page', page)) - 1
            stmt = con.execute(
                select(self._get_table_model(tablename)).offset(page * self._PAGE_SIZE).limit(self._PAGE_SIZE)
            )
            data = stmt.fetchall()
            table_schema = self._get_table_schema(tablename)
            table = TableData(title=tablename, columns=table_schema.columns, page=page + 1, count=len(data))
            for row in data:
                table.data.append(list(map(str, row)))

            return table

    def create_table(self, data: TableSchema) -> TableSchema:
        with self._engine.connect() as con:
            try:
                self._get_table_model(data.title)
            except ValueError:
                pass
            else:
                raise ValueError(f"Table '{data.title}' already exists")

            columns = [sqlalchemy.Column(col.name, col.get_sqlaclhemy_type(), nullable=col.nullable, primary_key=col.primary_key) for col in data.columns]
            table_model = sqlalchemy.Table(data.title, sqlalchemy.MetaData(), *columns)
            table_model.create(bind=self._engine)
            con.commit()

            return data

    def alter_table(self, data: TableSchema) -> TableSchema:
        with self._engine.connect() as connection:
            ctx = MigrationContext.configure(connection)
            op = Operations(ctx)

            existing_schema = self._get_table_schema(data.title)
            existing_columns = {col.name: col for col in existing_schema.columns}
            new_columns = {col.name: col for col in data.columns}

            # Добавить недостающие колонки
            for colname, new_col in new_columns.items():
                if colname not in existing_columns:
                    op.add_column(
                        data.title,
                        sqlalchemy.Column(
                            new_col.name,
                            new_col.get_sqlaclhemy_type(),
                            nullable=new_col.nullable,
                            primary_key=new_col.primary_key
                        )
                    )

            # Удалить старые колонки (если поддерживается)
            for colname in existing_columns:
                if colname not in new_columns:
                    try:
                        op.drop_column(data.title, colname)
                    except Exception as e:
                        if self.debug:
                            print(f"Can't drop column {colname}: {e}")

            return data

    def drop_table(self, tablename: str) -> TableSchema:
        schema = self._get_table_schema(tablename)
        self._get_table_model(tablename).drop(bind=self._engine)
        return schema

    def add_row(self, tablename: str, *data: str) -> TableSchema:
        table_schema = self._get_table_schema(tablename)

        with self._engine.connect() as con:
            values = {}
            i = -1
            for col in table_schema.columns:
                if col.name in self.kwargs:
                    values[col.name] = cast_sql_value(self.kwargs[col.name], col.type)
                elif not col.primary_key and len(data) > i + 1:
                    values[col.name] = cast_sql_value(data[i := i + 1], col.type)

            con.execute(self._get_table_model(tablename).insert().values(values))
            con.commit()

        return table_schema

    def add_column(self, tablename: str, data: Column) -> TableSchema:
        with self._engine.connect() as connection:
            ctx = MigrationContext.configure(connection)
            op = Operations(ctx)

            op.add_column(
                tablename,
                sqlalchemy.Column(
                    data.name,
                    data.get_sqlaclhemy_type(),
                    nullable=data.nullable,
                    primary_key=data.primary_key
                )
            )

        return self._get_table_schema(tablename)
