import re
import sys
from typing import Dict, List, Tuple
from datetime import datetime, date, time
from uuid import UUID
from decimal import Decimal
from typing import Any
import base64

from src.schemas import ColumnTypes


def to_snake_case(text: str) -> str:
    """
    Convert string to snake case

    :param text: text for convert
    :return: snake case string
    """
    if text is None:
        return None
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', text).lower()


def parse_args() -> Tuple[Dict[str, str], str, List[str]]:
    """
    Parse args
    :return: tuple of kwargs, command, command args
    """
    kwargs, command, command_args = {}, None, []
    for arg in sys.argv[1:]:
        if arg.startswith('--'):
            key, value = arg[2:].split('=')
            kwargs[key] = value
        elif not command:
            command = arg
        else:
            command_args.append(arg)

    return kwargs, to_snake_case(command), command_args


def cast_sql_value(value_str: str, column_type: ColumnTypes) -> Any:
    """
    Cast value to sql type
    
    :param value_str: value for cast
    :param column_type: sql type
    :return: casted value
    """
    if value_str is None:
        return None

    value_str = value_str.strip()

    try:
        match column_type:
            case ColumnTypes.INTEGER | ColumnTypes.SMALLINT | ColumnTypes.BIGINT:
                return int(value_str)
            case ColumnTypes.REAL | ColumnTypes.FLOAT | ColumnTypes.DOUBLE | ColumnTypes.DOUBLE_PRECISION:
                return float(value_str)
            case ColumnTypes.NUMERIC | ColumnTypes.DECIMAL:
                return Decimal(value_str)
            case ColumnTypes.BOOLEAN:
                return value_str.lower() in ("1", "true", "yes", "on")
            case ColumnTypes.TIMESTAMP | ColumnTypes.DATETIME:
                return datetime.fromisoformat(value_str)
            case ColumnTypes.DATE:
                return date.fromisoformat(value_str)
            case ColumnTypes.TIME:
                return time.fromisoformat(value_str)
            case ColumnTypes.UUID:
                return UUID(value_str)
            case ColumnTypes.BLOB | ColumnTypes.BINARY | ColumnTypes.VARBINARY:
                # Можно использовать base64 для сериализации бинарных данных
                return base64.b64decode(value_str)
            case ColumnTypes.TEXT | ColumnTypes.CLOB | ColumnTypes.VARCHAR | ColumnTypes.NVARCHAR | ColumnTypes.CHAR | ColumnTypes.NCHAR:
                return value_str
            case _:
                raise ValueError(f"Unknown column type: {column_type}")
    except Exception as e:
        raise ValueError(f"Error while casting value '{value_str}' of type {column_type}: {e}")
