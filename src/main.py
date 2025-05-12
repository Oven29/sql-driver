import json
from pprint import pprint
from pydantic import BaseModel
import traceback

from src.driver import Driver
from src.schemas import Answer
from src.utils import parse_args


def main() -> None:
    kwargs, command, command_args = parse_args()

    try:
        driver = Driver(**kwargs)
        res: BaseModel = driver.execute_command(command, *command_args)
        answer = Answer(ok=True, data_type=res.__class__.__name__, data=res)

    except Exception as e:
        answer = Answer(ok=False, error_message=str(e))

    if driver.debug:
        pprint(json.loads(answer.model_dump_json()))
    else:
        print(answer.model_dump_json())


if __name__ == '__main__':
    main()
