import sys

from loguru import logger


class RapidsSparkLogger:

    def __init__(self):
        logger.configure(
            handlers=[
                # dict(sink=sys.stderr, format="[{time}] {message}", backtrace=False, ),
                dict(sink=sys.stdout, format="[{time}] {message}", backtrace=True, ),
                dict(sink="resources/log/file.log", enqueue=True, serialize=True, backtrace=False, ),
            ],
            levels=[dict(name="NEW", no=13, icon="¤", color="")],
            extra={"common_to_all": "default"},
            patcher=lambda record: record["extra"].update(some_value=42),
            activation=[("my_module.secret", False), ("another_library.module", False)],
        )