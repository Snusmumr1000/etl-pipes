from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api.db.connection import Base


@dataclass
class ReadOneFromDb(Pipe):
    db: Session
    model: Base
    filter: dict[str, Any]

    async def __call__(self) -> Base:
        item = self.db.query(self.model).filter_by(**self.filter).first()
        return item


@dataclass
class ReadManyFromDb(Pipe):
    db: Session
    model: Base
    filter: dict[str, Any]

    async def __call__(self) -> list[Base]:
        items = (
            self.db.query(self.model).filter_by(**self.filter).all()
        )  # type: list[Base]
        return items
