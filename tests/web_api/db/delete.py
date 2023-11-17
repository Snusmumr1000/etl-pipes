from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api.db.connection import Base


@dataclass
class DeleteOneFromDb(Pipe):
    db: Session
    model: Base
    filter: dict[str, Any]

    async def __call__(self) -> None:
        item = self.db.query(self.model).filter_by(**self.filter).first()
        self.db.delete(item)
        self.db.commit()


@dataclass
class DeleteManyFromDb(Pipe):
    db: Session
    model: Base
    filter: dict[str, Any]

    async def __call__(self) -> None:
        self.db.query(self.model).filter_by(**self.filter).delete()
        self.db.commit()
