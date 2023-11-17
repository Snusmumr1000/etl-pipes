from dataclasses import dataclass

from sqlalchemy.orm import Session

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api.db.connection import Base


@dataclass
class UpdateItemInDb(Pipe):
    db: Session

    async def __call__(self, item: Base) -> Base:
        self.db.add(item)
        self.db.commit()
        self.db.refresh(item)
        return item


@dataclass
class UpdateManyInDb(Pipe):
    db: Session

    async def __call__(self, items: list[Base]) -> list[Base]:
        self.db.add_all(items)
        self.db.commit()
        self.db.refresh(items)
        return items
