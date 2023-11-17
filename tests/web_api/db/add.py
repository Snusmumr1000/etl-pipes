from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy.orm import Session

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api.db.connection import Base


@dataclass
class AddItemToDb(Pipe):
    db: Session

    async def __call__(self, item: Base) -> Base:
        item.id = str(uuid4())
        self.db.add(item)
        self.db.commit()
        self.db.refresh(item)
        return item
