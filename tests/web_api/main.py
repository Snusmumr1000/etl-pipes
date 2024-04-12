import uvicorn
from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from etl_pipes.pipes.maybe import Maybe
from etl_pipes.pipes.parallel import Parallel
from etl_pipes.pipes.pipeline.pipeline import Pipeline
from etl_pipes.pipes.void import Void
from tests.web_api.auth import AuthToken, CheckAccessForItem, CheckAccessForTodo, Ops
from tests.web_api.cache.todo_cache import CacheTodoDTO, GetTodoAndItemsFromCache
from tests.web_api.db import connection, models
from tests.web_api.db.add import AddItemToDb
from tests.web_api.db.connection import engine, get_db
from tests.web_api.db.delete import DeleteOneFromDb
from tests.web_api.db.read import ReadManyFromDb, ReadOneFromDb
from tests.web_api.db.update import UpdateItemInDb, UpdateManyInDb
from tests.web_api.domain_types import ItemId, TodoId
from tests.web_api.dto import (
    CreateOrUpdateItemDto,
    CreateTodoDto,
    GetItemDto,
    GetTodoDto,
)
from tests.web_api.mapping.item import (
    MapCreateOrUpdateItemDtoToDb,
    MapDbItemToDto,
    MapDbTodoItemToDomain,
    MapDomainTodoItemToDb,
)
from tests.web_api.mapping.todo import MapCreateTodoDtoToDb, MapDbTodoAndDbItemsToDto
from tests.web_api.services.item import ChangeTodoItemOrderService
from tests.web_api.validators import ValidateItem, ValidateTodo

connection.Base.metadata.create_all(bind=engine)

app = FastAPI()


# TBD: fix ignore[no-any-return] in all endpoints by mypy plugin


@app.get("/items/{item_id}")
async def read_item(
    token: AuthToken, item_id: ItemId, db: Session = Depends(get_db)
) -> GetItemDto:
    pipeline = Pipeline(
        [
            CheckAccessForItem(token, item_id, ops=[Ops.Read]),
            Pipeline(
                [  # ReadItemService?
                    ReadOneFromDb(model=models.Item, filter={"id": item_id}, db=db),
                    MapDbItemToDto(),
                ]
            ),
        ]
    )
    return await pipeline()  # type: ignore[no-any-return]


@app.put("/todos/{todo_id}/items")
async def create_or_update_item_in_todo(
    token: AuthToken, item: CreateOrUpdateItemDto, db: Session = Depends(get_db)
) -> GetItemDto:
    pipeline = Pipeline(
        [
            Parallel(
                [
                    CheckAccessForTodo(token, item.todo_id, ops=[Ops.Update]),
                    ValidateItem(item),
                ]
            ),
            MapCreateOrUpdateItemDtoToDb(),
            Maybe(AddItemToDb(db)).otherwise(UpdateItemInDb(db)),
            MapDbItemToDto(),
        ]
    )
    return await pipeline()  # type: ignore[no-any-return]


@app.delete("/todos/{todo_id}/items/{item_id}")
async def delete_item_from_todo(
    token: AuthToken, todo_id: TodoId, item_id: ItemId, db: Session = Depends(get_db)
) -> None:
    pipeline = Pipeline(
        [
            CheckAccessForTodo(token, todo_id, ops=[Ops.Update]),
            DeleteOneFromDb(db=db, model=models.Todo, filter={"id": todo_id}),
        ]
    )
    return await pipeline()  # type: ignore[no-any-return]


@app.get("/todos/{todo_id}")
async def read_todo(
    token: AuthToken, todo_id: TodoId, db: Session = Depends(get_db)
) -> GetTodoDto:
    pipeline = Pipeline(
        [
            CheckAccessForTodo(token, todo_id, ops=[Ops.Read]),
            Void(),
            Maybe(
                GetTodoAndItemsFromCache(todo_id=todo_id),
            ).otherwise(
                Pipeline(
                    [
                        Parallel(
                            [
                                ReadOneFromDb(
                                    db=db, model=models.Todo, filter={"id": todo_id}
                                ),
                                ReadManyFromDb(
                                    filter={"todo_id": todo_id},
                                    model=models.Item,
                                    db=db,
                                ),
                            ]
                        ),
                        MapDbTodoAndDbItemsToDto(),
                        CacheTodoDTO(),
                    ]
                )
            ),
        ]
    )
    return await pipeline()  # type: ignore[no-any-return]


@app.post("/todos")
async def create_todo(
    token: AuthToken, todo: CreateTodoDto, db: Session = Depends(get_db)
) -> GetTodoDto:
    pipeline = Pipeline(
        [
            ValidateTodo(),
            MapCreateTodoDtoToDb(),
            AddItemToDb(db),
            MapDbTodoAndDbItemsToDto(),
        ]
    )
    return await pipeline(todo)  # type: ignore[no-any-return]


@app.get("/todos/{todo_id}/items/{item_id}/order/{new_order}")
async def change_todo_item_order(
    token: AuthToken,
    todo_id: TodoId,
    item_id: ItemId,
    new_order: int,
    db: Session = Depends(get_db),
) -> list[GetItemDto]:
    pipeline = Pipeline(
        [
            CheckAccessForTodo(token, todo_id, ops=[Ops.Update]),
            ReadManyFromDb(db=db, model=models.Todo, filter={"todo_id": todo_id}),
            MapDbTodoItemToDomain(),
            ChangeTodoItemOrderService(item_id, new_order),
            MapDomainTodoItemToDb(),
            UpdateManyInDb(db=db),
            MapDbItemToDto(),
        ]
    )
    return await pipeline()  # type: ignore[no-any-return]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
