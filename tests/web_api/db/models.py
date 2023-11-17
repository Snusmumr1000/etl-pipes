from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from tests.web_api.db.connection import Base, engine


class Item(Base):  # type: ignore
    __tablename__ = "items"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    done = Column(Boolean, default=False)
    order = Column(Integer, nullable=False)
    todo_id = Column(String, ForeignKey("todos.id"))

    # Back-populates the 'items' attribute in Todo model.
    todo = relationship("Todo", back_populates="items")


class Todo(Base):  # type: ignore
    __tablename__ = "todos"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)

    # Back-populates the 'todo' attribute in Item model.
    items = relationship("Item", back_populates="todo")


if __name__ == "__main__":
    Base.metadata.create_all(engine)
