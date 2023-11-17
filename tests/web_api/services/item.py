from dataclasses import dataclass

from etl_pipes.pipes.base_pipe import Pipe
from tests.web_api import domain_types
from tests.web_api.domain_types import ItemId


@dataclass
class ChangeTodoItemOrderService(Pipe):
    item_id: ItemId
    new_order: int

    async def __call__(self, items: list[domain_types.Item]) -> list[domain_types.Item]:
        item = next((item for item in items if item.id == self.item_id), None)
        if not item:
            return []
        item.order = self.new_order
        items = self.change_order_of_affected_items(items, item.order)
        return items

    def change_order_of_affected_items(
        self, items: list[domain_types.Item], changed_order: int
    ) -> list[domain_types.Item]:
        if self.new_order < changed_order:
            items = self._increase_order_of_items(items)
        else:
            items = self._decrease_order_of_items(items)
        return items

    def _increase_order_of_items(
        self, items: list[domain_types.Item]
    ) -> list[domain_types.Item]:
        affected_items = []
        for item in items:
            if item.order >= self.new_order and item.id != self.item_id:
                item.order += 1
                affected_items.append(item)
        return affected_items

    def _decrease_order_of_items(
        self, items: list[domain_types.Item]
    ) -> list[domain_types.Item]:
        affected_items = []
        for item in items:
            if item.order <= self.new_order and item.id != self.item_id:
                item.order -= 1
                affected_items.append(item)
        return affected_items
