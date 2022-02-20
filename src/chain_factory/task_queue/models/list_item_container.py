from typing import List
from pydantic import BaseModel


class ListItem(BaseModel):
    name: str = ''
    content: str = ''


class ListItemContainer(BaseModel):
    list_items: List[ListItem] = []
