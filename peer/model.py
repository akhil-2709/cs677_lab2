from typing import Optional, Set

from enums.item_type import Item
from enums.peer_type import PeerType
from logger import get_logger

LOGGER = get_logger(__name__)

class Peer:
    def __init__(self,
                 id: int,
                 host: str,
                 port: int,
                 neighbours: Set[int],
                 peer_type: PeerType,
                 item: Item,
                 amt_earned: float,
                 amt_spent: float,
                 commission: float,
                 available_item_quantity: Optional[int] = None
                 ):
        self._id: int = id
        self._host: str = host
        self._port: int = port
        self._neighbours: Set[int] = neighbours
        self._type: PeerType = peer_type
        self.item: Item = item
        self.quantity: int = available_item_quantity  # Applicable only for sellers
        self._amt_earned = amt_earned
        self._amt_spent = amt_spent
        self.commission = commission

    def add_neighbour(self, id: int):
        self._neighbours.add(id)

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type

    @property
    def neighbours(self):
        return self._neighbours

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def max_hop_count(self):
        return self._max_hop_count

    @property
    def amt_earned(self):
        return self.amt_earned

    @property
    def amt_spent(self):
        return self.amt_earned

    @property
    def commission(self):
        return self.commission

    def print(self):
        LOGGER.info(self.__repr__())
       # print("hello")

    def __repr__(self):
        return f"Peer<id: {self._id}, neighbours: {self._neighbours}, type: {self._type}, item: {self.item}, quantity: {self.quantity}>"
