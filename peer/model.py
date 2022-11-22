from typing import Optional, Set, List

from enums.item_type import Item
from enums.peer_type import PeerType
from logger import get_logger

LOGGER = get_logger(__name__)


class Peer:
    def __init__(self,
                 id: int,
                 host: str,
                 port: int,
                 neighbours: List[int],
                 peer_type: PeerType,
                 item: Item,
                 amt_earned: float,
                 amt_spent: float,
                 commission: float,
                 vect_clock: List[int],
                 trader: int,
                 trader_host: str,
                 trader_port: int,
                 available_item_quantity: Optional[int] = None
                 ):
        self.id: int = id
        self.host: str = host
        self.port: int = port
        self.neighbours: List[int] = neighbours
        self.type: PeerType = peer_type
        self.item: Item = item
        self.quantity: int = available_item_quantity  # Applicable only for sellers
        self.amt_earned = amt_earned
        self.amt_spent = amt_spent
        self.commission = commission
        self.vect_clock: List[int] = vect_clock
        self.trader = trader
        self.trader_host = trader_host
        self.trader_port = trader_port

    def add_neighbour(self, id: int):
        self._neighbours.append(id)

    def print(self):
        LOGGER.info(self.__repr__())

    def __repr__(self):
        return f"Peer id: {self.id}, neighbours: {self.neighbours}, type: {self.type}, item: {self.item}," \
               f" quantity: {self.quantity} , vector clock: {self.vect_clock}  " \
               f"amt_spent : {self.amt_spent} , amt_earned: {self.amt_earned} , commission: {self.commission}>"
