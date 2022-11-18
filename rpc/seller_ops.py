# import threading
# from copy import copy
# from typing import List, Dict
#
# import Pyro4
#
# from enums.item_type import Item
# from logger import get_logger
# from peer.model import Peer
# from rpc.ops import CommonOps
# from utils import get_new_item
#
# LOGGER = get_logger(__name__)
#
# @Pyro4.expose
# class SellerOps(CommonOps):
#     def __init__(self,
#                  network: Dict[str, Peer],
#                  current_peer: Peer,
#                  item_quantities_map: dict,
#                  thread_pool_size=20):
#         super().__init__(network, current_peer, thread_pool_size)
#
#         self._item_quantities_map = item_quantities_map
#         self._buy_lock = threading.Lock()
#
#     def lookup(self, buyer_id: int, product: Item, hop_count: int, search_path: List[str]):
#         product = self.get_product_enum(product)
#
#         reply_path = copy(search_path)
#
#         # Send lookup requests to all neighbours
#         super().lookup(buyer_id, product, hop_count, search_path)
#
#         # Send reply if item found
#         with self._lookup_lock:
#             if self._check_if_item_available(product):
#                 LOGGER.info(f"Peer {self._current_peer.id} sending reply to buyer {buyer_id}, search_path {reply_path}!")
#                 self.reply(seller_id=self._current_peer.id, reply_path=reply_path)
#
#     def buy(self, buyer_id: str, product: Item):
#         product = self.get_product_enum(product)
#         LOGGER.info(f"Buy call Buyer: {buyer_id}, peer id {self._current_peer.id}, product: {product}")
#
#         with self._buy_lock:
#             if self._check_if_item_available(product):
#                 LOGGER.info(f"Item {product} is available")
#                 self._current_peer.quantity -= 1
#                 LOGGER.info(f"Buyer {buyer_id} purchased {product} from seller {self._current_peer.id} and quantity: {self._current_peer.quantity} remains now")
#
#             # Set new item and quantity for current peer
#             if self._current_peer.quantity <= 0:
#                 item = get_new_item(current_item=self._current_peer.item)
#                 quantity = self._item_quantities_map[item]
#
#                 self._current_peer.quantity = quantity
#                 self._current_peer.item = item
#
#                 LOGGER.info(f"Item {product} sold! Seller is now selling {item} and has a quantity {quantity}")
#
#                 raise ValueError(f"Could not execute Buy order for item {product}, buyer {buyer_id}")
#
#     def _check_if_item_available(self, product: Item):
#         return self._current_peer.item == product and self._current_peer.quantity > 0
