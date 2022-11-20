from typing import Dict


from peer.model import Peer
from rpc.ops import CommonOps



class OpsFactory:

    @staticmethod
    def get_ops(network: Dict[str, Peer],
                current_peer: Peer,
                item_quantities_map: dict,
                thread_pool_size=20,
                ):
        return CommonOps(network,
                         current_peer,
                         item_quantities_map,
                         thread_pool_size
                         )
