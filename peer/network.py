from typing import List, Dict

from enums.item_type import Item
from enums.peer_type import PeerType
from peer.model import Peer
from utils import get_free_port


class NetworkCreator:
    def __init__(self, nodes: List[Peer]):
        self._nodes: List[Peer] = nodes

    """
        {
            "p1": <Peer>,
            "p2": <Peer>
            ....
        }
    """

    def generate_network(self) -> Dict[str, Peer]:
        network_dict = {}
        num_nodes = len(self._nodes)

        for i, node in enumerate(self._nodes):
            # next_neighbour_index = (i + 1) % num_nodes
            # last_neighbour_index = (i - 1) % num_nodes

            # next_neighbour = self._nodes[next_neighbour_index]
            # prev_neighbour = self._nodes[last_neighbour_index]

            # # Form connections for all neighbors
            # node.add_neighbour(next_neighbour.id)
            # node.add_neighbour(prev_neighbour.id)

            for j in range(num_nodes):
                if (j!=i):
                    neighb_id = self._nodes[i]
                    node.add_neighbour(neighb_id)

            network_dict[node.id] = node

        return network_dict

    def print(self, network):
        for _, peer in network.items():
            peer.print()
