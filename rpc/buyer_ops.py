import Pyro4

from rpc.ops import CommonOps


@Pyro4.expose
class BuyerOps(CommonOps):
    pass
