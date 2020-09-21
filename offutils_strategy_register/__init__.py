#!/usr/bin/env python

from __future__ import print_function

from collections import namedtuple
from operator import itemgetter

try:
    import cPickle as pickle
except ImportError:
    import pickle

from sys import version

if version[0] == "2":
    from itertools import imap as map

import etcd3

from libcloud.compute.base import Node
from libcloud.compute.providers import get_driver
from libcloud.compute.types import NodeState

# TODO: Automatically import the right ones rather than listing:
from libcloud.compute.drivers.azure import AzureNodeLocation
from libcloud.compute.drivers.azure_arm import AzureImage, AzureNodeDriver
from libcloud.compute.drivers.ec2 import EC2NodeLocation

from offutils import update_d

__author__ = "Samuel Marks"
__version__ = "0.0.9"

# Namedtuples
MarshallLoads = namedtuple("MarshallLoads", "loads")
KeyVal = namedtuple("KeyVal", "key value")

# Types which can be easily serialised
normal_types = (
    type(dict),
    type(list),
    type(tuple),
    type(bool),
    type(float),
    type(str),
    type(int),
    type(None),
)

_get_client = etcd3.client

save_node_info = lambda node_name, node_info, folder="unclustered", marshall=pickle, **client_kwargs: _get_client(
    **client_kwargs
).put(
    "/".join((folder, node_name)), marshall.dumps(node_info)
)

get_node_info = lambda node_name, folder="unclustered", marshall=pickle, **client_kwargs: marshall.loads(
    _get_client(**client_kwargs).get("/".join((folder, node_name))).value.encode("utf8")
)

obj_to_d = (
    lambda obj: obj
    if isinstance(obj, dict)
    else {k: getattr(obj, k) for k in dir(obj) if not k.startswith("_")}
)


def node_to_dict(node):
    node_d = {
        attr: getattr(node, attr)
        for attr in dir(node)
        if not attr.startswith("__")
        and type(getattr(node, attr)) in normal_types
        and getattr(node, attr)
    }
    node_d["driver"] = (
        node.driver.__name__
        if node.driver.__class__.__name__ == "type"
        else node.driver.__class__.__name__
    )

    if hasattr(node, "extra") and node.extra:
        if (
            "network_interfaces" in node_d.get("extra", {})
            and node_d["extra"]["network_interfaces"]
        ):
            node_d["extra"]["network_interfaces"] = [
                interface
                if isinstance(interface, dict)
                else {"name": interface.name, "id": interface.id}
                for interface in node_d["extra"]["network_interfaces"]
            ]
        node_d["extra"] = {
            k: v
            for k, v in list(node.extra.items())
            if k not in ("secret", "key") and type(v) in normal_types
        }
    if hasattr(node, "availability_zone"):
        node_d["availability_zone"] = obj_to_d(node.availability_zone)
    return node_d


def dict_to_node(d):
    assert isinstance(d, dict)

    if "driver_cls" not in d:
        d["driver_cls"] = get_driver(d["extra"]["provider"])
        d["_class"] = d["driver_cls"].__name__

    if "state" in d and isinstance(d["state"], (str, bytes)):
        d["state"] = getattr(NodeState, d["state"].upper())

    _class = d.pop("_class")
    driver_cls = d.pop("driver_cls")
    for key in "get_uuid", "uuid":
        d.pop(key, None)
    # d['driver'] = driver_cls

    try:
        return globals()[_class](**d)
    except (TypeError, KeyError):
        return Node(**(update_d(d, driver=driver_cls)))


def dict_to_cls(d):
    assert isinstance(d, dict)
    _class = d.pop("_class")
    # if _class not in globals(): return import_module(_class)(**d)
    return _class(**d)


def print_dict_and_type(d):
    for key, val in list(d.items()):
        print("key = '{0}', val = `{1}`, type = {2}".format(key, val, type(val)))


def list_nodes(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    return list(_fetch_nodes(folder, marshall=marshall, **client_kwargs))


def fetch_node(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    return next(_fetch_nodes(folder, marshall=marshall, **client_kwargs))


def _fetch_nodes(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    _client = _get_client(**client_kwargs)
    return map(
        lambda _node: KeyVal("/".join((folder, _node.name)), _node),
        map(
            dict_to_node,
            filter(
                lambda _node: "public_ips" in _node,
                (
                    map(
                        marshall.loads,
                        map(
                            itemgetter(0),
                            _client.get_prefix(
                                folder[1:] if folder.startswith("/") else folder
                            ),
                        ),
                    )
                ),
            ),
        ),
    )
