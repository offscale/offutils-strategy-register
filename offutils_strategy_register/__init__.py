#!/usr/bin/env python

from types import (
    DictType,
    ListType,
    TupleType,
    BooleanType,
    FloatType,
    StringType,
    UnicodeType,
    IntType,
    NoneType,
    LongType,
)

from collections import namedtuple
from importlib import import_module
from etcd import Client, EtcdKeyNotFound, EtcdNotFile

try:
    import pickle as pickle
except ImportError:
    import pickle

from libcloud.compute.base import NodeImage, NodeLocation, NodeSize, Node
from libcloud.compute.providers import get_driver

# TODO: Automatically import the right ones rather than listing:
from libcloud.compute.drivers.azure import AzureNodeLocation
from libcloud.compute.drivers.azure_arm import AzureImage, AzureNodeDriver
from libcloud.compute.drivers.ec2 import EC2NodeLocation
from libcloud.compute.drivers.vagrant import VagrantNodeDriver

from offutils import update_d

__author__ = "Samuel Marks"
__version__ = "0.0.8"

# Namedtuples
MarshallLoads = namedtuple("MarshallLoads", "loads")
KeyVal = namedtuple("KeyVal", "key value")

# Types which can be easily serialised
normal_types = (
    DictType,
    ListType,
    TupleType,
    BooleanType,
    FloatType,
    StringType,
    UnicodeType,
    IntType,
    NoneType,
    LongType,
)

_get_client = lambda **kwargs: Client(**kwargs)

save_node_info = lambda node_name, node_info, folder="unclustered", marshall=pickle, **client_kwargs: _get_client(
    **client_kwargs
).set(
    "/".join((folder, node_name)), marshall.dumps(node_info)
)

get_node_info = lambda node_name, folder="unclustered", marshall=pickle, **client_kwargs: marshall.loads(
    _get_client(**client_kwargs)
    .read("/".join((folder, node_name)))
    .value.encode("utf8")
)

obj_to_d = (
    lambda obj: obj
    if type(obj) is DictType
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
            "network_interfaces" in node_d["extra"]
            and node_d["extra"]["network_interfaces"]
        ):
            node_d["extra"]["network_interfaces"] = [
                interface
                if type(interface) is DictType
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
    assert type(d) is DictType

    if "driver_cls" not in d:
        d["driver_cls"] = get_driver(d["extra"]["provider"])
        d["_class"] = d["driver_cls"].__name__

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
    assert type(d) is DictType
    _class = d.pop("_class")
    # if _class not in globals(): return import_module(_class)(**d)
    return _class(**d)


def print_f(*args, **kwargs):
    print((args, kwargs))


def print_dict_and_type(d):
    for key, val in list(d.items()):
        print(("key = '{0}', val = `{1}`, type = {2}".format(key, val, type(val))))


def list_nodes(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    try:
        return tuple(
            filter(
                None,
                list(
                    map(
                        lambda d: KeyVal(d.key, marshall.loads(d.value)),
                        _get_client(**client_kwargs)
                        .read(folder, recursive=True)
                        .children,
                    )
                ),
            )
        )
    except TypeError as e:
        if e.message == "expected string or buffer":
            raise EtcdNotFile('"{folder}" etcd folder is empty'.format(folder=folder))


def fetch_node(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    try:
        return next(
            list(
                filter(
                    None,
                    list(
                        map(
                            lambda d: KeyVal(d.key, marshall.loads(d.value)),
                            _get_client(**client_kwargs)
                            .read(folder, recursive=True)
                            .children,
                        )
                    ),
                )
            ),
            None,
        )
    except TypeError as e:
        if e.message == "expected string or buffer":
            raise EtcdNotFile('"{folder}" etcd folder is empty'.format(folder=folder))
