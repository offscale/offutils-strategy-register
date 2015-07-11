from setuptools import setup, find_packages
from os import path
from itertools import imap, ifilter
from ast import parse

if __name__ == '__main__':
    package_name = 'offutils_strategy_register'

    get_vals = lambda var0, var1: imap(lambda buf: next(imap(lambda e: e.value.s, parse(buf).body)),
                                       ifilter(lambda line: line.startswith(var0) or line.startswith(var1), f))

    with open(path.join(package_name, '__init__.py')) as f:
        __author__, __version__ = get_vals('__version__', '__author__')

    setup(
        name=package_name,
        author=__author__,
        version=__version__,
        description='Shared library (for strategy and register)',
        test_suite='tests',
        install_requires=['python-etcd', 'offutils'],
        packages=find_packages(),
        package_dir={package_name: package_name},
    )
