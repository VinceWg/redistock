# -*- coding: utf-8 -*-
"""A simple and easy-to-use and cluster-supported distributed lock implement
based on Redis and Python.
"""

from .redistock import Redistock
from .redistock import RedistockNotObtained

__all__ = [Redistock, RedistockNotObtained]

__version__ = '0.1.3'
