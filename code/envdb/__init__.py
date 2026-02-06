"""
Environmental Database (EnvDB) - A normalized data model for environmental monitoring
Supports water quality, air quality, soil, biodiversity, and Natura 2000 areas.
"""

from .models import (
    Authority,
    Region, 
    Station,
    Sample,
    Parameter,
    Classifier,
    Assessment,
    Base
)

from .database import Database
from .query import QueryBuilder
from .authorities import AuthorityType, Authority as AuthorityEnum
from .envregistry import (
    MolecularRegistry,
    ureg
)

__version__ = "1.0.0"
__all__ = [
    "Authority", 
    "Region", 
    "Station", 
    "Sample", 
    "Parameter", 
    "Classifier",
    "Assessment",
    "Base",
    "Database",
    "QueryBuilder",
    "AuthorityType",
    "AuthorityEnum"
]