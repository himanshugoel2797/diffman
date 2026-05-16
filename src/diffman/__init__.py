"""diffman: track simulation-pipeline forks and their parameter diffs."""

from .core import (
    Config,
    Variant,
    VariantRegistry,
    Stage,
    Pipeline,
    RunRegistry,
    RunContext,
    RunRecord,
    fingerprint,
    register,
    registry,
)
from .discovery import discover, load_module

__all__ = [
    'Config', 'Variant', 'VariantRegistry',
    'Stage', 'Pipeline',
    'RunRegistry', 'RunContext', 'RunRecord',
    'fingerprint', 'register', 'registry',
    'discover', 'load_module',
]

__version__ = '0.2.0'
