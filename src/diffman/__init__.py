"""diffman: variant/cache/run manager for simulation pipelines."""

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
    FP_VERSION,
)
from .submitters import LocalSubmitter, SlurmSubmitter, default_submitter
from .discovery import discover, DISCOVERED_PATHS, load_module

__all__ = [
    'Config', 'Variant', 'VariantRegistry',
    'Stage', 'Pipeline',
    'RunRegistry', 'RunContext', 'RunRecord',
    'fingerprint', 'register', 'registry', 'FP_VERSION',
    'LocalSubmitter', 'SlurmSubmitter', 'default_submitter',
    'discover', 'DISCOVERED_PATHS', 'load_module',
]

__version__ = '0.1.0'
