"""diffman: track simulation-pipeline forks and their parameter diffs."""

from .core import (
    Config,
    Variant,
    VariantRegistry,
    Stage,
    Pipeline,
    Chain,
    ChainStep,
    Variation,
    RunRegistry,
    RunContext,
    RunRecord,
    fingerprint,
    register,
    registry,
    mpi_rank,
    mpi_barrier,
)
from .discovery import discover, load_module

__all__ = [
    'Config', 'Variant', 'VariantRegistry',
    'Stage', 'Pipeline',
    'Chain', 'ChainStep', 'Variation',
    'RunRegistry', 'RunContext', 'RunRecord',
    'fingerprint', 'register', 'registry',
    'mpi_rank', 'mpi_barrier',
    'discover', 'load_module',
]

__version__ = '0.3.0'
