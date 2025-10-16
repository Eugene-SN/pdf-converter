"""Quality assurance helpers exposed as a proper Python package.

This module ensures that Airflow DAGs and auxiliary scripts can import the
quality assurance utilities (for example ``quality_assurance.visual_diff_system``)
without relying on implicit ``sys.path`` manipulation.  The file also exports a
minimal public API to make intra-package imports explicit.
"""

from .visual_diff_system import VisualDiffSystem, VisualDiffConfig  # noqa: F401
from .ast_comparator import ASTComparator, ASTComparisonConfig  # noqa: F401

__all__ = [
    "VisualDiffSystem",
    "VisualDiffConfig",
    "ASTComparator",
    "ASTComparisonConfig",
]

