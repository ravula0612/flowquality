"""Explicit data quality rules for batch Spark DataFrames."""
from .core import Check, QualityError, Report, Result, validate

__all__ = ["Check", "QualityError", "Report", "Result", "validate"]
__version__ = "0.1.0"
