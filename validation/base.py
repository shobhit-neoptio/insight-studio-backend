"""
Base validation module with shared constants, data classes, and helper functions.

This module provides the foundation for the validation system:
- ValidationLevel enum for the three levels of validation
- Data classes for validation results and issues
- Helper functions for string cleaning, value parsing, etc.
"""

import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import IntEnum
import pandas as pd


# ============================================================================
# VALIDATION LEVELS
# ============================================================================

class ValidationLevel(IntEnum):
    """Validation complexity levels."""
    FIELD_ROW = 1       # Field + Row validation (real-time)
    SINGLE_SHEET = 2    # Single sheet validation
    FULL_MODEL = 3      # Full cross-sheet validation


# ============================================================================
# CONSTANTS
# ============================================================================

COMMENT_PREFIXES = ["COMMENT", "--", "#"]
NULL_VALUE = "<NULL>"

# Valid data types for assessment nodes
VALID_DATA_TYPES = ["CONVERSATION", "ENUMERATION", "STATEMENT", "INTEGER"]

# Valid isDeterministic values
VALID_DETERMINISTIC_VALUES = ["TRUE", "FALSE", "DELEGATE"]

# Valid algorithm events
VALID_ALGORITHM_EVENTS = ["MODULE", "ASSESSMENT", "FINDING"]

# Valid relationship type codes
VALID_RELATIONSHIP_TYPE_CODES = ["DIFFERENTIAL", "COMORBID", "SUBTYPE", "EXCLUDES", "RELATED"]

# Duration matrix for assessment duration calculations
DURATION_MATRIX = {
    "ENUMERATION": {
        "TRUE": {"min_duration_secs": 5, "max_duration_secs": 8},
        "FALSE": {"min_duration_secs": 15, "max_duration_secs": 20},
        "DELEGATE": {"min_duration_secs": 10, "max_duration_secs": 14}
    },
    "INTEGER": {
        "TRUE": {"min_duration_secs": 5, "max_duration_secs": 8},
        "FALSE": {"min_duration_secs": 15, "max_duration_secs": 20},
        "DELEGATE": {"min_duration_secs": 10, "max_duration_secs": 14}
    },
    "STATEMENT": {
        "TRUE": {"min_duration_secs": 5, "max_duration_secs": 8},
        "FALSE": {"min_duration_secs": 8, "max_duration_secs": 12}
    },
    "CONVERSATION": {
        "TRUE": {"min_duration_secs": 15, "max_duration_secs": 30},
        "FALSE": {"min_duration_secs": 15, "max_duration_secs": 60},
        "DELEGATE": {"min_duration_secs": 15, "max_duration_secs": 30}
    },
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ValidationIssue:
    """Represents a single validation issue with verbose details."""
    sheet: str
    row: Optional[int]  # None for sheet-level issues
    field: Optional[str]
    severity: str  # "error", "warning", "info"
    message: str
    level: int = 1  # Validation level that detected this issue
    context: Dict[str, Any] = field(default_factory=dict)
    # Verbose fields for enhanced error reporting
    current_value: Optional[str] = None  # The actual value that caused the issue
    expected_values: Optional[List[str]] = None  # List of valid/expected values
    suggestion: Optional[str] = None  # How to fix the issue
    rule_description: Optional[str] = None  # What this validation rule checks
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        # Remove None values to keep response clean
        return {k: v for k, v in result.items() if v is not None}


@dataclass
class ValidationResult:
    """Result of any validation operation."""
    is_valid: bool
    level: int
    issues: List[ValidationIssue] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    @property
    def errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]
    
    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]
    
    @property
    def info(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "info"]
    
    @property
    def error_count(self) -> int:
        return len(self.errors)
    
    @property
    def warning_count(self) -> int:
        return len(self.warnings)
    
    def add_issue(self, issue: ValidationIssue):
        self.issues.append(issue)
        if issue.severity == "error":
            self.is_valid = False
    
    def merge(self, other: 'ValidationResult'):
        """Merge another ValidationResult into this one."""
        self.issues.extend(other.issues)
        if not other.is_valid:
            self.is_valid = False
        # Keep the higher level
        self.level = max(self.level, other.level)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "level": self.level,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issues": [i.to_dict() for i in self.issues],
            "timestamp": self.timestamp
        }


@dataclass
class ValidationCategory:
    """Results grouped by validation category (for Level 3)."""
    name: str
    passed: bool = True
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    issues: List[ValidationIssue] = field(default_factory=list)
    
    def add_issue(self, issue: ValidationIssue):
        self.issues.append(issue)
        if issue.severity == "error":
            self.error_count += 1
            self.passed = False
        elif issue.severity == "warning":
            self.warning_count += 1
        else:
            self.info_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "issues": [i.to_dict() for i in self.issues]
        }


@dataclass
class ModelValidationResult(ValidationResult):
    """Extended result for Level 3 full model validation."""
    categories: Dict[str, ValidationCategory] = field(default_factory=dict)
    
    def add_category(self, category: ValidationCategory):
        self.categories[category.name] = category
        for issue in category.issues:
            self.issues.append(issue)
        if not category.passed:
            self.is_valid = False
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base["categories"] = {k: v.to_dict() for k, v in self.categories.items()}
        return base


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def clean_json_str(input_string: str) -> str:
    """
    Clean a string by replacing illegal/special characters.
    Handles Mac's curly quotes, em-dashes, and other special characters.
    """
    if not input_string:
        return input_string
    
    cleaned_string = input_string
    
    replacement_dict = {
        '\u2018': "'",   # ' left single quote
        '\u2019': "'",   # ' right single quote
        '\u201C': '"',   # " left double quote
        '\u201D': '"',   # " right double quote
        '\u00B4': "'",   # ´ acute accent
        '\u2013': '-',   # – en-dash
        '\u2014': '-',   # — em-dash
        '\u2212': '-',   # − minus sign
        '\u00D7': '*',   # × multiplication
        '\u00F7': '/',   # ÷ division
        '\u00B1': '+/-', # ± plus-minus
        '\u221A': 'sqrt', # √ square root
        '\u221E': 'inf',  # ∞ infinity
        '\u00A9': '(c)',  # © copyright
        '\u00AE': '(r)',  # ® registered
        '\u2122': '(tm)', # ™ trademark
        '\u2026': '...',  # … ellipsis
        '\u2022': '*',    # • bullet
        '\u2265': '>=',   # ≥ greater than or equal
        '\u2264': '<=',   # ≤ less than or equal
        '\u2260': '!=',   # ≠ not equal
        '\u2192': '->'    # → arrow
    }
    
    for key, value in replacement_dict.items():
        cleaned_string = cleaned_string.replace(key, value)
    
    return cleaned_string


def is_comment(text: str) -> bool:
    """Check if text is a comment line (starts with COMMENT, --, or #)."""
    if not text:
        return False
    text_upper = str(text).strip().upper()
    for prefix in COMMENT_PREFIXES:
        if text_upper.startswith(prefix):
            return True
    return False


def is_row_to_skip(value: str, heading: str) -> bool:
    """Check if row should be skipped (empty, <NULL>, comment, or heading repeat)."""
    if value is None:
        return True
    text = str(value).strip().upper()
    if len(text) == 0 or text == NULL_VALUE.upper() or text == heading.upper() or is_comment(str(value)):
        return True
    return False


def is_empty(value: Any) -> bool:
    """Check if value is empty (None, empty string, or <NULL>)."""
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return len(stripped) == 0 or stripped.upper() == NULL_VALUE.upper()
    if isinstance(value, float) and pd.isna(value):
        return True
    return False


def get_str_value(row_dict: Dict[str, Any], key: str) -> str:
    """
    Get string value from dict, handling None, NaN, and special chars.
    Returns NULL_VALUE if empty/missing.
    """
    value = row_dict.get(key)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return NULL_VALUE
    if isinstance(value, str):
        cleaned = clean_json_str(value)
        stripped = cleaned.strip()
        if len(stripped) > 0:
            return stripped
        return NULL_VALUE
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    result = str(value).strip()
    return clean_json_str(result) if result else NULL_VALUE


def convert_string_to_bool(value: Any) -> bool:
    """Convert spreadsheet text value to boolean. Accepts TRUE or 1 as True."""
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        upper_val = value.strip().upper()
        if upper_val == "TRUE" or upper_val == "1":
            return True
        return False
    return False


def normalize_algorithm(algorithm: str) -> str:
    """Normalize algorithm expression for Python eval."""
    if not algorithm or algorithm.upper() == NULL_VALUE.upper():
        return NULL_VALUE
    
    result = algorithm
    
    if result.upper() == "START" or result.upper() == "IS_COMPLETE":
        return result.lower()
    
    # Normalize function names and operators
    result = result.replace("FACT(", "fact(").replace("FACTS(", "fact(")
    result = result.replace("GROUPS(", "groups(").replace("GROUP(", "groups(")
    result = result.replace(" OR ", " or ").replace(" AND ", " and ")
    result = result.replace(" || ", " or ").replace(" && ", " and ")
    result = result.replace(" NOT ", " not ")
    result = result.replace(".RESPONSE(", ".response(").replace(".RESPONSES(", ".response(")
    result = result.replace(" = ", " == ").replace(" => ", " >= ").replace(" =< ", " <= ")
    result = result.replace("(true)", "(True)").replace("(TRUE)", "(True)")
    result = result.replace("== true", "== True").replace("== TRUE", "== True")
    result = result.replace("(false)", "(False)").replace("(FALSE)", "(False)")
    result = result.replace("== false", "== False").replace("== FALSE", "== False")
    
    return result


def pair_values(val1: str, val2: str) -> Dict[str, str]:
    """Pair target values with enumeration values."""
    if "|" in val1:
        val1_list = [v.strip() for v in val1.split('|')]
    else:
        val1_list = [v.strip() for v in val1.split(',')]

    if "|" in val2:
        val2_list = [v.strip() for v in val2.split('|')]
    else:
        val2_list = [v.strip() for v in val2.split(',')]
    
    if len(val1_list) == 1:
        val1_list *= len(val2_list)
    
    return dict(zip(val2_list, val1_list))


# ============================================================================
# PUBLIC API
# ============================================================================

__all__ = [
    # Enums
    "ValidationLevel",
    
    # Data classes
    "ValidationIssue",
    "ValidationResult",
    "ValidationCategory",
    "ModelValidationResult",
    
    # Constants
    "COMMENT_PREFIXES",
    "NULL_VALUE",
    "VALID_DATA_TYPES",
    "VALID_DETERMINISTIC_VALUES",
    "VALID_ALGORITHM_EVENTS",
    "VALID_RELATIONSHIP_TYPE_CODES",
    "DURATION_MATRIX",
    
    # Helper functions
    "clean_json_str",
    "is_comment",
    "is_row_to_skip",
    "is_empty",
    "get_str_value",
    "convert_string_to_bool",
    "normalize_algorithm",
    "pair_values",
]
