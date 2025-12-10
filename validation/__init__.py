"""
Unified Multi-Level Validation System for Clinical Model Data.

This package consolidates all validation logic into three levels of complexity:
- Level 1: Field + Row validation (real-time frontend)
- Level 2: Single-sheet validation (before saving)
- Level 3: Full cross-sheet model validation (before export)

Usage:
    # Level 1 - Real-time field validation
    from validation import validate_field, validate_row
    result = validate_field("moduleCode", "TEST-MODULE", "Modules")
    result = validate_row(row_data, "Modules", existing_df)
    
    # Level 2 - Single sheet validation
    from validation import validate_sheet
    result = validate_sheet("Modules", df)
    
    # Level 3 - Full model validation
    from validation import validate_model
    result = validate_model(session_data)

Package Structure:
    - base.py: Shared constants, data classes, and helper functions
    - field_validator.py: Level 1 validation (field + row)
    - sheet_validator.py: Level 2 validation (single sheet)
    - model_validator.py: Level 3 validation (full model)
"""

# Base module - constants, data classes, helpers
from .base import (
    # Enums
    ValidationLevel,
    
    # Data classes
    ValidationIssue,
    ValidationResult,
    ValidationCategory,
    ModelValidationResult,
    
    # Constants
    COMMENT_PREFIXES,
    NULL_VALUE,
    VALID_DATA_TYPES,
    VALID_DETERMINISTIC_VALUES,
    VALID_ALGORITHM_EVENTS,
    VALID_RELATIONSHIP_TYPE_CODES,
    DURATION_MATRIX,
    
    # Helper functions
    clean_json_str,
    is_comment,
    is_row_to_skip,
    is_empty,
    get_str_value,
    convert_string_to_bool,
    normalize_algorithm,
    pair_values,
)

# Level 1 - Field + Row validation
from .field_validator import (
    validate_field,
    validate_row,
    
    # Algorithm expression helpers
    validate_algorithm_expression_basic,
    validate_algorithm_expression_facts,
    validate_algorithm_expression_groups,
    get_available_groups,
    get_group_info,
    get_available_modules,
    get_available_assessments,
    suggest_algorithm_id,
)

# Level 2 - Single sheet validation
from .sheet_validator import (
    validate_sheet,
    validate_fk_references,
)

# Level 3 - Full model validation
from .model_validator import (
    FullModelValidator,
    validate_model,
    validate_model_dict,
)


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
    
    # Level 1 API
    "validate_field",
    "validate_row",
    
    # Level 2 API
    "validate_sheet",
    "validate_fk_references",
    
    # Level 3 API
    "FullModelValidator",
    "validate_model",
    "validate_model_dict",
    
    # Algorithm expression helpers
    "validate_algorithm_expression_basic",
    "validate_algorithm_expression_facts",
    "validate_algorithm_expression_groups",
    "get_available_groups",
    "get_group_info",
    "get_available_modules",
    "get_available_assessments",
    "suggest_algorithm_id",
]
