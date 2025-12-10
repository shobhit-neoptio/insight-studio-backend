"""
Level 1: Field + Row Validation (Real-time Frontend).

This module provides validation for individual fields and rows,
designed for real-time validation in the frontend.

Also includes algorithm expression helpers for UI/real-time validation.
"""

import re
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

from .base import (
    ValidationLevel,
    ValidationIssue,
    ValidationResult,
    NULL_VALUE,
    clean_json_str,
    get_str_value,
)

# Import column definitions from excel_io
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.excel_io import get_column_definitions, get_column_order, ColumnDefinition


# ============================================================================
# LEVEL 1: FIELD VALIDATION
# ============================================================================

def validate_field(
    field_name: str,
    value: Any,
    sheet_name: str,
    existing_values: Optional[List[Any]] = None,
    is_edit: bool = False,
    original_value: Any = None,
    column_def: Optional[ColumnDefinition] = None
) -> ValidationResult:
    """
    Level 1: Validate a single field value.
    
    Args:
        field_name: Name of the field
        value: Value to validate
        sheet_name: Name of the sheet (for getting column definitions)
        existing_values: List of existing values for uniqueness check
        is_edit: Whether this is an edit operation
        original_value: Original value if editing
        column_def: Optional column definition (if not provided, looks up from sheet)
    
    Returns:
        ValidationResult with any issues found
    """
    result = ValidationResult(is_valid=True, level=ValidationLevel.FIELD_ROW)
    
    # Get column definition if not provided
    if column_def is None:
        column_defs = get_column_definitions(sheet_name)
        column_def = column_defs.get(field_name)
        if column_def is None:
            return result  # Unknown field, skip validation
    
    # Clean the value if it's a string
    if isinstance(value, str):
        value = clean_json_str(value)
    
    # Check required field
    if column_def.required:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=None,
                field=field_name,
                severity="error",
                message=f"{column_def.display_name} is required",
                level=ValidationLevel.FIELD_ROW
            ))
            return result
    
    # Skip further validation if value is empty and not required
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return result
    
    # Type-specific validation
    if column_def.data_type == "string":
        if not isinstance(value, str):
            value = str(value)
        
        if column_def.max_length and len(value) > column_def.max_length:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=None,
                field=field_name,
                severity="error",
                message=f"{column_def.display_name} must be {column_def.max_length} characters or less",
                level=ValidationLevel.FIELD_ROW
            ))
        
        if column_def.pattern:
            if not re.match(column_def.pattern, value):
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=None,
                    field=field_name,
                    severity="warning",
                    message=column_def.pattern_message or f"{column_def.display_name} format is not recommended",
                    level=ValidationLevel.FIELD_ROW
                ))
    
    elif column_def.data_type == "boolean":
        if not isinstance(value, bool):
            if isinstance(value, str):
                if value.upper() not in ('TRUE', 'FALSE', 'YES', 'NO', '1', '0'):
                    result.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=None,
                        field=field_name,
                        severity="error",
                        message=f"{column_def.display_name} must be a boolean value",
                        level=ValidationLevel.FIELD_ROW
                    ))
    
    elif column_def.data_type == "integer":
        try:
            int(value)
        except (ValueError, TypeError):
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=None,
                field=field_name,
                severity="error",
                message=f"{column_def.display_name} must be an integer",
                level=ValidationLevel.FIELD_ROW
            ))
    
    # Check uniqueness
    if column_def.unique and existing_values:
        compare_value = str(value).strip().upper() if value else ""
        existing_upper = [str(v).strip().upper() for v in existing_values if v is not None]
        
        if is_edit and original_value:
            original_upper = str(original_value).strip().upper()
            existing_upper = [v for v in existing_upper if v != original_upper]
        
        if compare_value in existing_upper:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=None,
                field=field_name,
                severity="error",
                message=f"{column_def.display_name} '{value}' already exists",
                level=ValidationLevel.FIELD_ROW
            ))
    
    return result


# ============================================================================
# LEVEL 1: ROW VALIDATION
# ============================================================================

def validate_row(
    row_data: Dict[str, Any],
    sheet_name: str,
    existing_df: Optional[pd.DataFrame] = None,
    is_edit: bool = False,
    edit_index: Optional[int] = None,
    row_number: Optional[int] = None
) -> ValidationResult:
    """
    Level 1: Validate an entire row of data.
    
    Args:
        row_data: Dictionary of field values
        sheet_name: Name of the sheet
        existing_df: Existing DataFrame for uniqueness checks
        is_edit: Whether this is an edit operation
        edit_index: Index of row being edited
        row_number: Excel row number (for error messages)
    
    Returns:
        ValidationResult with all issues found
    """
    result = ValidationResult(is_valid=True, level=ValidationLevel.FIELD_ROW)
    
    column_defs = get_column_definitions(sheet_name)
    column_order = get_column_order(sheet_name)
    
    for col_name in column_order:
        if col_name not in column_defs:
            continue
        
        col_def = column_defs[col_name]
        value = row_data.get(col_name)
        
        existing_values = None
        original_value = None
        if col_def.unique and existing_df is not None and col_name in existing_df.columns:
            existing_values = existing_df[col_name].tolist()
            if is_edit and edit_index is not None and edit_index < len(existing_values):
                original_value = existing_values[edit_index]
        
        field_result = validate_field(
            field_name=col_name,
            value=value,
            sheet_name=sheet_name,
            existing_values=existing_values,
            is_edit=is_edit,
            original_value=original_value,
            column_def=col_def
        )
        
        # Update row number in issues
        for issue in field_result.issues:
            issue.row = row_number
        
        result.merge(field_result)
    
    return result


# ============================================================================
# ALGORITHM EXPRESSION HELPERS (for UI/real-time validation)
# ============================================================================

def validate_algorithm_expression_basic(expression: str) -> Tuple[bool, List[str]]:
    """Basic validation of algorithm expression syntax."""
    messages = []
    is_valid = True
    
    if not expression or (isinstance(expression, str) and expression.strip() == ""):
        return False, ["Algorithm expression is required"]
    
    expression = expression.strip()
    
    # Check for fact references
    fact_pattern = re.compile(r"fact\(['\"]([^'\"]+)['\"]\)")
    fact_matches = fact_pattern.findall(expression)
    
    # Check for group references
    group_pattern = re.compile(r"groups\(['\"]([^'\"]+)['\"]\)")
    group_matches = group_pattern.findall(expression)
    
    if not fact_matches and not group_matches:
        messages.append("Expression must contain at least one fact() or groups() reference")
        is_valid = False
    else:
        if fact_matches:
            messages.append(f"Found {len(fact_matches)} fact reference(s)")
        if group_matches:
            messages.append(f"Found {len(group_matches)} group reference(s)")
    
    # Check parentheses balance
    open_parens = expression.count('(')
    close_parens = expression.count(')')
    if open_parens != close_parens:
        messages.append(f"Unbalanced parentheses: {open_parens} opening, {close_parens} closing")
        is_valid = False
    else:
        messages.append("Parentheses are balanced")
    
    # Check for valid comparison values
    fact_comparisons = re.findall(r"fact\(['\"][^'\"]+['\"]\)\s*==\s*\w+", expression)
    for comp in fact_comparisons:
        value = comp.split('==')[-1].strip()
        if value.lower() not in ['true', 'false']:
            messages.append(f"Invalid fact comparison value '{value}'. Must be True or False")
            is_valid = False
    
    # Basic syntax check
    try:
        test_expr = fact_pattern.sub('True', expression)
        test_expr = re.sub(r"groups\(['\"][^'\"]+['\"]\)\.response\([^)]+\)\s*(==|>=)\s*\d+", 'True', test_expr)
        compile(test_expr, '<string>', 'eval')
        messages.append("Expression syntax is valid")
    except SyntaxError as e:
        messages.append(f"Syntax error: {str(e)}")
        is_valid = False
    
    return is_valid, messages


def validate_algorithm_expression_facts(
    expression: str,
    excel_data: dict
) -> Tuple[bool, List[str], List[str], List[str]]:
    """Validate that all facts referenced in expression exist."""
    messages = []
    found_facts = []
    missing_facts = []
    is_valid = True
    
    if not expression:
        return True, [], [], []
    
    fact_pattern = re.compile(r"fact\(['\"]([^'\"]+)['\"]\)")
    referenced_facts = fact_pattern.findall(expression)
    
    if not referenced_facts:
        return True, [], [], []
    
    # Collect available facts
    available_facts = set()
    for level in range(4):
        sheet_name = f"Level {level} Facts"
        if sheet_name in excel_data:
            df = excel_data[sheet_name]
            if not df.empty and 'factId' in df.columns:
                for fact_id in df['factId'].tolist():
                    if fact_id and str(fact_id).strip():
                        available_facts.add(str(fact_id).strip())
    
    # Check each referenced fact
    for fact_id in referenced_facts:
        if fact_id in available_facts:
            found_facts.append(fact_id)
        else:
            missing_facts.append(fact_id)
            is_valid = False
    
    if found_facts:
        messages.append(f"{len(found_facts)} fact(s) found")
    
    if missing_facts:
        messages.append(f"{len(missing_facts)} fact(s) NOT found: {', '.join(missing_facts)}")
    
    return is_valid, messages, found_facts, missing_facts


def validate_algorithm_expression_groups(
    expression: str,
    excel_data: dict
) -> Tuple[bool, List[str], List[str], List[str], Dict[str, int]]:
    """Validate that all groups referenced in expression exist."""
    messages = []
    found_groups = []
    missing_groups = []
    group_fact_counts: Dict[str, int] = {}
    is_valid = True
    
    if not expression:
        return True, [], [], [], {}
    
    group_pattern = re.compile(r"groups\(['\"]([^'\"]+)['\"]\)")
    referenced_groups = list(dict.fromkeys(group_pattern.findall(expression)))
    
    if not referenced_groups:
        return True, [], [], [], {}
    
    # Collect available groups and their fact counts
    available_groups: Dict[str, int] = {}
    for level in range(4):
        sheet_name = f"Level {level} Facts"
        if sheet_name in excel_data:
            df = excel_data[sheet_name]
            if not df.empty and 'factGroup' in df.columns:
                for _, row in df.iterrows():
                    group_name = row.get('factGroup')
                    fact_id = row.get('factId')
                    if group_name and fact_id:
                        group_name = str(group_name).strip()
                        if group_name:
                            if group_name not in available_groups:
                                available_groups[group_name] = 0
                            available_groups[group_name] += 1
    
    available_groups_lower = {g.lower(): g for g in available_groups.keys()}
    
    for group_name in referenced_groups:
        group_lower = group_name.lower()
        if group_lower in available_groups_lower:
            actual_name = available_groups_lower[group_lower]
            found_groups.append(group_name)
            group_fact_counts[group_name] = available_groups[actual_name]
        else:
            missing_groups.append(group_name)
            is_valid = False
    
    if found_groups:
        group_info = [f"{g} ({group_fact_counts[g]} facts)" for g in found_groups[:3]]
        messages.append(f"{len(found_groups)} group(s) found: {', '.join(group_info)}")
    
    if missing_groups:
        messages.append(f"{len(missing_groups)} group(s) NOT found: {', '.join(missing_groups)}")
    
    return is_valid, messages, found_groups, missing_groups, group_fact_counts


def get_available_groups(excel_data: dict) -> List[str]:
    """Get list of available group names from Level Facts sheets."""
    groups = set()
    
    for level in range(4):
        sheet_name = f"Level {level} Facts"
        if sheet_name in excel_data:
            df = excel_data[sheet_name]
            if not df.empty and 'factGroup' in df.columns:
                for group in df['factGroup'].dropna().unique():
                    group_str = str(group).strip()
                    if group_str:
                        groups.add(group_str)
    
    return sorted(groups)


def get_group_info(excel_data: dict) -> Dict[str, Dict[str, Any]]:
    """Get detailed information about all groups."""
    groups: Dict[str, Dict[str, Any]] = {}
    
    for level in range(4):
        sheet_name = f"Level {level} Facts"
        if sheet_name in excel_data:
            df = excel_data[sheet_name]
            if not df.empty and 'factGroup' in df.columns:
                for _, row in df.iterrows():
                    group_name = row.get('factGroup')
                    fact_id = row.get('factId')
                    
                    if not group_name or not fact_id:
                        continue
                    
                    group_name = str(group_name).strip()
                    fact_id = str(fact_id).strip()
                    
                    if not group_name:
                        continue
                    
                    if group_name not in groups:
                        groups[group_name] = {
                            'fact_count': 0,
                            'fact_ids': [],
                            'levels': set()
                        }
                    
                    if fact_id not in groups[group_name]['fact_ids']:
                        groups[group_name]['fact_ids'].append(fact_id)
                        groups[group_name]['fact_count'] += 1
                    groups[group_name]['levels'].add(level)
    
    for group_name in groups:
        groups[group_name]['levels'] = sorted(groups[group_name]['levels'])
    
    return groups


def get_available_modules(excel_data: dict) -> List[str]:
    """Get list of available module codes."""
    if 'Modules' not in excel_data:
        return []
    
    modules_df = excel_data['Modules']
    if modules_df.empty or 'moduleCode' not in modules_df.columns:
        return []
    
    return [str(c).strip() for c in modules_df['moduleCode'].tolist() if c]


def get_available_assessments(excel_data: dict) -> List[str]:
    """Get list of available assessment codes."""
    if 'Assessments' not in excel_data:
        return []
    
    assessments_df = excel_data['Assessments']
    if assessments_df.empty or 'assessmentCode' not in assessments_df.columns:
        return []
    
    return [str(c).strip() for c in assessments_df['assessmentCode'].tolist() if c]


def suggest_algorithm_id(existing_df) -> str:
    """Suggest the next algorithm ID based on existing algorithms."""
    if existing_df is None or existing_df.empty:
        return "ALG_001"
    
    if 'algorithmId' not in existing_df.columns:
        return "ALG_001"
    
    existing_ids = existing_df['algorithmId'].tolist()
    
    numbers = []
    for alg_id in existing_ids:
        match = re.match(r'ALG_(\d+)', str(alg_id))
        if match:
            numbers.append(int(match.group(1)))
    
    if not numbers:
        return "ALG_001"
    
    next_num = max(numbers) + 1
    return f"ALG_{next_num:03d}"


# ============================================================================
# PUBLIC API
# ============================================================================

__all__ = [
    # Level 1 validation
    "validate_field",
    "validate_row",
    
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
