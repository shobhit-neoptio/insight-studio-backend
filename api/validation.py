"""
Validation logic for Excel sheet data.
Provides field-level and row-level validation based on column definitions.
"""

import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import pandas as pd

from .excel_io import get_column_definitions, get_column_order, ColumnDefinition


@dataclass
class ValidationError:
    """Represents a validation error for a specific field."""
    field: str
    message: str
    severity: str = "error"


@dataclass
class ValidationResult:
    """Result of validating a row or field."""
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationError]
    
    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


def validate_field(
    field_name: str,
    value: Any,
    column_def: ColumnDefinition,
    existing_values: Optional[List[Any]] = None,
    is_edit: bool = False,
    original_value: Any = None
) -> ValidationResult:
    """Validate a single field value against its column definition."""
    errors = []
    warnings = []
    
    # Check required field
    if column_def.required:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            errors.append(ValidationError(
                field=field_name,
                message=f"{column_def.display_name} is required"
            ))
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)
    
    # Skip further validation if value is empty and not required
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return ValidationResult(is_valid=True, errors=errors, warnings=warnings)
    
    # Type-specific validation
    if column_def.data_type == "string":
        if not isinstance(value, str):
            value = str(value)
        
        if column_def.max_length and len(value) > column_def.max_length:
            errors.append(ValidationError(
                field=field_name,
                message=f"{column_def.display_name} must be {column_def.max_length} characters or less"
            ))
        
        if column_def.pattern:
            if not re.match(column_def.pattern, value):
                warnings.append(ValidationError(
                    field=field_name,
                    message=column_def.pattern_message or f"{column_def.display_name} format is not recommended",
                    severity="warning"
                ))
    
    elif column_def.data_type == "boolean":
        if not isinstance(value, bool):
            if isinstance(value, str):
                if value.upper() not in ('TRUE', 'FALSE', 'YES', 'NO', '1', '0'):
                    errors.append(ValidationError(
                        field=field_name,
                        message=f"{column_def.display_name} must be a boolean value"
                    ))
    
    # Check uniqueness
    if column_def.unique and existing_values:
        compare_value = str(value).strip().upper() if value else ""
        existing_upper = [str(v).strip().upper() for v in existing_values if v is not None]
        
        if is_edit and original_value:
            original_upper = str(original_value).strip().upper()
            existing_upper = [v for v in existing_upper if v != original_upper]
        
        if compare_value in existing_upper:
            errors.append(ValidationError(
                field=field_name,
                message=f"{column_def.display_name} '{value}' already exists"
            ))
    
    is_valid = len(errors) == 0
    return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)


def validate_row(
    row_data: Dict[str, Any],
    sheet_name: str,
    existing_df: Optional[pd.DataFrame] = None,
    is_edit: bool = False,
    edit_index: Optional[int] = None
) -> ValidationResult:
    """Validate an entire row of data."""
    all_errors = []
    all_warnings = []
    
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
        
        result = validate_field(
            field_name=col_name,
            value=value,
            column_def=col_def,
            existing_values=existing_values,
            is_edit=is_edit,
            original_value=original_value
        )
        
        all_errors.extend(result.errors)
        all_warnings.extend(result.warnings)
    
    is_valid = len(all_errors) == 0
    return ValidationResult(is_valid=is_valid, errors=all_errors, warnings=all_warnings)


# ============================================================================
# ALGORITHM EXPRESSION VALIDATION
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
    group_fact_counts = {}
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

