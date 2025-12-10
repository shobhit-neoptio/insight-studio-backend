"""
Level 2: Single-Sheet Validation.

This module provides validation for entire sheets, including:
- Duplicate primary key detection
- Internal references within sheet (e.g., target nodeIds)
- dataType/isDeterministic compatibility
- Range validation for INTEGER nodes
- enumerationType validation for ENUMERATION nodes
"""

import re
from typing import Dict, List, Any, Optional, Set
import pandas as pd

from .base import (
    ValidationLevel,
    ValidationIssue,
    ValidationResult,
    NULL_VALUE,
    VALID_DATA_TYPES,
    VALID_DETERMINISTIC_VALUES,
    is_comment,
    is_row_to_skip,
    get_str_value,
)
from .field_validator import validate_row


# ============================================================================
# LEVEL 2: SINGLE-SHEET VALIDATION
# ============================================================================

def validate_sheet(
    sheet_name: str,
    df: pd.DataFrame,
    enumerations_df: Optional[pd.DataFrame] = None
) -> ValidationResult:
    """
    Level 2: Validate all rows in a single sheet.
    
    Includes Level 1 validations plus:
    - Duplicate primary key detection
    - Internal references within sheet (e.g., target nodeIds)
    - dataType/isDeterministic compatibility
    - Range validation for INTEGER nodes
    - enumerationType validation for ENUMERATION nodes
    
    Args:
        sheet_name: Name of the sheet
        df: DataFrame to validate
        enumerations_df: Optional enumerations data for cross-reference
    
    Returns:
        ValidationResult with all issues found
    """
    result = ValidationResult(is_valid=True, level=ValidationLevel.SINGLE_SHEET)
    
    if df.empty:
        return result
    
    # Track seen values for duplicate detection
    primary_keys = _get_primary_key_columns(sheet_name)
    seen_values: Dict[str, Set[str]] = {pk: set() for pk in primary_keys}
    
    # For Level Facts sheets, track nodeIds per assessment
    node_ids_per_assessment: Dict[str, Set[str]] = {}
    fact_ids_per_assessment: Dict[str, Set[str]] = {}
    
    # For Finding Relationships, track unique combinations
    seen_relationships: Set[str] = set()
    
    # First pass: validate each row
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        excel_row = int(idx) + 2
        
        # Level 1 validation
        row_result = validate_row(
            row_data=row_dict,
            sheet_name=sheet_name,
            existing_df=None,  # We do duplicate check separately
            is_edit=False,
            row_number=excel_row
        )
        result.merge(row_result)
        
        # Level 2: Duplicate primary key detection
        for pk in primary_keys:
            pk_value = get_str_value(row_dict, pk)
            if pk_value != NULL_VALUE and not is_comment(pk_value):
                pk_upper = pk_value.upper()
                if pk_upper in seen_values[pk]:
                    result.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=excel_row,
                        field=pk,
                        severity="error",
                        message=f"Duplicate {pk}='{pk_value}' found",
                        level=ValidationLevel.SINGLE_SHEET
                    ))
                seen_values[pk].add(pk_upper)
        
        # Level 2: Sheet-specific validations
        if _is_level_facts_sheet(sheet_name):
            _validate_level_facts_row(
                result, row_dict, excel_row, sheet_name,
                node_ids_per_assessment, fact_ids_per_assessment,
                enumerations_df
            )
        
        elif sheet_name.upper().startswith('ENUMERATIONS'):
            _validate_enumeration_row(result, row_dict, excel_row, sheet_name)
        
        elif sheet_name.upper() == 'FINDINGS':
            _validate_findings_row(result, row_dict, excel_row, sheet_name)
        
        elif sheet_name.upper() == 'FINDINGS RELATIONSHIPS':
            _validate_finding_relationships_row(result, row_dict, excel_row, sheet_name, seen_relationships)
    
    # Second pass: validate targets reference valid nodeIds (for Level Facts)
    if _is_level_facts_sheet(sheet_name):
        _validate_level_facts_targets(result, df, sheet_name, node_ids_per_assessment)
    
    return result


# ============================================================================
# PRIVATE HELPERS
# ============================================================================

def _get_primary_key_columns(sheet_name: str) -> List[str]:
    """Get primary key columns for a sheet."""
    sheet_upper = sheet_name.upper()
    if sheet_upper == 'MODULES':
        return ['moduleCode']
    elif sheet_upper == 'ASSESSMENTS':
        return ['assessmentCode']
    elif sheet_upper == 'FINDINGS':
        return ['findingCode']
    elif sheet_upper == 'FINDINGS RELATIONSHIPS':
        return []  # No single primary key, combination is unique (handled specially)
    elif 'LEVEL' in sheet_upper and 'FACTS' in sheet_upper:
        return []  # nodeId is unique per assessment, handled specially
    elif sheet_upper.startswith('ENUMERATIONS'):
        return []  # value is unique per enumerationType, handled specially
    elif sheet_upper.startswith('ALGORITHMS'):
        return ['algorithmId']
    return []


def _is_level_facts_sheet(sheet_name: str) -> bool:
    """Check if sheet is a Level Facts sheet."""
    upper = sheet_name.upper()
    return 'LEVEL' in upper and 'FACTS' in upper


def _validate_level_facts_row(
    result: ValidationResult,
    row_dict: Dict[str, Any],
    excel_row: int,
    sheet_name: str,
    node_ids_per_assessment: Dict[str, Set[str]],
    fact_ids_per_assessment: Dict[str, Set[str]],
    enumerations_df: Optional[pd.DataFrame]
):
    """Validate a single Level Facts row (Level 2)."""
    assessment_id = get_str_value(row_dict, 'assessmentId').upper()
    node_id = get_str_value(row_dict, 'nodeId').upper()
    fact_id = get_str_value(row_dict, 'factId').upper()
    fact_group = get_str_value(row_dict, 'factGroup').upper()
    data_type = get_str_value(row_dict, 'dataType').upper()
    is_deterministic = get_str_value(row_dict, 'isDeterministic').upper()
    enumeration_type = get_str_value(row_dict, 'enumerationType')
    range_val = get_str_value(row_dict, 'range').upper()
    
    if is_row_to_skip(assessment_id, 'assessmentId'):
        return
    
    # Initialize tracking sets
    if assessment_id not in node_ids_per_assessment:
        node_ids_per_assessment[assessment_id] = set()
        fact_ids_per_assessment[assessment_id] = set()
    
    # Duplicate nodeId within assessment
    if node_id != NULL_VALUE and not is_comment(node_id):
        if node_id in node_ids_per_assessment[assessment_id]:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='nodeId',
                severity="error",
                message=f"assessment='{assessment_id}', nodeId='{node_id}' - duplicate nodeId found",
                level=ValidationLevel.SINGLE_SHEET
            ))
        node_ids_per_assessment[assessment_id].add(node_id)
    
    # Duplicate factId within assessment
    if fact_id != NULL_VALUE:
        if fact_id in fact_ids_per_assessment[assessment_id]:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='factId',
                severity="error",
                message=f"assessment='{assessment_id}', nodeId='{node_id}' - duplicate factId='{fact_id}' found",
                level=ValidationLevel.SINGLE_SHEET
            ))
        fact_ids_per_assessment[assessment_id].add(fact_id)
    
    # Valid dataType
    if data_type == NULL_VALUE:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='dataType',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - missing dataType",
            level=ValidationLevel.SINGLE_SHEET
        ))
    elif data_type not in VALID_DATA_TYPES:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='dataType',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - invalid dataType='{data_type}'",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # Valid isDeterministic
    if is_deterministic == NULL_VALUE:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='isDeterministic',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - missing isDeterministic",
            level=ValidationLevel.SINGLE_SHEET
        ))
    elif is_deterministic not in VALID_DETERMINISTIC_VALUES:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='isDeterministic',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - invalid isDeterministic='{is_deterministic}'",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # DELEGATE only for ENUMERATION
    if is_deterministic == "DELEGATE" and data_type != "ENUMERATION":
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='isDeterministic',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - isDeterministic='DELEGATE' not allowed for dataType='{data_type}'",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # factId not allowed for STATEMENT
    if data_type == "STATEMENT" and fact_id != NULL_VALUE:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='factId',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - STATEMENT cannot have a factId",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # factGroup not allowed for non-enumeration
    if fact_group != NULL_VALUE and data_type in ["INTEGER", "STATEMENT", "CONVERSATION"]:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='factGroup',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - {data_type} cannot have a factGroup",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # Range validation for INTEGER
    if data_type == "INTEGER":
        if range_val == NULL_VALUE:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='range',
                severity="error",
                message=f"assessment='{assessment_id}', nodeId='{node_id}' - range is mandatory for INTEGER",
                level=ValidationLevel.SINGLE_SHEET
            ))
        elif not re.match(r'^\d+-\d+$', range_val):
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='range',
                severity="error",
                message=f"assessment='{assessment_id}', nodeId='{node_id}' - invalid range format '{range_val}'",
                level=ValidationLevel.SINGLE_SHEET
            ))
    elif range_val != NULL_VALUE:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='range',
            severity="error",
            message=f"assessment='{assessment_id}', nodeId='{node_id}' - range not allowed for {data_type}",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # enumerationType required for ENUMERATION
    if data_type == "ENUMERATION":
        if enumeration_type == NULL_VALUE:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='enumerationType',
                severity="error",
                message=f"assessment='{assessment_id}', nodeId='{node_id}' - missing enumerationType",
                level=ValidationLevel.SINGLE_SHEET
            ))
        elif enumerations_df is not None:
            # Check if enumerationType exists
            if 'enumerationType' in enumerations_df.columns:
                enum_types = enumerations_df['enumerationType'].dropna().unique()
                if enumeration_type not in enum_types:
                    result.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=excel_row,
                        field='enumerationType',
                        severity="error",
                        message=f"assessment='{assessment_id}', nodeId='{node_id}' - invalid enumerationType='{enumeration_type}'",
                        level=ValidationLevel.SINGLE_SHEET
                    ))


def _validate_enumeration_row(
    result: ValidationResult,
    row_dict: Dict[str, Any],
    excel_row: int,
    sheet_name: str
):
    """Validate a single Enumeration row (Level 2)."""
    enum_type = get_str_value(row_dict, 'enumerationType')
    value = get_str_value(row_dict, 'value')
    tags = get_str_value(row_dict, 'tags').upper()
    
    if enum_type == NULL_VALUE or is_comment(enum_type):
        return
    
    # Value is required
    if value == NULL_VALUE:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='value',
            severity="error",
            message=f"enumerationType='{enum_type}' - missing value",
            level=ValidationLevel.SINGLE_SHEET
        ))
    
    # Tags validation
    if tags != NULL_VALUE and 'DELEGATE=' not in tags:
        result.add_issue(ValidationIssue(
            sheet=sheet_name,
            row=excel_row,
            field='tags',
            severity="error",
            message=f"enumerationType='{enum_type}' - invalid tags='{tags}' - only 'DELEGATE=' supported",
            level=ValidationLevel.SINGLE_SHEET
        ))


def _validate_findings_row(
    result: ValidationResult,
    row_dict: Dict[str, Any],
    excel_row: int,
    sheet_name: str
):
    """Validate a single Findings row (Level 2)."""
    finding_code = get_str_value(row_dict, 'findingCode')
    icd_code = get_str_value(row_dict, 'icdCode')
    tags = get_str_value(row_dict, 'tags')
    
    if finding_code == NULL_VALUE or is_comment(finding_code):
        return
    
    # ICD code format validation (if provided)
    if icd_code != NULL_VALUE:
        # ICD-10 format: Letter followed by 2 digits, optionally followed by a decimal and 1-4 digits
        # Examples: F32, F32.1, A01.0, Z99.89
        icd_pattern = r'^[A-Z]\d{2}(\.\d{1,4})?$'
        if not re.match(icd_pattern, icd_code.upper()):
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='icdCode',
                severity="warning",
                message=f"findingCode='{finding_code}' - ICD code '{icd_code}' may not be in standard format (e.g., F32.1, A01.0)",
                level=ValidationLevel.SINGLE_SHEET
            ))
    
    # Tags format validation (comma-separated)
    if tags != NULL_VALUE:
        # Check that tags are properly formatted (comma-separated, no empty tags)
        tag_list = [t.strip() for t in tags.split(',')]
        empty_tags = [t for t in tag_list if t == '']
        if empty_tags:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='tags',
                severity="warning",
                message=f"findingCode='{finding_code}' - tags contain empty values (check for consecutive commas)",
                level=ValidationLevel.SINGLE_SHEET
            ))


# Valid relationship types
VALID_RELATIONSHIP_TYPES = {"DIFFERENTIAL", "COMORBID", "SUBTYPE", "EXCLUDES", "RELATED"}


def _validate_finding_relationships_row(
    result: ValidationResult,
    row_dict: Dict[str, Any],
    excel_row: int,
    sheet_name: str,
    seen_relationships: Set[str]
):
    """Validate a single Finding Relationships row (Level 2)."""
    source_finding = get_str_value(row_dict, 'sourceFindingCode')
    target_finding = get_str_value(row_dict, 'targetFindingCode')
    relationship_type = get_str_value(row_dict, 'relationshipTypeCode')
    descriptor = get_str_value(row_dict, 'descriptor')
    
    # Skip empty or comment rows
    if source_finding == NULL_VALUE or is_comment(source_finding):
        return
    
    # Validate relationshipTypeCode is one of valid types
    if relationship_type != NULL_VALUE:
        relationship_upper = relationship_type.upper()
        if relationship_upper not in VALID_RELATIONSHIP_TYPES:
            valid_list = ', '.join(sorted(VALID_RELATIONSHIP_TYPES))
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='relationshipTypeCode',
                severity="warning",
                message=f"relationshipTypeCode='{relationship_type}' is not a recognized type. Valid types: {valid_list}",
                level=ValidationLevel.SINGLE_SHEET
            ))
    
    # Check for self-referencing relationship
    if source_finding != NULL_VALUE and target_finding != NULL_VALUE:
        if source_finding.upper() == target_finding.upper():
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='targetFindingCode',
                severity="warning",
                message=f"Self-referencing relationship: sourceFindingCode and targetFindingCode are both '{source_finding}'",
                level=ValidationLevel.SINGLE_SHEET
            ))
    
    # Check for duplicate relationship (same source+target+type combination)
    if source_finding != NULL_VALUE and target_finding != NULL_VALUE and relationship_type != NULL_VALUE:
        relationship_key = f"{source_finding.upper()}|{target_finding.upper()}|{relationship_type.upper()}"
        if relationship_key in seen_relationships:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='sourceFindingCode',
                severity="error",
                message=f"Duplicate relationship: '{source_finding}' -> '{target_finding}' ({relationship_type}) already exists",
                level=ValidationLevel.SINGLE_SHEET
            ))
        seen_relationships.add(relationship_key)


def _validate_level_facts_targets(
    result: ValidationResult,
    df: pd.DataFrame,
    sheet_name: str,
    node_ids_per_assessment: Dict[str, Set[str]]
):
    """Validate that targets reference valid nodeIds within the assessment."""
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        excel_row = int(idx) + 2
        
        assessment_id = get_str_value(row_dict, 'assessmentId').upper()
        node_id = get_str_value(row_dict, 'nodeId').upper()
        target = get_str_value(row_dict, 'target').upper()
        data_type = get_str_value(row_dict, 'dataType').upper()
        
        if is_row_to_skip(assessment_id, 'assessmentId') or target == NULL_VALUE:
            continue
        
        node_id_set = node_ids_per_assessment.get(assessment_id, set())
        
        # Parse target list
        if "|" in target:
            target_list = [v.strip() for v in target.split('|')]
        else:
            target_list = [v.strip() for v in target.split(',')]
        
        # Non-enumeration can only have single target
        if data_type != "ENUMERATION" and len(target_list) > 1:
            result.add_issue(ValidationIssue(
                sheet=sheet_name,
                row=excel_row,
                field='target',
                severity="error",
                message=f"assessment='{assessment_id}', nodeId='{node_id}' - only single target allowed for {data_type}",
                level=ValidationLevel.SINGLE_SHEET
            ))
        
        # Check each target exists
        for t in target_list:
            if t not in node_id_set and t != "EXIT" and t != NULL_VALUE:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='target',
                    severity="error",
                    message=f"assessment='{assessment_id}', nodeId='{node_id}' - target='{t}' not found",
                    level=ValidationLevel.SINGLE_SHEET
                ))


# ============================================================================
# FK REFERENCE VALIDATION (WARNING-LEVEL)
# ============================================================================

def validate_fk_references(
    row_dict: Dict[str, Any],
    sheet_name: str,
    excel_row: int,
    fk_options: Dict[str, List[str]]
) -> ValidationResult:
    """
    Validate foreign key references in a row (produces warnings, not errors).
    
    This is lenient validation - allows submission but warns about invalid references.
    
    Args:
        row_dict: Row data to validate
        sheet_name: Name of the sheet
        excel_row: Excel row number for error reporting
        fk_options: Dict with available FK values:
            - moduleCodes: List of valid module codes
            - assessmentCodes: List of valid assessment codes  
            - findingCodes: List of valid finding codes
            - enumerationTypes: List of valid enumeration types
    
    Returns:
        ValidationResult with warnings for invalid FK references
    """
    result = ValidationResult(is_valid=True, level=ValidationLevel.SINGLE_SHEET)
    
    sheet_upper = sheet_name.upper()
    
    # Assessments: moduleCode -> Modules
    if sheet_upper == 'ASSESSMENTS':
        module_code = get_str_value(row_dict, 'moduleCode')
        if module_code != NULL_VALUE and not is_comment(module_code):
            module_codes = fk_options.get('moduleCodes', [])
            # Case-insensitive comparison
            if module_code.upper() not in [m.upper() for m in module_codes]:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='moduleCode',
                    severity="warning",
                    message=f"moduleCode='{module_code}' not found in Modules - will need to be created",
                    level=ValidationLevel.SINGLE_SHEET
                ))
    
    # Level Facts: assessmentId -> Assessments, enumerationType -> Enumerations
    elif _is_level_facts_sheet(sheet_name):
        # Validate assessmentId
        assessment_id = get_str_value(row_dict, 'assessmentId')
        if assessment_id != NULL_VALUE and not is_comment(assessment_id):
            assessment_codes = fk_options.get('assessmentCodes', [])
            if assessment_id.upper() not in [a.upper() for a in assessment_codes]:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='assessmentId',
                    severity="warning",
                    message=f"assessmentId='{assessment_id}' not found in Assessments - will need to be created",
                    level=ValidationLevel.SINGLE_SHEET
                ))
        
        # Validate enumerationType (for ENUMERATION dataType)
        data_type = get_str_value(row_dict, 'dataType').upper()
        if data_type == "ENUMERATION":
            enum_type = get_str_value(row_dict, 'enumerationType')
            if enum_type != NULL_VALUE:
                enum_types = fk_options.get('enumerationTypes', [])
                if enum_type not in enum_types:
                    result.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=excel_row,
                        field='enumerationType',
                        severity="warning",
                        message=f"enumerationType='{enum_type}' not found in Enumerations - will need to be created",
                        level=ValidationLevel.SINGLE_SHEET
                    ))
    
    # Algorithms: moduleCode, assessmentCode, findingCode
    elif sheet_upper.startswith('ALGORITHMS'):
        # Validate moduleCode
        module_code = get_str_value(row_dict, 'moduleCode')
        if module_code != NULL_VALUE and not is_comment(module_code):
            module_codes = fk_options.get('moduleCodes', [])
            if module_code.upper() not in [m.upper() for m in module_codes]:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='moduleCode',
                    severity="warning",
                    message=f"moduleCode='{module_code}' not found in Modules - will need to be created",
                    level=ValidationLevel.SINGLE_SHEET
                ))
        
        # Validate assessmentCode
        assessment_code = get_str_value(row_dict, 'assessmentCode')
        if assessment_code != NULL_VALUE and not is_comment(assessment_code):
            assessment_codes = fk_options.get('assessmentCodes', [])
            # Handle comma-separated assessment codes
            for ac in [a.strip() for a in assessment_code.split(',')]:
                if ac and ac.upper() not in [a.upper() for a in assessment_codes]:
                    result.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=excel_row,
                        field='assessmentCode',
                        severity="warning",
                        message=f"assessmentCode='{ac}' not found in Assessments - will need to be created",
                        level=ValidationLevel.SINGLE_SHEET
                    ))
        
        # Validate findingCode
        finding_code = get_str_value(row_dict, 'findingCode')
        if finding_code != NULL_VALUE and not is_comment(finding_code):
            finding_codes = fk_options.get('findingCodes', [])
            if finding_code.upper() not in [f.upper() for f in finding_codes]:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='findingCode',
                    severity="warning",
                    message=f"findingCode='{finding_code}' not found in Findings - will need to be created",
                    level=ValidationLevel.SINGLE_SHEET
                ))
    
    # Findings Relationships: sourceFindingCode, targetFindingCode -> Findings
    elif sheet_upper == 'FINDINGS RELATIONSHIPS':
        finding_codes = fk_options.get('findingCodes', [])
        finding_codes_upper = [f.upper() for f in finding_codes]
        
        # Validate sourceFindingCode
        source_code = get_str_value(row_dict, 'sourceFindingCode')
        if source_code != NULL_VALUE and not is_comment(source_code):
            if source_code.upper() not in finding_codes_upper:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='sourceFindingCode',
                    severity="warning",
                    message=f"sourceFindingCode='{source_code}' not found in Findings - will need to be created",
                    level=ValidationLevel.SINGLE_SHEET
                ))
        
        # Validate targetFindingCode
        target_code = get_str_value(row_dict, 'targetFindingCode')
        if target_code != NULL_VALUE and not is_comment(target_code):
            if target_code.upper() not in finding_codes_upper:
                result.add_issue(ValidationIssue(
                    sheet=sheet_name,
                    row=excel_row,
                    field='targetFindingCode',
                    severity="warning",
                    message=f"targetFindingCode='{target_code}' not found in Findings - will need to be created",
                    level=ValidationLevel.SINGLE_SHEET
                ))
    
    return result


# ============================================================================
# PUBLIC API
# ============================================================================

__all__ = [
    "validate_sheet",
    "validate_fk_references",
]
