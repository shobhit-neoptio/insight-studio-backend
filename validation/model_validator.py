"""
Level 3: Full Model Validation (Cross-Sheet / Batch).

This module provides validation for the entire clinical model,
including cross-sheet reference checks and integrity validation.

Validates:
- Cross-sheet reference checks
- Assessment catalog integrity
- Finding relationships
- Algorithm validation (fact/group existence, syntax, boolean mappings)
- Duration calculations
"""

import re
from typing import Dict, List, Any, Optional, Set
import pandas as pd

from .base import (
    ValidationLevel,
    ValidationIssue,
    ValidationResult,
    ValidationCategory,
    ModelValidationResult,
    NULL_VALUE,
    VALID_DATA_TYPES,
    VALID_DETERMINISTIC_VALUES,
    VALID_ALGORITHM_EVENTS,
    VALID_RELATIONSHIP_TYPE_CODES,
    DURATION_MATRIX,
    is_comment,
    is_row_to_skip,
    is_empty,
    get_str_value,
    convert_string_to_bool,
    normalize_algorithm,
)


# ============================================================================
# LEVEL 3: FULL MODEL VALIDATOR CLASS
# ============================================================================

class FullModelValidator:
    """
    Level 3: Full model validator for cross-sheet validation.
    
    Validates the entire Excel model including:
    - Cross-sheet reference checks
    - Assessment catalog integrity
    - Finding relationships
    - Algorithm validation (fact/group existence, syntax, boolean mappings)
    - Duration calculations
    """
    
    def __init__(self, session_data: Dict[str, pd.DataFrame]):
        """
        Initialize validator with Excel sheet data.
        
        Args:
            session_data: Dict mapping sheet names to pandas DataFrames
        """
        self.session_data = session_data
        self.result = ModelValidationResult(is_valid=True, level=ValidationLevel.FULL_MODEL)
        
        # Internal model structures
        self.modules: Dict[str, Dict] = {}
        self.assessments: Dict[str, Dict] = {}
        self.enumerations: Dict[str, List[Dict]] = {}
        self.findings: Dict[str, Dict] = {}
        self.findings_relationships: List[Dict] = []
        self.grouped_nodes: Dict[str, Dict] = {}
        self.algorithms: List[Dict] = []
        self.fact_id_set: Set[str] = set()
        self.fact_group_set: Set[str] = set()
        
        # Build issues found during model construction
        self.build_issues: List[ValidationIssue] = []
        
        # Build model structure
        self._build_model()
    
    def _build_model(self):
        """Build internal model structure from DataFrames."""
        self._build_modules()
        self._build_assessments()
        self._build_enumerations()
        self._build_findings()
        self._build_findings_relationships()
        self._build_grouped_nodes()
        self._build_algorithms()
    
    def _build_modules(self):
        """Build modules dict from Modules sheet."""
        if 'Modules' not in self.session_data:
            return
        
        df = self.session_data['Modules']
        if df.empty:
            return
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            module_code = get_str_value(row_dict, 'moduleCode')
            
            if module_code == NULL_VALUE or is_comment(module_code):
                continue
            
            module_code = module_code.upper()
            row_index = int(idx)  # 0-based index for frontend array access
            
            if module_code in self.modules:
                first_row = self.modules[module_code]['row']
                self.build_issues.append(ValidationIssue(
                    sheet='Modules',
                    row=row_index,
                    field='moduleCode',
                    severity='error',
                    message=f"Duplicate moduleCode='{module_code}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=module_code,
                    suggestion=f"Use a unique moduleCode. '{module_code}' was first defined on row {first_row}.",
                    rule_description="Each moduleCode must be unique within the Modules sheet."
                ))
                continue
            
            self.modules[module_code] = {
                'row': row_index,
                'sheet': 'Modules',
                'moduleCode': module_code,
                'isUserAssignable': get_str_value(row_dict, 'isUserAssignable').upper(),
                'name': get_str_value(row_dict, 'name'),
                'clientFriendlyName': get_str_value(row_dict, 'clientFriendlyName'),
                'description': get_str_value(row_dict, 'description'),
                'estimatedDuration': get_str_value(row_dict, 'estimatedDuration'),
            }
    
    def _build_assessments(self):
        """Build assessments dict from Assessments sheet."""
        if 'Assessments' not in self.session_data:
            return
        
        df = self.session_data['Assessments']
        if df.empty:
            return
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            assessment_code = get_str_value(row_dict, 'assessmentCode')
            
            if assessment_code == NULL_VALUE or is_comment(assessment_code):
                continue
            
            assessment_code = assessment_code.upper()
            row_index = int(idx)  # 0-based index for frontend array access
            
            if assessment_code in self.assessments:
                first_row = self.assessments[assessment_code]['row']
                self.build_issues.append(ValidationIssue(
                    sheet='Assessments',
                    row=row_index,
                    field='assessmentCode',
                    severity='error',
                    message=f"Duplicate assessmentCode='{assessment_code}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=assessment_code,
                    suggestion=f"Use a unique assessmentCode. '{assessment_code}' was first defined on row {first_row}.",
                    rule_description="Each assessmentCode must be unique within the Assessments sheet."
                ))
                continue
            
            self.assessments[assessment_code] = {
                'row': row_index,
                'sheet': 'Assessments',
                'assessmentCode': assessment_code,
                'assessmentId': assessment_code,
                'moduleCode': get_str_value(row_dict, 'moduleCode').upper(),
                'isUserAssignable': get_str_value(row_dict, 'isUserAssignable').upper(),
                'name': get_str_value(row_dict, 'name'),
                'clientFriendlyName': get_str_value(row_dict, 'clientFriendlyName'),
                'description': get_str_value(row_dict, 'description'),
                'estimatedDuration': get_str_value(row_dict, 'estimatedDuration'),
            }
    
    def _build_enumerations(self):
        """Build enumerations dict from Enumerations sheet(s)."""
        enum_sheets = [s for s in self.session_data.keys() 
                       if s.upper().startswith('ENUMERATIONS')]
        
        for sheet_name in enum_sheets:
            df = self.session_data[sheet_name]
            if df.empty:
                continue
            
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                enum_type = get_str_value(row_dict, 'enumerationType')
                
                if enum_type == NULL_VALUE or is_comment(enum_type):
                    continue
                
                if enum_type not in self.enumerations:
                    self.enumerations[enum_type] = []
                
                derived_value = row_dict.get('derivedValue') or row_dict.get('derivedBooleanValue')
                derived_bool = None
                if derived_value is not None:
                    if isinstance(derived_value, bool):
                        derived_bool = derived_value
                    elif isinstance(derived_value, str):
                        upper_val = derived_value.strip().upper()
                        if upper_val == 'TRUE':
                            derived_bool = True
                        elif upper_val == 'FALSE':
                            derived_bool = False
                
                self.enumerations[enum_type].append({
                    'value': get_str_value(row_dict, 'value'),
                    'derivedBooleanValue': derived_bool,
                    'tags': get_str_value(row_dict, 'tags').upper(),
                    'sequence': get_str_value(row_dict, 'seq'),
                    'languageCode': get_str_value(row_dict, 'languageCode').upper(),
                    'row': int(idx),  # 0-based index for frontend array access
                    'sheet': sheet_name
                })
    
    def _build_findings(self):
        """Build findings dict from Findings sheet."""
        if 'Findings' not in self.session_data:
            return
        
        df = self.session_data['Findings']
        if df.empty:
            return
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            finding_code = get_str_value(row_dict, 'findingCode')
            
            if finding_code == NULL_VALUE or is_comment(finding_code):
                continue
            
            finding_code = finding_code.upper()
            row_index = int(idx)  # 0-based index for frontend array access
            
            if finding_code in self.findings:
                first_row = self.findings[finding_code]['row']
                self.build_issues.append(ValidationIssue(
                    sheet='Findings',
                    row=row_index,
                    field='findingCode',
                    severity='error',
                    message=f"Duplicate findingCode='{finding_code}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=finding_code,
                    suggestion=f"Use a unique findingCode. '{finding_code}' was first defined on row {first_row}.",
                    rule_description="Each findingCode must be unique within the Findings sheet."
                ))
                continue
            
            self.findings[finding_code] = {
                'row': row_index,
                'sheet': 'Findings',
                'findingCode': finding_code,
                'name': get_str_value(row_dict, 'name'),
                'clientFriendlyName': get_str_value(row_dict, 'clientFriendlyName'),
                'icdCode': get_str_value(row_dict, 'icdCode'),
                'tags': get_str_value(row_dict, 'tags'),
            }
    
    def _build_findings_relationships(self):
        """Build findings_relationships list from Findings Relationships sheet."""
        if 'Findings Relationships' not in self.session_data:
            return
        
        df = self.session_data['Findings Relationships']
        if df.empty:
            return
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            source_code = get_str_value(row_dict, 'sourceFindingCode')
            
            if source_code == NULL_VALUE or is_comment(source_code):
                continue
            
            self.findings_relationships.append({
                'row': int(idx),  # 0-based index for frontend array access
                'sheet': 'Findings Relationships',
                'sourceFindingCode': source_code.upper(),
                'targetFindingCode': get_str_value(row_dict, 'targetFindingCode').upper(),
                'sequence': get_str_value(row_dict, 'sequence'),
                'relationshipTypeCode': get_str_value(row_dict, 'relationshipTypeCode').upper(),
                'descriptor': get_str_value(row_dict, 'descriptor'),
            })
    
    def _build_grouped_nodes(self):
        """Build grouped_nodes from Level Facts sheets."""
        fact_sheets = []
        for sheet_name in self.session_data.keys():
            if sheet_name.upper().startswith('LEVEL') and 'FACTS' in sheet_name.upper():
                fact_sheets.append(sheet_name)
            elif sheet_name not in ['Modules', 'Assessments', 'Findings', 'Findings Relationships', 'Algorithms']:
                df = self.session_data[sheet_name]
                if not df.empty and 'nodeId' in df.columns:
                    fact_sheets.append(sheet_name)
        
        for sheet_name in fact_sheets:
            df = self.session_data[sheet_name]
            if df.empty:
                continue
            
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                node_id = get_str_value(row_dict, 'nodeId')
                
                if is_row_to_skip(node_id, 'nodeId'):
                    continue
                
                node_id = node_id.upper()
                assessment_id = get_str_value(row_dict, 'assessmentId').upper()
                fact_id = get_str_value(row_dict, 'factId').upper()
                fact_group = get_str_value(row_dict, 'factGroup').upper()
                
                node = {
                    'sheet': sheet_name,
                    'row': int(idx),  # 0-based index for frontend array access
                    'nodeId': node_id,
                    'assessmentId': assessment_id,
                    'factId': fact_id,
                    'factGroup': fact_group,
                    'nodeText': get_str_value(row_dict, 'nodeText'),
                    'dataType': get_str_value(row_dict, 'dataType').upper(),
                    'isDeterministic': get_str_value(row_dict, 'isDeterministic').upper(),
                    'range': get_str_value(row_dict, 'range').upper(),
                    'enumerationType': get_str_value(row_dict, 'enumerationType'),
                    'target': get_str_value(row_dict, 'target').upper(),
                    'restartPoint': convert_string_to_bool(get_str_value(row_dict, 'isRestartPoint')),
                }
                
                if assessment_id != NULL_VALUE:
                    if assessment_id not in self.grouped_nodes:
                        self.grouped_nodes[assessment_id] = {
                            'nodes': [],
                            'row': int(idx),  # 0-based index for frontend array access
                            'sheet': sheet_name,
                            'cross_sheet_reported': False
                        }
                    else:
                        existing_sheet = self.grouped_nodes[assessment_id]['sheet']
                        if existing_sheet != sheet_name:
                            if not self.grouped_nodes[assessment_id].get('cross_sheet_reported'):
                                self.build_issues.append(ValidationIssue(
                                    sheet=sheet_name,
                                    row=int(idx),  # 0-based index for frontend array access
                                    field='assessmentId',
                                    severity='error',
                                    message=f"assessmentId='{assessment_id}' found on multiple sheets",
                                    level=ValidationLevel.FULL_MODEL,
                                    current_value=assessment_id,
                                    suggestion=f"Keep all nodes for assessment '{assessment_id}' on a single sheet. Found on both '{existing_sheet}' and '{sheet_name}'.",
                                    rule_description="All nodes belonging to the same assessment must be on the same Level Facts sheet."
                                ))
                                self.grouped_nodes[assessment_id]['cross_sheet_reported'] = True
                            continue
                    self.grouped_nodes[assessment_id]['nodes'].append(node)
                
                if fact_id != NULL_VALUE:
                    self.fact_id_set.add(fact_id)
                
                if fact_group != NULL_VALUE:
                    self.fact_group_set.add(fact_group)
    
    def _build_algorithms(self):
        """Build algorithms list from Algorithms sheet(s)."""
        algo_sheets = [s for s in self.session_data.keys() 
                       if s.upper().startswith('ALGORITHMS')]
        
        for sheet_name in algo_sheets:
            df = self.session_data[sheet_name]
            if df.empty:
                continue
            
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                
                skip_row = get_str_value(row_dict, 'skipRow').upper()
                algorithm = get_str_value(row_dict, 'algorithm')
                
                if skip_row == 'YES' or is_row_to_skip(algorithm, 'algorithm'):
                    continue
                
                normalized_algo = normalize_algorithm(algorithm)
                event = get_str_value(row_dict, 'event').upper()
                finding_code = get_str_value(row_dict, 'findingCode').upper()
                module_code = get_str_value(row_dict, 'moduleCode').upper()
                assessment_code = get_str_value(row_dict, 'assessmentCode').upper()
                
                if event == "ASSESSMENT" and assessment_code != NULL_VALUE:
                    assessment_codes = [v.strip().upper() for v in assessment_code.split(',')]
                    for ac in assessment_codes:
                        self.algorithms.append({
                            'row': int(idx),  # 0-based index for frontend array access
                            'sheet': sheet_name,
                            'algorithmId': get_str_value(row_dict, 'algorithmId'),
                            'algorithm': normalized_algo,
                            'originalAlgorithm': algorithm,
                            'event': event,
                            'moduleCode': module_code,
                            'assessmentCode': ac,
                            'findingCode': finding_code,
                        })
                else:
                    self.algorithms.append({
                        'row': int(idx),  # 0-based index for frontend array access
                        'sheet': sheet_name,
                        'algorithmId': get_str_value(row_dict, 'algorithmId'),
                        'algorithm': normalized_algo,
                        'originalAlgorithm': algorithm,
                        'event': event,
                        'moduleCode': module_code,
                        'assessmentCode': assessment_code,
                        'findingCode': finding_code,
                    })
    
    def validate(self) -> ModelValidationResult:
        """Run full model validation."""
        self._validate_catalog_integrity()
        self._validate_assessment_catalog()
        self._calculate_assessment_durations()
        self._validate_enumerations()
        self._validate_assessments()
        self._validate_findings_relationships()
        self._validate_algorithms()
        
        return self.result
    
    def _validate_catalog_integrity(self):
        """Report duplicate/cross-sheet issues found during build."""
        category = ValidationCategory(name="Catalog Integrity")
        for issue in self.build_issues:
            category.add_issue(issue)
        self.result.add_category(category)
    
    def _validate_assessment_catalog(self):
        """Validate cross-references between sheets."""
        category = ValidationCategory(name="Assessment Catalog")
        
        # Get list of valid codes for suggestions
        valid_assessment_codes = list(self.assessments.keys())
        valid_module_codes = list(self.modules.keys())
        
        # Check assessments in Level Facts exist in Assessments catalog
        for assessment_code, value in self.grouped_nodes.items():
            if assessment_code not in self.assessments:
                category.add_issue(ValidationIssue(
                    sheet=value['sheet'],
                    row=value['row'],
                    field="assessmentCode",
                    severity="error",
                    message=f"assessmentCode='{assessment_code}' not found in Assessments catalog",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=assessment_code,
                    expected_values=valid_assessment_codes[:10] if valid_assessment_codes else None,
                    suggestion=f"Add '{assessment_code}' to the Assessments sheet, or correct the assessmentId in this row.",
                    rule_description="Every assessmentId referenced in Level Facts must exist in the Assessments catalog."
                ))
        
        # Check moduleCode in Assessments exists in Modules
        for assessment_id, item in self.assessments.items():
            module_code = item.get('moduleCode', NULL_VALUE)
            if module_code != NULL_VALUE and module_code not in self.modules:
                category.add_issue(ValidationIssue(
                    sheet=item['sheet'],
                    row=item['row'],
                    field="moduleCode",
                    severity="error",
                    message=f"moduleCode='{module_code}' not found in Modules catalog",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=module_code,
                    expected_values=valid_module_codes[:10] if valid_module_codes else None,
                    suggestion=f"Add '{module_code}' to the Modules sheet, or use one of the existing module codes.",
                    rule_description="Every moduleCode referenced in Assessments must exist in the Modules catalog."
                ))
        
        self.result.add_category(category)
    
    def _calculate_assessment_durations(self):
        """Calculate and validate assessment durations."""
        category = ValidationCategory(name="Duration Calculation")
        
        for assessment_id, value in self.grouped_nodes.items():
            min_secs = 0
            max_secs = 0
            
            for node in value['nodes']:
                data_type = node['dataType']
                is_det = node['isDeterministic']
                
                if data_type in DURATION_MATRIX:
                    if is_det in DURATION_MATRIX[data_type]:
                        min_secs += DURATION_MATRIX[data_type][is_det]["min_duration_secs"]
                        max_secs += DURATION_MATRIX[data_type][is_det]["max_duration_secs"]
            
            min_mins = (min_secs + 59) // 60
            max_mins = (max_secs + 59) // 60
            
            if min_mins == max_mins:
                duration_str = f"{min_mins} minute{'s' if min_mins != 1 else ''}"
            else:
                duration_str = f"{min_mins} - {max_mins} minutes"
            
            if assessment_id in self.assessments:
                specified = self.assessments[assessment_id].get('estimatedDuration', NULL_VALUE)
                if specified == NULL_VALUE or is_empty(specified):
                    self.assessments[assessment_id]['estimatedDuration'] = duration_str
                    category.add_issue(ValidationIssue(
                        sheet="Assessments",
                        row=self.assessments[assessment_id]['row'],
                        field="estimatedDuration",
                        severity="info",
                        message=f"Calculated duration for '{assessment_id}': {duration_str}",
                        level=ValidationLevel.FULL_MODEL,
                        current_value="(not specified)",
                        suggestion=f"You can override this by setting estimatedDuration manually to a value like '{duration_str}'.",
                        rule_description="Duration is calculated based on the number and types of nodes in the assessment."
                    ))
        
        self.result.add_category(category)
    
    def _validate_enumerations(self):
        """Validate enumeration values."""
        category = ValidationCategory(name="Enumerations")
        
        for enum_type, value_list in self.enumerations.items():
            seen_values: Set[str] = set()
            
            for enum_item in value_list:
                sheet = enum_item['sheet']
                row = enum_item['row']
                value = enum_item['value']
                
                if value == NULL_VALUE:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="value",
                        severity="error",
                        message=f"enumerationType='{enum_type}' - missing value",
                        level=ValidationLevel.FULL_MODEL,
                        current_value="(empty)",
                        suggestion=f"Provide a value for this enumeration entry in type '{enum_type}'.",
                        rule_description="Each enumeration entry must have a non-empty value."
                    ))
                elif value in seen_values:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="value",
                        severity="error",
                        message=f"enumerationType='{enum_type}' - duplicate value '{value}'",
                        level=ValidationLevel.FULL_MODEL,
                        current_value=value,
                        suggestion=f"Use a unique value within enumerationType '{enum_type}'. This value already exists.",
                        rule_description="Each value within an enumerationType must be unique."
                    ))
                else:
                    seen_values.add(value)
                
                tags = enum_item.get('tags', NULL_VALUE)
                if tags != NULL_VALUE and 'DELEGATE=' not in tags:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="tags",
                        severity="error",
                        message=f"Invalid tags='{tags}' - only 'DELEGATE=' supported",
                        level=ValidationLevel.FULL_MODEL,
                        current_value=tags,
                        expected_values=["DELEGATE=<nodeId>"],
                        suggestion="Use 'DELEGATE=<nodeId>' format for tags, or leave the field empty.",
                        rule_description="The tags field only supports the DELEGATE= syntax for delegation."
                    ))
        
        self.result.add_category(category)
    
    def _validate_assessments(self):
        """Validate assessment nodes."""
        category = ValidationCategory(name="Assessments")
        
        # Get available enumeration types for suggestions
        available_enum_types = list(self.enumerations.keys())
        
        for assessment_id, assessment_dict in self.grouped_nodes.items():
            node_list = assessment_dict['nodes']
            node_id_set: Set[str] = set()
            
            # First pass: collect nodeIds
            for node in node_list:
                node_id = node.get('nodeId', NULL_VALUE)
                if node_id != NULL_VALUE and not is_comment(node_id):
                    node_id_set.add(node_id)
            
            # Validate each node
            for node in node_list:
                sheet = node['sheet']
                row = node['row']
                node_id = node.get('nodeId', NULL_VALUE)
                data_type = node.get('dataType', NULL_VALUE)
                enumeration_type = node.get('enumerationType', NULL_VALUE)
                target = node.get('target', NULL_VALUE)
                
                # enumerationType must exist
                if data_type == "ENUMERATION":
                    if enumeration_type != NULL_VALUE and enumeration_type not in self.enumerations:
                        category.add_issue(ValidationIssue(
                            sheet=sheet, row=row, field="enumerationType",
                            severity="error",
                            message=f"Invalid enumerationType='{enumeration_type}'",
                            level=ValidationLevel.FULL_MODEL,
                            current_value=enumeration_type,
                            expected_values=available_enum_types[:10] if available_enum_types else None,
                            suggestion=f"Define '{enumeration_type}' in the Enumerations sheet, or use an existing type.",
                            rule_description="When dataType is ENUMERATION, the enumerationType must be defined in the Enumerations sheet."
                        ))
                
                # Validate targets
                if target != NULL_VALUE:
                    if "|" in target:
                        target_list = [v.strip() for v in target.split('|')]
                    else:
                        target_list = [v.strip() for v in target.split(',')]
                    
                    # Get valid targets for suggestion
                    valid_targets = list(node_id_set)[:10]
                    valid_targets.append("EXIT")
                    
                    for t in target_list:
                        if t not in node_id_set and t != "EXIT" and t != NULL_VALUE:
                            category.add_issue(ValidationIssue(
                                sheet=sheet, row=row, field="target",
                                severity="error",
                                message=f"target='{t}' not found in assessment",
                                level=ValidationLevel.FULL_MODEL,
                                current_value=t,
                                expected_values=valid_targets,
                                suggestion=f"Use a valid nodeId from this assessment, or 'EXIT' to end the flow.",
                                rule_description="Target must reference a valid nodeId within the same assessment, or 'EXIT'."
                            ))
        
        self.result.add_category(category)
    
    def _validate_findings_relationships(self):
        """Validate finding relationships."""
        category = ValidationCategory(name="Findings Relationships")
        
        # Get valid finding codes for suggestions
        valid_finding_codes = list(self.findings.keys())
        
        for item in self.findings_relationships:
            sheet = item['sheet']
            row = item['row']
            
            source = item.get('sourceFindingCode', NULL_VALUE)
            target = item.get('targetFindingCode', NULL_VALUE)
            rel_type = item.get('relationshipTypeCode', NULL_VALUE)
            
            if source not in self.findings:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="sourceFindingCode",
                    severity="error",
                    message=f"Invalid sourceFindingCode '{source}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=source,
                    expected_values=valid_finding_codes[:10] if valid_finding_codes else None,
                    suggestion=f"Add '{source}' to the Findings sheet, or use an existing findingCode.",
                    rule_description="sourceFindingCode must reference a valid finding defined in the Findings sheet."
                ))
            
            if target not in self.findings:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="targetFindingCode",
                    severity="error",
                    message=f"Invalid targetFindingCode '{target}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=target,
                    expected_values=valid_finding_codes[:10] if valid_finding_codes else None,
                    suggestion=f"Add '{target}' to the Findings sheet, or use an existing findingCode.",
                    rule_description="targetFindingCode must reference a valid finding defined in the Findings sheet."
                ))
            
            if rel_type != NULL_VALUE and rel_type not in VALID_RELATIONSHIP_TYPE_CODES:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="relationshipTypeCode",
                    severity="warning",
                    message=f"Unexpected relationshipTypeCode: '{rel_type}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=rel_type,
                    expected_values=list(VALID_RELATIONSHIP_TYPE_CODES),
                    suggestion=f"Use one of the supported relationship types: {', '.join(VALID_RELATIONSHIP_TYPE_CODES)}.",
                    rule_description="relationshipTypeCode should be one of the predefined relationship types."
                ))
        
        self.result.add_category(category)
    
    def _validate_algorithms(self):
        """Validate algorithms."""
        category = ValidationCategory(name="Algorithms")
        
        # Get valid codes for suggestions
        valid_module_codes = list(self.modules.keys())
        valid_assessment_codes = list(self.assessments.keys())
        valid_finding_codes = list(self.findings.keys())
        
        for algo in self.algorithms:
            sheet = algo['sheet']
            row = algo['row']
            algorithm = algo.get('algorithm', NULL_VALUE)
            original_algorithm = algo.get('originalAlgorithm', algorithm)
            event = algo.get('event', NULL_VALUE)
            module_code = algo.get('moduleCode', NULL_VALUE)
            assessment_code = algo.get('assessmentCode', NULL_VALUE)
            finding_code = algo.get('findingCode', NULL_VALUE)
            
            # Valid event
            if event not in VALID_ALGORITHM_EVENTS:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="event",
                    severity="error",
                    message=f"Invalid event '{event}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=event,
                    expected_values=list(VALID_ALGORITHM_EVENTS),
                    suggestion=f"Use one of: {', '.join(VALID_ALGORITHM_EVENTS)}.",
                    rule_description="The event field must be one of the predefined algorithm event types."
                ))
                continue
            
            # Event-specific validation
            if event == 'MODULE':
                if is_empty(module_code) or module_code == NULL_VALUE:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="moduleCode",
                        severity="error",
                        message="MODULE event requires moduleCode",
                        level=ValidationLevel.FULL_MODEL,
                        current_value="(empty)",
                        expected_values=valid_module_codes[:10] if valid_module_codes else None,
                        suggestion="Specify a moduleCode for this MODULE event algorithm.",
                        rule_description="Algorithms with event=MODULE must specify which module they apply to."
                    ))
                elif module_code not in self.modules:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="moduleCode",
                        severity="error",
                        message=f"Invalid moduleCode '{module_code}'",
                        level=ValidationLevel.FULL_MODEL,
                        current_value=module_code,
                        expected_values=valid_module_codes[:10] if valid_module_codes else None,
                        suggestion=f"Add '{module_code}' to the Modules sheet, or use an existing module code.",
                        rule_description="The moduleCode must reference a valid module defined in the Modules sheet."
                    ))
            
            elif event == 'ASSESSMENT':
                if is_empty(assessment_code) or assessment_code == NULL_VALUE:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="assessmentCode",
                        severity="error",
                        message="ASSESSMENT event requires assessmentCode",
                        level=ValidationLevel.FULL_MODEL,
                        current_value="(empty)",
                        expected_values=valid_assessment_codes[:10] if valid_assessment_codes else None,
                        suggestion="Specify an assessmentCode for this ASSESSMENT event algorithm.",
                        rule_description="Algorithms with event=ASSESSMENT must specify which assessment they apply to."
                    ))
                elif assessment_code not in self.assessments:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="assessmentCode",
                        severity="error",
                        message=f"Invalid assessmentCode '{assessment_code}'",
                        level=ValidationLevel.FULL_MODEL,
                        current_value=assessment_code,
                        expected_values=valid_assessment_codes[:10] if valid_assessment_codes else None,
                        suggestion=f"Add '{assessment_code}' to the Assessments sheet, or use an existing code.",
                        rule_description="The assessmentCode must reference a valid assessment defined in the Assessments sheet."
                    ))
            
            elif event == 'FINDING':
                if is_empty(finding_code) or finding_code == NULL_VALUE:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="findingCode",
                        severity="warning",
                        message="FINDING event missing findingCode",
                        level=ValidationLevel.FULL_MODEL,
                        current_value="(empty)",
                        expected_values=valid_finding_codes[:10] if valid_finding_codes else None,
                        suggestion="Consider specifying a findingCode for this FINDING event algorithm.",
                        rule_description="Algorithms with event=FINDING should specify the finding they produce."
                    ))
                elif finding_code not in self.findings:
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="findingCode",
                        severity="error",
                        message=f"Invalid findingCode '{finding_code}'",
                        level=ValidationLevel.FULL_MODEL,
                        current_value=finding_code,
                        expected_values=valid_finding_codes[:10] if valid_finding_codes else None,
                        suggestion=f"Add '{finding_code}' to the Findings sheet, or use an existing code.",
                        rule_description="The findingCode must reference a valid finding defined in the Findings sheet."
                    ))
            
            # Validate algorithm expression
            if algorithm.lower() != "start" and algorithm != NULL_VALUE:
                # NOT not allowed
                if " not " in algorithm.lower() or "not(" in algorithm.lower():
                    category.add_issue(ValidationIssue(
                        sheet=sheet, row=row, field="algorithm",
                        severity="error",
                        message="NOT operator is not allowed",
                        level=ValidationLevel.FULL_MODEL,
                        current_value=original_algorithm,
                        suggestion="Rewrite the expression without using NOT. Use positive conditions instead.",
                        rule_description="The NOT operator is not supported in algorithm expressions due to evaluation constraints."
                    ))
                
                # Validate fact/group references
                self._validate_algorithm_terms(category, algo)
            
            # Syntax check
            error = self._test_algorithm_syntax(algorithm)
            if error:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="algorithm",
                    severity="error",
                    message=error,
                    level=ValidationLevel.FULL_MODEL,
                    current_value=original_algorithm,
                    suggestion="Check parentheses matching, operator syntax (and/or), and function calls like fact('ID') or groups('NAME').response(True).",
                    rule_description="Algorithm expressions must be valid Python-like boolean expressions."
                ))
        
        self.result.add_category(category)
    
    def _validate_algorithm_terms(self, category: ValidationCategory, algo: Dict):
        """Validate fact and group references in algorithm."""
        sheet = algo['sheet']
        row = algo['row']
        algorithm = algo.get('algorithm', '')
        original_algorithm = algo.get('originalAlgorithm', algorithm)
        
        # Get sample valid fact IDs and groups for suggestions
        sample_fact_ids = list(self.fact_id_set)[:10]
        sample_groups = list(self.fact_group_set)[:10]
        
        # Extract fact references
        fact_pattern = r"fact\(['\"]([^'\"]+)['\"]\)"
        fact_matches = re.findall(fact_pattern, algorithm, re.IGNORECASE)
        
        for fact_id in fact_matches:
            if fact_id.upper() not in self.fact_id_set:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="algorithm",
                    severity="error",
                    message=f"Invalid factId '{fact_id}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=fact_id,
                    expected_values=sample_fact_ids if sample_fact_ids else None,
                    suggestion=f"Check that factId '{fact_id}' exists in one of the Level Facts sheets. Fact IDs are case-insensitive.",
                    rule_description="fact() references must use a valid factId defined in a Level Facts sheet."
                ))
        
        # Check for comparison operators in fact-based expressions
        # Only group-based expressions can use >, <, >=, <= operators
        if fact_matches:
            # Split algorithm into terms by 'and'/'or' and check each term containing fact()
            terms = re.split(r'\band\b|\bor\b', algorithm, flags=re.IGNORECASE)
            for term in terms:
                # Check if this term contains a fact() call
                if re.search(r'fact\s*\(', term, re.IGNORECASE):
                    # Check if this term contains comparison operators
                    # Use regex to match >=, <=, >, < but not == or !=
                    if re.search(r'>=|<=|(?<![=!])>(?!=)|(?<![=!])<(?!=)', term):
                        category.add_issue(ValidationIssue(
                            sheet=sheet, row=row, field="algorithm",
                            severity="error",
                            message="Comparison operators (>, <, >=, <=) cannot be used with fact() expressions",
                            level=ValidationLevel.FULL_MODEL,
                            current_value=original_algorithm,
                            suggestion="Use comparison operators only with groups() expressions. Fact expressions should use == or != for equality checks, or evaluate to True/False.",
                            rule_description="Only group-based expressions can use comparison operators (>, <, >=, <=). Fact-based expressions must use equality operators (==, !=) or boolean evaluation."
                        ))
                        break  # Only report once per algorithm
        
        # Extract group references
        group_pattern = r"groups\(['\"]([^'\"]+)['\"]\)"
        group_matches = re.findall(group_pattern, algorithm, re.IGNORECASE)
        
        for group_name in group_matches:
            if group_name.upper() not in self.fact_group_set:
                category.add_issue(ValidationIssue(
                    sheet=sheet, row=row, field="algorithm",
                    severity="error",
                    message=f"Invalid factGroup '{group_name}'",
                    level=ValidationLevel.FULL_MODEL,
                    current_value=group_name,
                    expected_values=sample_groups if sample_groups else None,
                    suggestion=f"Check that factGroup '{group_name}' exists in one of the Level Facts sheets. Group names are case-insensitive.",
                    rule_description="groups() references must use a valid factGroup name defined in a Level Facts sheet."
                ))
    
    def _test_algorithm_syntax(self, algorithm: str) -> Optional[str]:
        """Test algorithm syntax using Python eval."""
        class Group:
            def __init__(self, *args): pass
            def response(self, val): return 1
        
        class Groups:
            def __call__(self, *args): return Group()
        
        class Fact:
            def __call__(self, *args): return True
        
        class IsComplete:
            def __call__(self, *args): return True
        
        try:
            if algorithm.lower() == "start" or algorithm == NULL_VALUE:
                return None
            
            groups = Groups()
            fact = Fact()
            is_complete = IsComplete()
            eval(algorithm)
            return None
        except Exception as e:
            return f"Syntax error: {str(e)}"


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def validate_model(session_data: Dict[str, pd.DataFrame]) -> ModelValidationResult:
    """
    Level 3: Validate entire model with cross-sheet references.
    
    Args:
        session_data: Dict mapping sheet names to pandas DataFrames
    
    Returns:
        ModelValidationResult with all validation categories
    """
    validator = FullModelValidator(session_data)
    return validator.validate()


def validate_model_dict(session_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Level 3: Validate model and return result as dictionary.
    
    Args:
        session_data: Dict mapping sheet names to pandas DataFrames
    
    Returns:
        Dict representation of validation results
    """
    result = validate_model(session_data)
    return result.to_dict()


# ============================================================================
# PUBLIC API
# ============================================================================

__all__ = [
    "FullModelValidator",
    "validate_model",
    "validate_model_dict",
]
