"""
Model Validator - Comprehensive validation of clinical model data.
Ported from insight-cds-utilities validation logic.
"""

import re
from typing import Dict, List, Any, Optional, Set, Tuple, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime
import pandas as pd


# Constants from original validator
COMMENT_PREFIXES = ["COMMENT", "--", "#"]
NULL_VALUE = "<NULL>"


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    sheet: str
    row: Optional[int]  # None for sheet-level issues
    field: Optional[str]
    severity: str  # "error" or "warning"
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationCategory:
    """Results for a validation category."""
    name: str
    passed: bool = True
    error_count: int = 0
    warning_count: int = 0
    issues: List[ValidationIssue] = field(default_factory=list)
    
    def add_issue(self, issue: ValidationIssue):
        self.issues.append(issue)
        if issue.severity == "error":
            self.error_count += 1
            self.passed = False
        else:
            self.warning_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issues": [i.to_dict() for i in self.issues]
        }


@dataclass
class ValidationResults:
    """Complete validation results."""
    overall_passed: bool = True
    total_errors: int = 0
    total_warnings: int = 0
    categories: Dict[str, ValidationCategory] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def add_category(self, category: ValidationCategory):
        self.categories[category.name] = category
        self.total_errors += category.error_count
        self.total_warnings += category.warning_count
        if not category.passed:
            self.overall_passed = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_passed": self.overall_passed,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "categories": {k: v.to_dict() for k, v in self.categories.items()},
            "timestamp": self.timestamp
        }


# Helper functions
def is_empty(value: Any) -> bool:
    """Check if a value is empty or null."""
    if value is None:
        return True
    if isinstance(value, str):
        return len(value.strip()) == 0 or value.upper() == NULL_VALUE
    if pd.isna(value):
        return True
    return False


def is_comment(text: str) -> bool:
    """Check if text is a comment."""
    if not text:
        return False
    text_upper = str(text).upper()
    for prefix in COMMENT_PREFIXES:
        if text_upper.startswith(prefix):
            return True
    return False


def get_str_value(row: Dict[str, Any], field: str) -> str:
    """Get string value from row dict, handling None and NaN."""
    value = row.get(field)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


class ModelValidator:
    """Validates clinical model data across all sheets."""
    
    def __init__(self, session_data: Dict[str, pd.DataFrame]):
        self.session_data = session_data
        self.results = ValidationResults()
        
        # Build lookup sets for cross-reference validation
        self._build_lookups()
    
    def _build_lookups(self):
        """Build lookup sets for validation."""
        # Module codes
        self.module_codes: Set[str] = set()
        if 'Modules' in self.session_data:
            df = self.session_data['Modules']
            if not df.empty and 'moduleCode' in df.columns:
                self.module_codes = {
                    str(c).strip() for c in df['moduleCode'].dropna() 
                    if str(c).strip()
                }
        
        # Assessment codes
        self.assessment_codes: Set[str] = set()
        if 'Assessments' in self.session_data:
            df = self.session_data['Assessments']
            if not df.empty and 'assessmentCode' in df.columns:
                self.assessment_codes = {
                    str(c).strip() for c in df['assessmentCode'].dropna() 
                    if str(c).strip()
                }
        
        # Fact IDs and groups from Level Facts sheets
        self.fact_ids: Set[str] = set()
        self.fact_groups: Set[str] = set()
        self.fact_data: Dict[str, Dict] = {}  # factId -> fact data
        # Track nodes per assessment for uniqueness validation
        self.nodes_by_assessment: Dict[str, Dict[str, List[Tuple[str, int, int]]]] = {}  # assessmentId -> {nodeId -> [(sheet, row, level)]}
        
        for level in range(4):
            sheet_name = f"Level {level} Facts"
            if sheet_name in self.session_data:
                df = self.session_data[sheet_name]
                if not df.empty:
                    for idx, row in df.iterrows():
                        row_dict = row.to_dict()
                        fact_id = get_str_value(row_dict, 'factId')
                        node_id = get_str_value(row_dict, 'nodeId')
                        assessment_id = get_str_value(row_dict, 'assessmentId')
                        
                        if fact_id:
                            self.fact_ids.add(fact_id)
                            self.fact_data[fact_id] = {
                                'level': level,
                                'sheet': sheet_name,
                                'row': idx,
                                'factGroup': get_str_value(row_dict, 'factGroup'),
                                'dataType': get_str_value(row_dict, 'dataType'),
                                'enumerationType': get_str_value(row_dict, 'enumerationType'),
                            }
                        
                        fact_group = get_str_value(row_dict, 'factGroup')
                        if fact_group:
                            self.fact_groups.add(fact_group)
                        
                        # Track nodeId per assessment for uniqueness validation
                        if assessment_id and node_id:
                            if assessment_id not in self.nodes_by_assessment:
                                self.nodes_by_assessment[assessment_id] = {}
                            if node_id not in self.nodes_by_assessment[assessment_id]:
                                self.nodes_by_assessment[assessment_id][node_id] = []
                            self.nodes_by_assessment[assessment_id][node_id].append((sheet_name, int(idx), level))
        
        # Enumeration types and values from Enumerations sheet
        self.enumeration_types: Set[str] = set()
        self.enumeration_values: Dict[str, List[Dict]] = {}  # type -> list of value dicts
        
        if 'Enumerations' in self.session_data:
            df = self.session_data['Enumerations']
            if not df.empty:
                for idx, row in df.iterrows():
                    row_dict = row.to_dict()
                    enum_type = get_str_value(row_dict, 'enumerationType')
                    value = get_str_value(row_dict, 'value')
                    
                    if enum_type:
                        self.enumeration_types.add(enum_type)
                        if enum_type not in self.enumeration_values:
                            self.enumeration_values[enum_type] = []
                        
                        # Get derivedBooleanValue
                        derived_bool = row_dict.get('derivedBooleanValue')
                        if isinstance(derived_bool, str):
                            derived_bool = derived_bool.upper() == 'TRUE'
                        
                        self.enumeration_values[enum_type].append({
                            'value': value,
                            'derivedBooleanValue': derived_bool,
                            'tags': get_str_value(row_dict, 'tags'),
                            'row': int(idx)
                        })
        
        # Also collect enumeration types referenced in Level Facts (for validation)
        for level in range(4):
            sheet_name = f"Level {level} Facts"
            if sheet_name in self.session_data:
                df = self.session_data[sheet_name]
                if not df.empty and 'enumerationType' in df.columns:
                    for _, row in df.iterrows():
                        enum_type = get_str_value(row.to_dict(), 'enumerationType')
                        if enum_type:
                            self.enumeration_types.add(enum_type)
        
        # Finding codes
        self.finding_codes: Set[str] = set()
        if 'Findings' in self.session_data:
            df = self.session_data['Findings']
            if not df.empty and 'findingCode' in df.columns:
                self.finding_codes = {
                    str(c).strip() for c in df['findingCode'].dropna() 
                    if str(c).strip()
                }
    
    def validate(self) -> ValidationResults:
        """Run all validations and return results."""
        # Validate each category in order for referential integrity
        self._validate_modules()
        self._validate_assessments()
        self._validate_enumerations()
        self._validate_level_facts()
        self._validate_algorithms()
        self._validate_findings()
        self._validate_findings_relationships()
        self._validate_cross_references()
        
        return self.results
    
    def _validate_modules(self):
        """Validate the Modules sheet."""
        category = ValidationCategory(name="Modules")
        
        if 'Modules' not in self.session_data:
            category.add_issue(ValidationIssue(
                sheet="Modules",
                row=None,
                field=None,
                severity="warning",
                message="Modules sheet not found"
            ))
            self.results.add_category(category)
            return
        
        df = self.session_data['Modules']
        if df.empty:
            category.add_issue(ValidationIssue(
                sheet="Modules",
                row=None,
                field=None,
                severity="warning",
                message="Modules sheet is empty"
            ))
            self.results.add_category(category)
            return
        
        seen_codes: Set[str] = set()
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Check moduleCode
            module_code = get_str_value(row_dict, 'moduleCode')
            if is_empty(module_code):
                category.add_issue(ValidationIssue(
                    sheet="Modules",
                    row=int(idx),
                    field="moduleCode",
                    severity="error",
                    message="Module code is required"
                ))
            elif module_code in seen_codes:
                category.add_issue(ValidationIssue(
                    sheet="Modules",
                    row=int(idx),
                    field="moduleCode",
                    severity="error",
                    message=f"Duplicate module code: '{module_code}'"
                ))
            else:
                seen_codes.add(module_code)
            
            # Check moduleName
            module_name = get_str_value(row_dict, 'moduleName')
            if is_empty(module_name):
                category.add_issue(ValidationIssue(
                    sheet="Modules",
                    row=int(idx),
                    field="moduleName",
                    severity="warning",
                    message=f"Module name is empty for '{module_code}'"
                ))
        
        self.results.add_category(category)
    
    def _validate_assessments(self):
        """Validate the Assessments sheet."""
        category = ValidationCategory(name="Assessments")
        
        if 'Assessments' not in self.session_data:
            category.add_issue(ValidationIssue(
                sheet="Assessments",
                row=None,
                field=None,
                severity="warning",
                message="Assessments sheet not found"
            ))
            self.results.add_category(category)
            return
        
        df = self.session_data['Assessments']
        if df.empty:
            category.add_issue(ValidationIssue(
                sheet="Assessments",
                row=None,
                field=None,
                severity="warning",
                message="Assessments sheet is empty"
            ))
            self.results.add_category(category)
            return
        
        seen_codes: Set[str] = set()
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Check assessmentCode
            assessment_code = get_str_value(row_dict, 'assessmentCode')
            if is_empty(assessment_code):
                category.add_issue(ValidationIssue(
                    sheet="Assessments",
                    row=int(idx),
                    field="assessmentCode",
                    severity="error",
                    message="Assessment code is required"
                ))
            elif assessment_code in seen_codes:
                category.add_issue(ValidationIssue(
                    sheet="Assessments",
                    row=int(idx),
                    field="assessmentCode",
                    severity="error",
                    message=f"Duplicate assessment code: '{assessment_code}'"
                ))
            else:
                seen_codes.add(assessment_code)
            
            # Check moduleCode reference
            module_code = get_str_value(row_dict, 'moduleCode')
            if not is_empty(module_code) and module_code not in self.module_codes:
                category.add_issue(ValidationIssue(
                    sheet="Assessments",
                    row=int(idx),
                    field="moduleCode",
                    severity="error",
                    message=f"Module code '{module_code}' not found in Modules sheet",
                    context={"referenced_code": module_code}
                ))
            
            # Check assessmentName
            assessment_name = get_str_value(row_dict, 'assessmentName')
            if is_empty(assessment_name):
                category.add_issue(ValidationIssue(
                    sheet="Assessments",
                    row=int(idx),
                    field="assessmentName",
                    severity="warning",
                    message=f"Assessment name is empty for '{assessment_code}'"
                ))
        
        self.results.add_category(category)
    
    def _validate_enumerations(self):
        """Validate the Enumerations sheet (Rules EN-01, EN-02, EN-03)."""
        category = ValidationCategory(name="Enumerations")
        
        if 'Enumerations' not in self.session_data:
            # Not an error - sheet can be absent
            self.results.add_category(category)
            return
        
        df = self.session_data['Enumerations']
        if df.empty:
            self.results.add_category(category)
            return
        
        # Track values per enumeration type for uniqueness check
        values_by_type: Dict[str, Set[str]] = {}
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            enum_type = get_str_value(row_dict, 'enumerationType')
            value = get_str_value(row_dict, 'value')
            tags = get_str_value(row_dict, 'tags')
            
            # Skip comment rows
            if is_comment(enum_type) or is_comment(value):
                continue
            
            # EN-01: Enumeration Value Must Be Present
            if is_empty(value):
                category.add_issue(ValidationIssue(
                    sheet="Enumerations",
                    row=int(idx),
                    field="value",
                    severity="error",
                    message=f"Enumeration value is required (enumerationType: '{enum_type}')"
                ))
                continue
            
            # EN-02: Enumeration Values Must Be Unique Within Type
            if enum_type:
                if enum_type not in values_by_type:
                    values_by_type[enum_type] = set()
                
                if value in values_by_type[enum_type]:
                    category.add_issue(ValidationIssue(
                        sheet="Enumerations",
                        row=int(idx),
                        field="value",
                        severity="error",
                        message=f"Duplicate value '{value}' in enumerationType '{enum_type}'"
                    ))
                else:
                    values_by_type[enum_type].add(value)
            
            # EN-03: Tags Must Follow DELEGATE= Format
            if not is_empty(tags) and 'DELEGATE=' not in tags.upper():
                category.add_issue(ValidationIssue(
                    sheet="Enumerations",
                    row=int(idx),
                    field="tags",
                    severity="error",
                    message=f"Invalid tags '{tags}' - only 'DELEGATE=' format is supported"
                ))
        
        self.results.add_category(category)
    
    def _validate_level_facts(self):
        """Validate Level Facts sheets with comprehensive AS-* rules."""
        # Valid data types per documentation
        valid_data_types = {'ENUMERATION', 'INTEGER', 'STATEMENT', 'CONVERSATION'}
        valid_is_deterministic = {'TRUE', 'FALSE', 'DELEGATE'}
        
        for level in range(4):
            sheet_name = f"Level {level} Facts"
            category = ValidationCategory(name=sheet_name)
            
            if sheet_name not in self.session_data:
                # Not an error - sheets can be empty
                self.results.add_category(category)
                continue
            
            df = self.session_data[sheet_name]
            if df.empty:
                self.results.add_category(category)
                continue
            
            # Build lookup of all nodeIds in this sheet for target validation
            all_node_ids_in_sheet: Set[str] = set()
            for _, row in df.iterrows():
                node_id = get_str_value(row.to_dict(), 'nodeId')
                if node_id and not is_comment(node_id):
                    all_node_ids_in_sheet.add(node_id)
            
            # Track uniqueness per assessment
            seen_node_ids_per_assessment: Dict[str, Set[str]] = {}
            seen_fact_ids_per_assessment: Dict[str, Set[str]] = {}
            
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                
                node_id = get_str_value(row_dict, 'nodeId')
                fact_id = get_str_value(row_dict, 'factId')
                assessment_id = get_str_value(row_dict, 'assessmentId')
                data_type = get_str_value(row_dict, 'dataType').upper()
                fact_group = get_str_value(row_dict, 'factGroup')
                is_deterministic = get_str_value(row_dict, 'isDeterministic').upper()
                range_val = get_str_value(row_dict, 'range')
                enum_type = get_str_value(row_dict, 'enumerationType')
                target = get_str_value(row_dict, 'target')
                node_text = get_str_value(row_dict, 'nodeText')
                
                # Skip comment rows
                if is_comment(node_id):
                    continue
                
                # AS-01: NodeId Is Required
                if is_empty(node_id):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="nodeId",
                        severity="error",
                        message=f"Node ID is required (assessment: '{assessment_id}')"
                    ))
                    continue  # Skip further validation for this row
                
                # AS-02: NodeId Must Be Unique Within Assessment
                if assessment_id:
                    if assessment_id not in seen_node_ids_per_assessment:
                        seen_node_ids_per_assessment[assessment_id] = set()
                    
                    if node_id in seen_node_ids_per_assessment[assessment_id]:
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="nodeId",
                            severity="error",
                            message=f"Duplicate nodeId '{node_id}' within assessment '{assessment_id}'"
                        ))
                    else:
                        seen_node_ids_per_assessment[assessment_id].add(node_id)
                
                # AS-03: NodeText Is Required
                if is_empty(node_text):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="nodeText",
                        severity="error",
                        message=f"Node text is required (assessment: '{assessment_id}', nodeId: '{node_id}')"
                    ))
                
                # AS-05: DataType Must Be Valid
                if is_empty(data_type):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="dataType",
                        severity="error",
                        message=f"Data type is required (nodeId: '{node_id}')"
                    ))
                elif data_type not in valid_data_types:
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="dataType",
                        severity="error",
                        message=f"Invalid dataType '{data_type}' - must be ENUMERATION, INTEGER, STATEMENT, or CONVERSATION"
                    ))
                
                # AS-06 & AS-07: FactId rules based on dataType
                if data_type == 'STATEMENT':
                    # AS-06: STATEMENT Nodes Cannot Have FactId
                    if not is_empty(fact_id):
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="factId",
                            severity="error",
                            message=f"STATEMENT node cannot have a factId (nodeId: '{node_id}')"
                        ))
                else:
                    # AS-07: Non-STATEMENT Nodes Should Have FactId (warning)
                    if is_empty(fact_id) and data_type in valid_data_types:
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="factId",
                            severity="warning",
                            message=f"Missing factId for {data_type} node '{node_id}'"
                        ))
                
                # AS-04: FactId Must Be Unique Within Assessment
                if not is_empty(fact_id) and assessment_id:
                    if assessment_id not in seen_fact_ids_per_assessment:
                        seen_fact_ids_per_assessment[assessment_id] = set()
                    
                    if fact_id in seen_fact_ids_per_assessment[assessment_id]:
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="factId",
                            severity="error",
                            message=f"Duplicate factId '{fact_id}' within assessment '{assessment_id}'"
                        ))
                    else:
                        seen_fact_ids_per_assessment[assessment_id].add(fact_id)
                
                # AS-08 & AS-09: FactGroup rules
                if not is_empty(fact_group):
                    # AS-08: Non-ENUMERATION Cannot Be In FactGroup
                    if data_type != 'ENUMERATION' and data_type in valid_data_types:
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="factGroup",
                            severity="error",
                            message=f"{data_type} node cannot be in a factGroup (nodeId: '{node_id}')"
                        ))
                elif data_type == 'ENUMERATION' and not is_empty(fact_id):
                    # AS-09: ENUMERATION Without FactGroup Warning
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="factGroup",
                        severity="warning",
                        message=f"Missing optional factGroup for enumeration factId '{fact_id}'"
                    ))
                
                # AS-10: isDeterministic Must Be Valid
                if is_empty(is_deterministic):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="isDeterministic",
                        severity="error",
                        message=f"isDeterministic is required (nodeId: '{node_id}')"
                    ))
                elif is_deterministic not in valid_is_deterministic:
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="isDeterministic",
                        severity="error",
                        message=f"Invalid isDeterministic '{is_deterministic}' - must be TRUE, FALSE, or DELEGATE"
                    ))
                elif is_deterministic == 'DELEGATE':
                    # AS-11: DELEGATE Only Valid For ENUMERATION
                    if data_type != 'ENUMERATION':
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="isDeterministic",
                            severity="error",
                            message=f"isDeterministic='DELEGATE' not allowed for dataType='{data_type}'"
                        ))
                
                # AS-12 & AS-13: Range rules for INTEGER
                if data_type == 'INTEGER':
                    # AS-12: Range Mandatory For INTEGER
                    if is_empty(range_val):
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="range",
                            severity="error",
                            message=f"Range is mandatory for dataType=INTEGER (nodeId: '{node_id}')"
                        ))
                    else:
                        # AS-13: Range Format Validation
                        range_pattern = re.compile(r'^\d+-\d+$')
                        if not range_pattern.match(range_val):
                            category.add_issue(ValidationIssue(
                                sheet=sheet_name,
                                row=int(idx),
                                field="range",
                                severity="error",
                                message=f"Invalid range '{range_val}' - must be format 'min-max' (e.g., '0-100')"
                            ))
                elif not is_empty(range_val):
                    # Range not allowed for non-INTEGER types
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="range",
                        severity="error",
                        message=f"Range not allowed for dataType='{data_type}' (nodeId: '{node_id}')"
                    ))
                
                # AS-14 & AS-15: EnumerationType rules
                if data_type == 'ENUMERATION':
                    # AS-14: EnumerationType Required For ENUMERATION
                    if is_empty(enum_type):
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="enumerationType",
                            severity="error",
                            message=f"enumerationType required for ENUMERATION dataType (nodeId: '{node_id}')"
                        ))
                    else:
                        # AS-15: EnumerationType Must Exist
                        if enum_type not in self.enumeration_values:
                            category.add_issue(ValidationIssue(
                                sheet=sheet_name,
                                row=int(idx),
                                field="enumerationType",
                                severity="error",
                                message=f"enumerationType '{enum_type}' not found in Enumerations sheet"
                            ))
                
                # AS-16, AS-17, AS-18, AS-19: Target rules
                # AS-16: Target Is Required
                if is_empty(target):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="target",
                        severity="error",
                        message=f"Target is required (nodeId: '{node_id}')"
                    ))
                else:
                    # Parse targets (pipe-separated for multiple)
                    targets = [t.strip() for t in target.split('|')]
                    
                    # AS-17: Target Must Reference Valid NodeId Or EXIT
                    for t in targets:
                        if t.upper() != 'EXIT' and t not in all_node_ids_in_sheet:
                            category.add_issue(ValidationIssue(
                                sheet=sheet_name,
                                row=int(idx),
                                field="target",
                                severity="error",
                                message=f"Target '{t}' not found in sheet (nodeId: '{node_id}')"
                            ))
                    
                    # AS-18 & AS-19: Target cardinality rules
                    if data_type == 'ENUMERATION' and enum_type in self.enumeration_values:
                        # AS-18: Target Cardinality Must Match Enumeration
                        enum_value_count = len(self.enumeration_values[enum_type])
                        if len(targets) > 1 and len(targets) != enum_value_count:
                            category.add_issue(ValidationIssue(
                                sheet=sheet_name,
                                row=int(idx),
                                field="target",
                                severity="error",
                                message=f"Target must be length 1 or same length as enumeration '{enum_type}' ({enum_value_count} values)"
                            ))
                    elif data_type != 'ENUMERATION' and len(targets) > 1:
                        # AS-19: Non-ENUMERATION Single Target Only
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="target",
                            severity="error",
                            message=f"Only a single target allowed for {data_type} nodes"
                        ))
                
                # Check assessmentId reference
                if not is_empty(assessment_id) and assessment_id not in self.assessment_codes:
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="assessmentId",
                        severity="error",
                        message=f"Assessment ID '{assessment_id}' not found in Assessments sheet",
                        context={"referenced_code": assessment_id}
                    ))
            
            self.results.add_category(category)
    
    def _validate_algorithms(self):
        """Validate the Algorithms sheet."""
        category = ValidationCategory(name="Algorithms")
        
        if 'Algorithms' not in self.session_data:
            # Not an error - can be empty
            self.results.add_category(category)
            return
        
        df = self.session_data['Algorithms']
        if df.empty:
            self.results.add_category(category)
            return
        
        seen_ids: Set[str] = set()
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Check algorithmId
            algorithm_id = get_str_value(row_dict, 'algorithmId')
            if is_empty(algorithm_id):
                category.add_issue(ValidationIssue(
                    sheet="Algorithms",
                    row=int(idx),
                    field="algorithmId",
                    severity="error",
                    message="Algorithm ID is required"
                ))
            elif algorithm_id in seen_ids:
                category.add_issue(ValidationIssue(
                    sheet="Algorithms",
                    row=int(idx),
                    field="algorithmId",
                    severity="error",
                    message=f"Duplicate algorithm ID: '{algorithm_id}'"
                ))
            else:
                seen_ids.add(algorithm_id)
            
            # Check event type
            event = get_str_value(row_dict, 'event').upper()
            valid_events = {'MODULE', 'ASSESSMENT', 'FINDING'}
            if not is_empty(event) and event not in valid_events:
                category.add_issue(ValidationIssue(
                    sheet="Algorithms",
                    row=int(idx),
                    field="event",
                    severity="error",
                    message=f"Invalid event type: '{event}'. Must be MODULE, ASSESSMENT, or FINDING"
                ))
            
            # Validate event-specific fields
            if event == 'MODULE':
                module_code = get_str_value(row_dict, 'moduleCode')
                if is_empty(module_code):
                    category.add_issue(ValidationIssue(
                        sheet="Algorithms",
                        row=int(idx),
                        field="moduleCode",
                        severity="error",
                        message="Module code required for MODULE event"
                    ))
                elif module_code not in self.module_codes:
                    category.add_issue(ValidationIssue(
                        sheet="Algorithms",
                        row=int(idx),
                        field="moduleCode",
                        severity="error",
                        message=f"Module code '{module_code}' not found in Modules sheet"
                    ))
            
            elif event == 'ASSESSMENT':
                assessment_code = get_str_value(row_dict, 'assessmentCode')
                if is_empty(assessment_code):
                    category.add_issue(ValidationIssue(
                        sheet="Algorithms",
                        row=int(idx),
                        field="assessmentCode",
                        severity="error",
                        message="Assessment code required for ASSESSMENT event"
                    ))
                elif assessment_code not in self.assessment_codes:
                    category.add_issue(ValidationIssue(
                        sheet="Algorithms",
                        row=int(idx),
                        field="assessmentCode",
                        severity="error",
                        message=f"Assessment code '{assessment_code}' not found in Assessments sheet"
                    ))
            
            elif event == 'FINDING':
                # AL-04: FINDING Event Requires findingCode (warning)
                finding_code = get_str_value(row_dict, 'findingCode')
                if is_empty(finding_code):
                    category.add_issue(ValidationIssue(
                        sheet="Algorithms",
                        row=int(idx),
                        field="findingCode",
                        severity="warning",
                        message="Finding code is recommended for FINDING event"
                    ))
                else:
                    # AL-05: findingCode Must Exist in Findings
                    if finding_code not in self.finding_codes:
                        category.add_issue(ValidationIssue(
                            sheet="Algorithms",
                            row=int(idx),
                            field="findingCode",
                            severity="error",
                            message=f"Finding code '{finding_code}' not found in Findings sheet"
                        ))
            
            # Validate algorithm expression
            algorithm = get_str_value(row_dict, 'algorithm')
            if not is_empty(algorithm) and algorithm.upper() != 'START':
                self._validate_algorithm_expression(
                    category, int(idx), algorithm_id, algorithm
                )
        
        self.results.add_category(category)
    
    def _validate_algorithm_expression(
        self, 
        category: ValidationCategory, 
        row: int, 
        algorithm_id: str, 
        expression: str
    ):
        """Validate an algorithm expression with comprehensive AL-* rules."""
        # AL-07: Check for NOT (no longer allowed)
        if ' NOT ' in expression.upper() or 'NOT(' in expression.upper():
            category.add_issue(ValidationIssue(
                sheet="Algorithms",
                row=row,
                field="algorithm",
                severity="error",
                message=f"NOT operator is no longer allowed in expressions",
                context={"algorithm_id": algorithm_id}
            ))
        
        # Check parentheses balance
        if expression.count('(') != expression.count(')'):
            category.add_issue(ValidationIssue(
                sheet="Algorithms",
                row=row,
                field="algorithm",
                severity="error",
                message="Unbalanced parentheses in expression",
                context={"algorithm_id": algorithm_id}
            ))
        
        # Define patterns
        fact_pattern = re.compile(r"fact\(['\"]([^'\"]+)['\"]\)")
        group_pattern = re.compile(r"groups\(['\"]([^'\"]+)['\"]\)")
        is_complete_pattern = re.compile(r"is_complete\(['\"]([^'\"]+)['\"]\)")
        
        fact_matches = fact_pattern.findall(expression)
        group_matches = group_pattern.findall(expression)
        is_complete_matches = is_complete_pattern.findall(expression)
        
        # AL-08: Terms Must Contain fact(), groups(), or is_complete()
        if not fact_matches and not group_matches and not is_complete_matches:
            category.add_issue(ValidationIssue(
                sheet="Algorithms",
                row=row,
                field="algorithm",
                severity="error",
                message="Expression must contain at least one fact(), groups(), or is_complete() call",
                context={"algorithm_id": algorithm_id}
            ))
        
        # AL-09: Validate fact() references
        for fact_id in fact_matches:
            if fact_id not in self.fact_ids:
                category.add_issue(ValidationIssue(
                    sheet="Algorithms",
                    row=row,
                    field="algorithm",
                    severity="error",
                    message=f"Fact ID '{fact_id}' not found in Level Facts sheets",
                    context={"algorithm_id": algorithm_id, "missing_fact": fact_id}
                ))
            else:
                fact_data = self.fact_data.get(fact_id, {})
                data_type = fact_data.get('dataType', '').upper()
                
                # Find the term containing this fact for operator analysis
                fact_term_pattern = rf"fact\(['\"]({re.escape(fact_id)})['\"](?:\)|[^)]*\))[^a-zA-Z]*([<>=!]+)[^a-zA-Z]*([a-zA-Z0-9]+)"
                term_matches = re.findall(fact_term_pattern, expression, re.IGNORECASE)
                
                if data_type == 'ENUMERATION':
                    # AL-10: ENUMERATION Facts Cannot Use < or > Operators
                    if '<' in expression or '>' in expression:
                        # Check if this fact specifically uses comparison operators
                        for match in term_matches:
                            if len(match) >= 2:
                                operator = match[1]
                                if '<' in operator or '>' in operator:
                                    category.add_issue(ValidationIssue(
                                        sheet="Algorithms",
                                        row=row,
                                        field="algorithm",
                                        severity="error",
                                        message=f"ENUMERATION fact '{fact_id}' cannot use < or > operators",
                                        context={"algorithm_id": algorithm_id}
                                    ))
                                    break
                    
                    # AL-11: ENUMERATION Facts Must Test TRUE/FALSE
                    has_bool_comparison = False
                    for match in term_matches:
                        if len(match) >= 3:
                            value = match[2].upper()
                            if value in ['TRUE', 'FALSE']:
                                has_bool_comparison = True
                                break
                    
                    if term_matches and not has_bool_comparison:
                        category.add_issue(ValidationIssue(
                            sheet="Algorithms",
                            row=row,
                            field="algorithm",
                            severity="error",
                            message=f"ENUMERATION fact '{fact_id}' must be compared to TRUE or FALSE",
                            context={"algorithm_id": algorithm_id}
                        ))
                
                elif data_type == 'INTEGER':
                    # AL-12: INTEGER Facts Must Use Comparison Operators
                    has_comparison = False
                    has_bool_value = False
                    
                    for match in term_matches:
                        if len(match) >= 2:
                            operator = match[1]
                            if operator in ['==', '!=', '<', '>', '<=', '>=']:
                                has_comparison = True
                            if len(match) >= 3:
                                value = match[2].upper()
                                if value in ['TRUE', 'FALSE']:
                                    has_bool_value = True
                    
                    if term_matches and (not has_comparison or has_bool_value):
                        category.add_issue(ValidationIssue(
                            sheet="Algorithms",
                            row=row,
                            field="algorithm",
                            severity="error",
                            message=f"INTEGER fact '{fact_id}' must use comparison operators and cannot test TRUE/FALSE",
                            context={"algorithm_id": algorithm_id}
                        ))
        
        # AL-13: Validate groups() references
        for group_name in group_matches:
            if group_name not in self.fact_groups:
                category.add_issue(ValidationIssue(
                    sheet="Algorithms",
                    row=row,
                    field="algorithm",
                    severity="error",
                    message=f"Fact group '{group_name}' not found in Level Facts sheets",
                    context={"algorithm_id": algorithm_id, "missing_group": group_name}
                ))
            else:
                # AL-14: All Facts In Group Must Map To Boolean
                self._validate_group_boolean_mapping(category, row, algorithm_id, group_name)
        
        # Basic syntax validation using Python eval
        try:
            test_expr = fact_pattern.sub('True', expression)
            test_expr = re.sub(
                r"groups\(['\"][^'\"]+['\"]\)\.response\([^)]+\)\s*(==|>=|<=|<|>|!=)\s*\d+", 
                'True', 
                test_expr
            )
            test_expr = re.sub(
                r"is_complete\(['\"][^'\"]+['\"]\)",
                'True',
                test_expr
            )
            compile(test_expr, '<string>', 'eval')
        except SyntaxError as e:
            category.add_issue(ValidationIssue(
                sheet="Algorithms",
                row=row,
                field="algorithm",
                severity="error",
                message=f"Syntax error in expression: {str(e)}",
                context={"algorithm_id": algorithm_id}
            ))
    
    def _validate_group_boolean_mapping(
        self,
        category: ValidationCategory,
        row: int,
        algorithm_id: str,
        group_name: str
    ):
        """AL-14: Validate that all facts in a group have boolean mappings."""
        # Find all facts in this group
        for fact_id, fact_data in self.fact_data.items():
            if fact_data.get('factGroup') == group_name:
                enum_type = fact_data.get('enumerationType')
                if enum_type and enum_type in self.enumeration_values:
                    # Check if all enum values have derivedBooleanValue
                    for enum_val in self.enumeration_values[enum_type]:
                        derived_bool = enum_val.get('derivedBooleanValue')
                        if derived_bool is None:
                            category.add_issue(ValidationIssue(
                                sheet="Algorithms",
                                row=row,
                                field="algorithm",
                                severity="error",
                                message=f"factGroup '{group_name}', factId '{fact_id}' uses enumerationType '{enum_type}' - value '{enum_val.get('value')}' missing derivedBooleanValue",
                                context={"algorithm_id": algorithm_id}
                            ))
                            # Only report one issue per group to avoid flooding
                            return
    
    def _validate_findings(self):
        """Validate the Findings sheet."""
        category = ValidationCategory(name="Findings")
        
        if 'Findings' not in self.session_data:
            # Not an error - sheet can be absent
            self.results.add_category(category)
            return
        
        df = self.session_data['Findings']
        if df.empty:
            self.results.add_category(category)
            return
        
        seen_codes: Set[str] = set()
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            finding_code = get_str_value(row_dict, 'findingCode')
            
            # Skip comment rows
            if is_comment(finding_code):
                continue
            
            # findingCode is required
            if is_empty(finding_code):
                category.add_issue(ValidationIssue(
                    sheet="Findings",
                    row=int(idx),
                    field="findingCode",
                    severity="error",
                    message="Finding code is required"
                ))
                continue
            
            # findingCode must be unique
            if finding_code in seen_codes:
                category.add_issue(ValidationIssue(
                    sheet="Findings",
                    row=int(idx),
                    field="findingCode",
                    severity="error",
                    message=f"Duplicate finding code: '{finding_code}'"
                ))
            else:
                seen_codes.add(finding_code)
            
            # name is required
            name = get_str_value(row_dict, 'name')
            if is_empty(name):
                category.add_issue(ValidationIssue(
                    sheet="Findings",
                    row=int(idx),
                    field="name",
                    severity="warning",
                    message=f"Finding name is empty for '{finding_code}'"
                ))
        
        self.results.add_category(category)
    
    def _validate_findings_relationships(self):
        """Validate Finding Relationships sheet (Rules FR-01 to FR-04)."""
        category = ValidationCategory(name="Finding Relationships")
        
        if 'Finding Relationships' not in self.session_data:
            # Not an error - sheet can be absent
            self.results.add_category(category)
            return
        
        df = self.session_data['Finding Relationships']
        if df.empty:
            self.results.add_category(category)
            return
        
        expected_relationship_types = {'CAUSAL'}
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            source_code = get_str_value(row_dict, 'sourceFindingCode')
            target_code = get_str_value(row_dict, 'targetFindingCode')
            rel_type = get_str_value(row_dict, 'relationshipTypeCode')
            descriptor = get_str_value(row_dict, 'descriptor')
            
            # Skip comment rows
            if is_comment(source_code):
                continue
            
            # FR-01: sourceFindingCode Must Exist
            if is_empty(source_code):
                category.add_issue(ValidationIssue(
                    sheet="Finding Relationships",
                    row=int(idx),
                    field="sourceFindingCode",
                    severity="error",
                    message="Source finding code is required"
                ))
            elif source_code not in self.finding_codes:
                category.add_issue(ValidationIssue(
                    sheet="Finding Relationships",
                    row=int(idx),
                    field="sourceFindingCode",
                    severity="error",
                    message=f"Source finding code '{source_code}' not found in Findings sheet"
                ))
            
            # FR-02: targetFindingCode Must Exist
            if is_empty(target_code):
                category.add_issue(ValidationIssue(
                    sheet="Finding Relationships",
                    row=int(idx),
                    field="targetFindingCode",
                    severity="error",
                    message="Target finding code is required"
                ))
            elif target_code not in self.finding_codes:
                category.add_issue(ValidationIssue(
                    sheet="Finding Relationships",
                    row=int(idx),
                    field="targetFindingCode",
                    severity="error",
                    message=f"Target finding code '{target_code}' not found in Findings sheet"
                ))
            
            # FR-03: Unexpected relationshipTypeCode Warning
            if not is_empty(rel_type) and rel_type.upper() not in expected_relationship_types:
                category.add_issue(ValidationIssue(
                    sheet="Finding Relationships",
                    row=int(idx),
                    field="relationshipTypeCode",
                    severity="warning",
                    message=f"Unexpected relationship type code: '{rel_type}'"
                ))
            
            # FR-04: Descriptor Is Mandatory
            if is_empty(descriptor):
                category.add_issue(ValidationIssue(
                    sheet="Finding Relationships",
                    row=int(idx),
                    field="descriptor",
                    severity="error",
                    message="Descriptor is mandatory for finding relationships"
                ))
        
        self.results.add_category(category)
    
    def _validate_cross_references(self):
        """Validate cross-references between sheets."""
        category = ValidationCategory(name="Cross-References")
        
        # Check that all assessments reference valid modules
        # (Already done in _validate_assessments, but we can add more here)
        
        # Check for orphaned facts (facts not in any assessment)
        facts_in_assessments: Set[str] = set()
        for level in range(4):
            sheet_name = f"Level {level} Facts"
            if sheet_name in self.session_data:
                df = self.session_data[sheet_name]
                if not df.empty and 'assessmentId' in df.columns:
                    for _, row in df.iterrows():
                        assessment_id = get_str_value(row.to_dict(), 'assessmentId')
                        if assessment_id:
                            facts_in_assessments.add(assessment_id)
        
        # Check if all referenced assessments exist
        for assessment_ref in facts_in_assessments:
            if assessment_ref not in self.assessment_codes:
                category.add_issue(ValidationIssue(
                    sheet="Cross-References",
                    row=None,
                    field=None,
                    severity="warning",
                    message=f"Assessment '{assessment_ref}' referenced in Level Facts but not found in Assessments sheet",
                    context={"referenced_code": assessment_ref}
                ))
        
        self.results.add_category(category)


def validate_model(session_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Main entry point for model validation."""
    validator = ModelValidator(session_data)
    results = validator.validate()
    return results.to_dict()
