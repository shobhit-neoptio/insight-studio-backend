"""
Model Validator - Comprehensive validation of clinical model data.
Ported from insight-cds-utilities validation logic.
"""

import re
from typing import Dict, List, Any, Optional, Set, Tuple
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
        
        for level in range(4):
            sheet_name = f"Level {level} Facts"
            if sheet_name in self.session_data:
                df = self.session_data[sheet_name]
                if not df.empty:
                    for idx, row in df.iterrows():
                        fact_id = get_str_value(row.to_dict(), 'factId')
                        if fact_id:
                            self.fact_ids.add(fact_id)
                            self.fact_data[fact_id] = {
                                'level': level,
                                'sheet': sheet_name,
                                'row': idx,
                                'factGroup': get_str_value(row.to_dict(), 'factGroup'),
                                'dataType': get_str_value(row.to_dict(), 'dataType'),
                                'enumerationType': get_str_value(row.to_dict(), 'enumerationType'),
                            }
                        
                        fact_group = get_str_value(row.to_dict(), 'factGroup')
                        if fact_group:
                            self.fact_groups.add(fact_group)
        
        # Enumeration types
        self.enumeration_types: Set[str] = set()
        self.enumeration_values: Dict[str, Set[str]] = {}  # type -> set of values
        
        # Try to extract from Level Facts sheets
        for level in range(4):
            sheet_name = f"Level {level} Facts"
            if sheet_name in self.session_data:
                df = self.session_data[sheet_name]
                if not df.empty and 'enumerationType' in df.columns:
                    for _, row in df.iterrows():
                        enum_type = get_str_value(row.to_dict(), 'enumerationType')
                        if enum_type:
                            self.enumeration_types.add(enum_type)
    
    def validate(self) -> ValidationResults:
        """Run all validations and return results."""
        # Validate each category
        self._validate_modules()
        self._validate_assessments()
        self._validate_level_facts()
        self._validate_algorithms()
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
    
    def _validate_level_facts(self):
        """Validate Level Facts sheets."""
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
            
            seen_fact_ids: Set[str] = set()
            
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                
                # Check factId
                fact_id = get_str_value(row_dict, 'factId')
                if is_empty(fact_id):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="factId",
                        severity="error",
                        message="Fact ID is required"
                    ))
                elif fact_id in seen_fact_ids:
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="factId",
                        severity="error",
                        message=f"Duplicate fact ID: '{fact_id}'"
                    ))
                else:
                    seen_fact_ids.add(fact_id)
                
                # Check nodeId
                node_id = get_str_value(row_dict, 'nodeId')
                if is_empty(node_id):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="nodeId",
                        severity="error",
                        message="Node ID is required"
                    ))
                
                # Check nodeText
                node_text = get_str_value(row_dict, 'nodeText')
                if is_empty(node_text):
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="nodeText",
                        severity="warning",
                        message=f"Node text is empty for fact '{fact_id}'"
                    ))
                
                # Check dataType
                data_type = get_str_value(row_dict, 'dataType').upper()
                valid_data_types = {'ENUMERATION', 'INTEGER', 'STATEMENT', 'CONVERSATION', 'BOOLEAN'}
                if not is_empty(data_type) and data_type not in valid_data_types:
                    category.add_issue(ValidationIssue(
                        sheet=sheet_name,
                        row=int(idx),
                        field="dataType",
                        severity="warning",
                        message=f"Unusual data type: '{data_type}'"
                    ))
                
                # Check enumerationType for ENUMERATION data type
                if data_type == 'ENUMERATION':
                    enum_type = get_str_value(row_dict, 'enumerationType')
                    if is_empty(enum_type):
                        category.add_issue(ValidationIssue(
                            sheet=sheet_name,
                            row=int(idx),
                            field="enumerationType",
                            severity="error",
                            message=f"Enumeration type required for ENUMERATION data type (fact: '{fact_id}')"
                        ))
                
                # Check assessmentId reference
                assessment_id = get_str_value(row_dict, 'assessmentId')
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
        """Validate an algorithm expression."""
        # Check for NOT (no longer allowed)
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
        
        # Validate fact() references
        fact_pattern = re.compile(r"fact\(['\"]([^'\"]+)['\"]\)")
        fact_matches = fact_pattern.findall(expression)
        
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
                # Check if fact is ENUMERATION and expression uses proper comparison
                fact_data = self.fact_data.get(fact_id, {})
                if fact_data.get('dataType', '').upper() == 'ENUMERATION':
                    # For enumeration, should compare to True/False
                    fact_expr_pattern = rf"fact\(['\"]({re.escape(fact_id)})['\"](?:\)|[^)]*\))\s*(==|!=)\s*(\w+)"
                    matches = re.findall(fact_expr_pattern, expression, re.IGNORECASE)
                    for match in matches:
                        if len(match) >= 3:
                            value = match[2].upper()
                            if value not in ['TRUE', 'FALSE']:
                                category.add_issue(ValidationIssue(
                                    sheet="Algorithms",
                                    row=row,
                                    field="algorithm",
                                    severity="warning",
                                    message=f"Enumeration fact '{fact_id}' should compare to True/False",
                                    context={"algorithm_id": algorithm_id}
                                ))
        
        # Validate groups() references
        group_pattern = re.compile(r"groups\(['\"]([^'\"]+)['\"]\)")
        group_matches = group_pattern.findall(expression)
        
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
        
        # Basic syntax validation using Python eval
        try:
            test_expr = fact_pattern.sub('True', expression)
            test_expr = re.sub(
                r"groups\(['\"][^'\"]+['\"]\)\.response\([^)]+\)\s*(==|>=)\s*\d+", 
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
