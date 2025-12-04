"""
FastAPI backend for Excel Sheet Builder.
Provides REST API for Excel I/O and validation.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import pandas as pd
from io import BytesIO
import json
import re

from api.excel_io import ExcelManager, AVAILABLE_SHEETS, get_column_order, get_empty_row
from api.validation import (
    validate_row,
    validate_algorithm_expression_basic,
    validate_algorithm_expression_facts,
    validate_algorithm_expression_groups,
    get_available_modules,
    get_available_assessments,
    get_available_groups,
    get_group_info,
    suggest_algorithm_id,
)

app = FastAPI(
    title="Excel Sheet Builder API",
    description="API for building clinical model Excel sheets",
    version="1.0.0"
)

# CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174", "http://localhost:3000", "http://127.0.0.1:5173", "http://127.0.0.1:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for current session data
session_data: Dict[str, pd.DataFrame] = {}
excel_manager: Optional[ExcelManager] = None


# ============================================================================
# Pydantic Models
# ============================================================================

class SheetConfig(BaseModel):
    name: str
    description: str
    icon: str
    columns: List[Dict[str, Any]]
    column_order: List[str]


class RowData(BaseModel):
    data: Dict[str, Any]


class ValidationRequest(BaseModel):
    sheet_name: str
    row_data: Dict[str, Any]
    is_edit: bool = False
    edit_index: Optional[int] = None


class ExpressionValidationRequest(BaseModel):
    expression: str


class ValidationResponse(BaseModel):
    is_valid: bool
    errors: List[Dict[str, str]]
    warnings: List[Dict[str, str]]


# ============================================================================
# Startup
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize with empty sheets."""
    global session_data, excel_manager
    excel_manager = ExcelManager()
    session_data = excel_manager.create_new_workbook()


# ============================================================================
# Sheet Configuration Endpoints
# ============================================================================

@app.get("/api/config/sheets")
async def get_sheets_config() -> Dict[str, Any]:
    """Get configuration for all available sheets."""
    config = {}
    for sheet_name, sheet_info in AVAILABLE_SHEETS.items():
        columns = []
        for col_name in sheet_info["column_order"]:
            col_def = sheet_info["columns"][col_name]
            columns.append({
                "name": col_def.name,
                "display_name": col_def.display_name,
                "data_type": col_def.data_type,
                "required": col_def.required,
                "max_length": col_def.max_length,
                "default_value": col_def.default_value,
                "unique": col_def.unique,
                "help_text": col_def.help_text,
                "pattern": col_def.pattern,
                "pattern_message": col_def.pattern_message,
            })
        
        config[sheet_name] = {
            "name": sheet_info["name"],
            "description": sheet_info["description"],
            "icon": sheet_info["icon"],
            "columns": columns,
            "column_order": sheet_info["column_order"],
            "level": sheet_info.get("level"),
        }
    
    return config


# ============================================================================
# File Upload/Download Endpoints
# ============================================================================

@app.post("/api/upload")
async def upload_excel(file: UploadFile = File(...)) -> Dict[str, Any]:
    """Upload an Excel file and return all sheet data as JSON."""
    global session_data, excel_manager
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="File must be an Excel file (.xlsx or .xls)")
    
    try:
        content = await file.read()
        file_buffer = BytesIO(content)
        
        excel_manager = ExcelManager()
        session_data = excel_manager.load_from_file(file_buffer)
        
        # Convert DataFrames to JSON-serializable format
        result = {}
        for sheet_name, df in session_data.items():
            # Replace NaN with None and convert to records
            df_clean = df.where(pd.notnull(df), None)
            result[sheet_name] = df_clean.to_dict(orient='records')
        
        return {
            "success": True,
            "filename": file.filename,
            "sheets": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading file: {str(e)}")


@app.get("/api/download")
async def download_excel():
    """Download the current state as an Excel file."""
    global session_data, excel_manager
    
    if excel_manager is None:
        excel_manager = ExcelManager()
        session_data = excel_manager.create_new_workbook()
    
    # Update manager with current session data
    for sheet_name, df in session_data.items():
        excel_manager.update_sheet_data(sheet_name, df)
    
    try:
        content = excel_manager.save_to_file()
        
        return StreamingResponse(
            BytesIO(content),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=model_generator.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating file: {str(e)}")


@app.post("/api/reset")
async def reset_data() -> Dict[str, Any]:
    """Reset all data to empty sheets."""
    global session_data, excel_manager
    
    excel_manager = ExcelManager()
    session_data = excel_manager.create_new_workbook()
    
    result = {}
    for sheet_name, df in session_data.items():
        result[sheet_name] = df.to_dict(orient='records')
    
    return {"success": True, "sheets": result}


# ============================================================================
# Sheet CRUD Endpoints
# ============================================================================

@app.get("/api/sheets/{sheet_name}")
async def get_sheet_data(sheet_name: str) -> Dict[str, Any]:
    """Get all data for a specific sheet."""
    global session_data
    
    if sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {sheet_name}")
    
    if sheet_name not in session_data:
        columns = get_column_order(sheet_name)
        session_data[sheet_name] = pd.DataFrame(columns=columns)
    
    df = session_data[sheet_name]
    df_clean = df.where(pd.notnull(df), None)
    
    return {
        "sheet_name": sheet_name,
        "rows": df_clean.to_dict(orient='records'),
        "row_count": len(df)
    }


@app.post("/api/sheets/{sheet_name}/rows")
async def add_row(sheet_name: str, row: RowData) -> Dict[str, Any]:
    """Add a new row to a sheet."""
    global session_data
    
    if sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {sheet_name}")
    
    if sheet_name not in session_data:
        columns = get_column_order(sheet_name)
        session_data[sheet_name] = pd.DataFrame(columns=columns)
    
    df = session_data[sheet_name]
    
    # Validate the row
    validation = validate_row(row.data, sheet_name, df, is_edit=False)
    
    if not validation.is_valid:
        return {
            "success": False,
            "validation": {
                "is_valid": False,
                "errors": [{"field": e.field, "message": e.message} for e in validation.errors],
                "warnings": [{"field": w.field, "message": w.message} for w in validation.warnings]
            }
        }
    
    # Add the row
    new_row = pd.DataFrame([row.data])
    session_data[sheet_name] = pd.concat([df, new_row], ignore_index=True)
    
    df_clean = session_data[sheet_name].where(pd.notnull(session_data[sheet_name]), None)
    
    return {
        "success": True,
        "row_index": len(session_data[sheet_name]) - 1,
        "rows": df_clean.to_dict(orient='records')
    }


@app.put("/api/sheets/{sheet_name}/rows/{row_index}")
async def update_row(sheet_name: str, row_index: int, row: RowData) -> Dict[str, Any]:
    """Update an existing row in a sheet."""
    global session_data
    
    if sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {sheet_name}")
    
    if sheet_name not in session_data:
        raise HTTPException(status_code=404, detail="Sheet has no data")
    
    df = session_data[sheet_name]
    
    if row_index < 0 or row_index >= len(df):
        raise HTTPException(status_code=404, detail=f"Row index {row_index} out of range")
    
    # Validate the row
    validation = validate_row(row.data, sheet_name, df, is_edit=True, edit_index=row_index)
    
    if not validation.is_valid:
        return {
            "success": False,
            "validation": {
                "is_valid": False,
                "errors": [{"field": e.field, "message": e.message} for e in validation.errors],
                "warnings": [{"field": w.field, "message": w.message} for w in validation.warnings]
            }
        }
    
    # Update the row
    for col, value in row.data.items():
        if col in df.columns:
            df.at[row_index, col] = value
    
    session_data[sheet_name] = df
    df_clean = df.where(pd.notnull(df), None)
    
    return {
        "success": True,
        "rows": df_clean.to_dict(orient='records')
    }


@app.delete("/api/sheets/{sheet_name}/rows/{row_index}")
async def delete_row(sheet_name: str, row_index: int) -> Dict[str, Any]:
    """Delete a row from a sheet."""
    global session_data
    
    if sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {sheet_name}")
    
    if sheet_name not in session_data:
        raise HTTPException(status_code=404, detail="Sheet has no data")
    
    df = session_data[sheet_name]
    
    if row_index < 0 or row_index >= len(df):
        raise HTTPException(status_code=404, detail=f"Row index {row_index} out of range")
    
    # Delete the row
    df = df.drop(index=row_index).reset_index(drop=True)
    session_data[sheet_name] = df
    
    df_clean = df.where(pd.notnull(df), None)
    
    return {
        "success": True,
        "rows": df_clean.to_dict(orient='records')
    }


@app.delete("/api/sheets/{sheet_name}/rows")
async def delete_multiple_rows(sheet_name: str, indices: List[int]) -> Dict[str, Any]:
    """Delete multiple rows from a sheet."""
    global session_data
    
    if sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {sheet_name}")
    
    if sheet_name not in session_data:
        raise HTTPException(status_code=404, detail="Sheet has no data")
    
    df = session_data[sheet_name]
    
    # Filter valid indices
    valid_indices = [i for i in indices if 0 <= i < len(df)]
    
    if valid_indices:
        df = df.drop(index=valid_indices).reset_index(drop=True)
        session_data[sheet_name] = df
    
    df_clean = df.where(pd.notnull(df), None)
    
    return {
        "success": True,
        "deleted_count": len(valid_indices),
        "rows": df_clean.to_dict(orient='records')
    }


# ============================================================================
# Validation Endpoints
# ============================================================================

@app.post("/api/validate/row")
async def validate_row_data(request: ValidationRequest) -> ValidationResponse:
    """Validate a row of data."""
    global session_data
    
    if request.sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {request.sheet_name}")
    
    df = session_data.get(request.sheet_name, pd.DataFrame())
    
    result = validate_row(
        request.row_data,
        request.sheet_name,
        df,
        is_edit=request.is_edit,
        edit_index=request.edit_index
    )
    
    return ValidationResponse(
        is_valid=result.is_valid,
        errors=[{"field": e.field, "message": e.message} for e in result.errors],
        warnings=[{"field": w.field, "message": w.message} for w in result.warnings]
    )


@app.post("/api/validate/expression")
async def validate_expression(request: ExpressionValidationRequest) -> Dict[str, Any]:
    """Validate an algorithm expression."""
    global session_data
    
    # Basic syntax validation
    is_valid, messages = validate_algorithm_expression_basic(request.expression)
    
    # Fact reference validation
    facts_valid, facts_messages, found_facts, missing_facts = validate_algorithm_expression_facts(
        request.expression, session_data
    )
    
    # Group reference validation
    groups_valid, groups_messages, found_groups, missing_groups, group_counts = validate_algorithm_expression_groups(
        request.expression, session_data
    )
    
    return {
        "is_valid": is_valid and facts_valid and groups_valid,
        "syntax": {
            "is_valid": is_valid,
            "messages": messages
        },
        "facts": {
            "is_valid": facts_valid,
            "messages": facts_messages,
            "found": found_facts,
            "missing": missing_facts
        },
        "groups": {
            "is_valid": groups_valid,
            "messages": groups_messages,
            "found": found_groups,
            "missing": missing_groups,
            "counts": group_counts
        }
    }


# ============================================================================
# Reference Data Endpoints
# ============================================================================

@app.get("/api/facts")
async def get_all_facts() -> Dict[str, Any]:
    """Get all facts from Level Facts sheets."""
    global session_data
    
    facts = []
    for level in range(4):
        sheet_name = f"Level {level} Facts"
        if sheet_name in session_data:
            df = session_data[sheet_name]
            if not df.empty:
                for _, row in df.iterrows():
                    fact_id = row.get('factId')
                    if fact_id and str(fact_id).strip():
                        facts.append({
                            "fact_id": str(fact_id).strip(),
                            "node_id": row.get('nodeId', ''),
                            "node_text": row.get('nodeText', ''),
                            "fact_group": row.get('factGroup', ''),
                            "assessment_id": row.get('assessmentId', ''),
                            "level": level,
                            "data_type": row.get('dataType', ''),
                        })
    
    return {"facts": facts, "count": len(facts)}


@app.get("/api/groups")
async def get_all_groups() -> Dict[str, Any]:
    """Get all fact groups from Level Facts sheets."""
    global session_data
    
    group_info = get_group_info(session_data)
    
    groups = []
    for group_name, info in group_info.items():
        groups.append({
            "group_name": group_name,
            "fact_count": info["fact_count"],
            "fact_ids": info["fact_ids"],
            "levels": info["levels"]
        })
    
    return {"groups": groups, "count": len(groups)}


@app.get("/api/modules")
async def get_all_modules() -> Dict[str, Any]:
    """Get all module codes."""
    global session_data
    
    modules = get_available_modules(session_data)
    return {"modules": modules, "count": len(modules)}


@app.get("/api/assessments")
async def get_all_assessments() -> Dict[str, Any]:
    """Get all assessment codes."""
    global session_data
    
    assessments = get_available_assessments(session_data)
    return {"assessments": assessments, "count": len(assessments)}


@app.get("/api/algorithms/suggest-id")
async def suggest_next_algorithm_id() -> Dict[str, str]:
    """Suggest the next algorithm ID."""
    global session_data
    
    df = session_data.get("Algorithms", pd.DataFrame())
    suggested = suggest_algorithm_id(df)
    
    return {"suggested_id": suggested}


@app.get("/api/empty-row/{sheet_name}")
async def get_empty_row_template(sheet_name: str) -> Dict[str, Any]:
    """Get an empty row with default values for a sheet."""
    if sheet_name not in AVAILABLE_SHEETS:
        raise HTTPException(status_code=404, detail=f"Unknown sheet: {sheet_name}")
    
    return get_empty_row(sheet_name)


# ============================================================================
# Health Check
# ============================================================================

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

