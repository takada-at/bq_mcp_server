"""Export query results to various file formats"""

import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, List


def validate_output_path(output_path: str) -> str:
    """
    Validate and normalize output path to prevent path traversal attacks.

    Args:
        output_path: The path where the file will be saved

    Returns:
        Normalized absolute path

    Raises:
        ValueError: If path is invalid or potentially dangerous
    """
    # First normalize the path to resolve .. and .
    normalized = os.path.normpath(output_path)

    # Convert to absolute path
    abs_path = os.path.abspath(normalized)

    # Re-normalize after converting to absolute
    normalized = os.path.normpath(abs_path)

    # Check for path traversal attempts by looking at the original path
    if "../" in output_path or "..\\" in output_path:
        raise ValueError("Path traversal detected in output path")

    # Check if trying to write to system directories
    system_dirs = [
        "/etc",
        "/usr",
        "/bin",
        "/sbin",
        "/System",
        "/Windows",
        "/Program Files",
    ]
    for sys_dir in system_dirs:
        if normalized.startswith(sys_dir):
            raise ValueError(f"Cannot write to system directory: {sys_dir}")

    # Create parent directory if it doesn't exist
    parent_dir = os.path.dirname(normalized)
    if parent_dir and not os.path.exists(parent_dir):
        try:
            os.makedirs(parent_dir, exist_ok=True)
        except PermissionError:
            raise ValueError(f"Cannot create directory: {parent_dir}")

    return normalized


def _serialize_value(value: Any) -> Any:
    """
    Serialize a value for file export.

    Args:
        value: The value to serialize

    Returns:
        Serialized value suitable for CSV/JSON export
    """
    if isinstance(value, datetime):
        return value.isoformat()
    elif value is None:
        return None  # Will be handled differently by CSV vs JSON
    else:
        return value


async def export_to_csv(
    rows: List[Dict[str, Any]], output_path: str, include_header: bool = True
) -> int:
    """
    Export rows to CSV file.

    Args:
        rows: List of dictionaries to export
        output_path: Path where to save the CSV file
        include_header: Whether to include header row

    Returns:
        Size of the created file in bytes

    Raises:
        ValueError: If path validation fails
        IOError: If file writing fails
    """
    # Validate path
    validated_path = validate_output_path(output_path)

    # Handle empty rows
    if not rows:
        with open(validated_path, "w", newline="", encoding="utf-8") as f:
            pass  # Create empty file
        return os.path.getsize(validated_path)

    # Get all unique field names from all rows
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    # Write CSV
    with open(validated_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if include_header:
            writer.writeheader()

        for row in rows:
            # Serialize values
            serialized_row = {}
            for key, value in row.items():
                serialized_value = _serialize_value(value)
                # CSV converts None to empty string
                if serialized_value is None:
                    serialized_row[key] = ""
                else:
                    serialized_row[key] = serialized_value
            writer.writerow(serialized_row)

    return os.path.getsize(validated_path)


async def export_to_jsonl(rows: List[Dict[str, Any]], output_path: str) -> int:
    """
    Export rows to JSONL (JSON Lines) file.

    Args:
        rows: List of dictionaries to export
        output_path: Path where to save the JSONL file

    Returns:
        Size of the created file in bytes

    Raises:
        ValueError: If path validation fails
        IOError: If file writing fails
    """
    # Validate path
    validated_path = validate_output_path(output_path)

    # Write JSONL
    with open(validated_path, "w", encoding="utf-8") as f:
        for row in rows:
            # Serialize datetime values
            serialized_row = {}
            for key, value in row.items():
                serialized_row[key] = _serialize_value(value)

            # Write as JSON line
            json_line = json.dumps(serialized_row, ensure_ascii=False)
            f.write(json_line + "\n")

    return os.path.getsize(validated_path)
