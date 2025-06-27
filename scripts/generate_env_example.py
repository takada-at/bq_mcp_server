#!/usr/bin/env python3
"""
Generate .env.example from Settings class

This script extracts environment variables from the field definitions
of the core.entities.Settings class and automatically generates
a .env.example file.
"""

from pathlib import Path
from typing import Any, List, Tuple

from pydantic.fields import FieldInfo

from bq_mcp.core.entities import Settings


def _extract_default_value(field_info: FieldInfo) -> Any:
    """Extract default value from field info"""
    if hasattr(field_info, "default") and field_info.default is not ...:
        if str(field_info.default) != "PydanticUndefined":
            return field_info.default
    elif (
        hasattr(field_info, "default_factory")
        and field_info.default_factory is not None
    ):
        try:
            default_value = field_info.default_factory()
            # Convert empty containers to None for cleaner output
            if default_value in ([], {}, set()):
                return None
            return default_value
        except Exception:
            return None
    return None


def extract_env_variables_from_settings() -> List[Tuple[str, Any, str]]:
    """Extract environment variables from Settings class"""
    env_vars = []
    model_fields = Settings.model_fields

    for field_name, field_info in model_fields.items():
        # Generate environment variable name (snake_case -> UPPER_CASE)
        env_name = field_name.upper()

        # Get default value
        default_value = _extract_default_value(field_info)

        # Get field type information
        field_type = getattr(field_info, "annotation", None)

        # Generate comment
        comment = generate_comment_for_field(
            field_name, default_value, field_type, field_info
        )

        env_vars.append((env_name, default_value, comment))

    return env_vars


def _is_optional_field(
    field_type: Any, default_value: Any, field_info: FieldInfo
) -> bool:
    """Check if field is optional based on type and default value"""
    import typing

    has_default = (
        default_value is not None and str(default_value) != "PydanticUndefined"
    ) or (
        hasattr(field_info, "default_factory")
        and field_info.default_factory is not None
    )

    is_union_with_none = (
        hasattr(field_type, "__origin__")
        and field_type.__origin__ is typing.Union
        and type(None) in field_type.__args__
    )

    return has_default or is_union_with_none


def generate_comment_for_field(
    field_name: str, default_value: Any, field_type: Any, field_info: FieldInfo
) -> str:
    """Generate comment from field information"""
    # Get field description
    description = getattr(field_info, "description", "") if field_info else ""

    # Determine if field is optional
    is_optional = _is_optional_field(field_type, default_value, field_info)
    required_or_optional = "Optional" if is_optional else "Required"

    # Build comment parts
    comment_parts = [f"{required_or_optional}:"]

    if description:
        comment_parts.append(description)
    else:
        comment_parts.append(f"{field_name} configuration")

    if default_value is not None:
        comment_parts.append(f"Defaults to {default_value}.")

    return " ".join(comment_parts)


def _format_env_value(default_value: Any) -> str:
    """Format environment variable value for output"""
    if (
        default_value is None
        or default_value == ""
        or str(default_value) == "PydanticUndefined"
    ):
        return ""
    return str(default_value)


def _add_comment_lines(lines: List[str], comment: str) -> None:
    """Add comment lines to output"""
    for comment_line in comment.split("\n"):
        if not comment_line.startswith("#"):
            lines.append(f"# {comment_line}")
        else:
            lines.append(comment_line)


def generate_env_example(env_vars: List[Tuple[str, Any, str]], output_path: Path):
    """Generate .env.example from environment variables"""
    # Group by category
    categories = {
        "GCP Settings": [
            "GCP_SERVICE_ACCOUNT_KEY_PATH",
            "PROJECT_IDS",
            "DATASET_FILTERS",
        ],
        "Cache Settings": ["CACHE_TTL_SECONDS", "CACHE_FILE_BASE_DIR"],
        "API Server Settings": ["API_HOST", "API_PORT"],
        "Query Execution Settings": [
            "MAX_SCAN_BYTES",
            "DEFAULT_QUERY_LIMIT",
            "QUERY_TIMEOUT_SECONDS",
        ],
    }

    # Convert environment variables to dictionary
    env_dict = {name: (default, comment) for name, default, comment in env_vars}

    lines = []
    lines.append("# .env.example - BigQuery Metadata API Server Configuration")
    lines.append("")

    for category, var_names in categories.items():
        lines.append(f"# --- {category} ---")

        for var_name in var_names:
            if var_name in env_dict:
                default_value, comment = env_dict[var_name]

                # Add comment
                _add_comment_lines(lines, comment)

                # Environment variable setting
                value = _format_env_value(default_value)
                lines.append(f"{var_name}={value}")
                lines.append("")

    # Write to file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    """Main function"""
    # Get project root
    project_root = Path(__file__).parent.parent

    env_example_file = project_root / ".env.example"

    try:
        # Extract environment variables from Settings class
        env_vars = extract_env_variables_from_settings()

        if not env_vars:
            print("Warning: No environment variables found in Settings class")
            return 1

        # Generate .env.example
        generate_env_example(env_vars, env_example_file)

        print(f"Successfully generated {env_example_file}")
        print(f"Found {len(env_vars)} environment variables from Settings class:")
        for var_name, default_value, _ in env_vars:
            print(f"  - {var_name} (default: {default_value})")

        return 0

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
