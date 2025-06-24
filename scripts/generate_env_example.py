#!/usr/bin/env python3
"""
Generate .env.example from Settings class

This script extracts environment variables from the field definitions
of the core.entities.Settings class and automatically generates
a .env.example file.
"""

from pathlib import Path
from typing import List, Tuple, Any

from bq_mcp.core.entities import Settings
from pydantic.fields import FieldInfo


def extract_env_variables_from_settings() -> List[Tuple[str, Any, str]]:
    """Extract environment variables from Settings class"""
    env_vars = []

    # Get model information of Settings class
    model_fields = Settings.model_fields

    for field_name, field_info in model_fields.items():
        # Generate environment variable name (snake_case -> UPPER_CASE)
        env_name = field_name.upper()

        # Get default value
        default_value = None
        if hasattr(field_info, "default") and field_info.default is not ...:
            # Handle PydanticUndefined appropriately
            if str(field_info.default) != "PydanticUndefined":
                default_value = field_info.default

        # Get field type information
        field_type = (
            field_info.annotation if hasattr(field_info, "annotation") else None
        )

        # Generate comment
        comment = generate_comment_for_field(
            field_name, default_value, field_type, field_info
        )

        env_vars.append((env_name, default_value, comment))

    return env_vars


def generate_comment_for_field(
    field_name: str, default_value: Any, field_type: Any, field_info: FieldInfo
) -> str:
    """Generate comment from field information"""

    # Get field description
    description = getattr(field_info, "description", "") if field_info else ""

    # Determine required/optional from type information
    # Check if field is Optional[T] type
    import typing

    is_optional = (
        default_value is not None and str(default_value) != "PydanticUndefined"
    ) or (
        hasattr(field_type, "__origin__")
        and field_type.__origin__ is typing.Union
        and type(None) in field_type.__args__
    )
    required_or_optional = "Optional" if is_optional else "Required"

    # Auto-generated comment
    comment_parts = [f"{required_or_optional}:"]

    if description:
        comment_parts.append(description)
    else:
        comment_parts.append(f"{field_name} configuration")

    if default_value is not None:
        comment_parts.append(f"Defaults to {default_value}.")

    return " ".join(comment_parts)


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
                for comment_line in comment.split("\n"):
                    lines.append(
                        f"# {comment_line}"
                        if not comment_line.startswith("#")
                        else comment_line
                    )

                # Environment variable setting
                if (
                    default_value is None
                    or default_value == ""
                    or str(default_value) == "PydanticUndefined"
                ):
                    lines.append(f"{var_name}=")
                elif isinstance(default_value, str):
                    lines.append(f"{var_name}={default_value}")
                else:
                    lines.append(f"{var_name}={default_value}")

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
