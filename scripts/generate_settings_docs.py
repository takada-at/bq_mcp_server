#!/usr/bin/env python3
"""
Generate documentation for MCP server settings from its command-line arguments.
This script parses the mcp_server.py file using AST and generates a markdown
documentation file for all settings and their corresponding environment variables.
"""

import ast
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))


class ArgParseVisitor(ast.NodeVisitor):
    """AST visitor to extract argument parser information"""

    def __init__(self):
        self.arguments: List[Dict[str, any]] = []
        self.in_parse_args = False
        self.in_add_argument = False
        self.current_arg = {}

    def visit_FunctionDef(self, node):
        """Visit function definitions to find parse_args"""
        if node.name == "parse_args":
            self.in_parse_args = True
            self.generic_visit(node)
            self.in_parse_args = False

    def visit_Call(self, node):
        """Visit function calls to find add_argument calls"""
        if self.in_parse_args:
            # Check if this is parser.add_argument call
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_argument"
            ):
                self.current_arg = {}
                self._extract_argument_info(node)
                if self.current_arg:
                    self.arguments.append(self.current_arg)
        self.generic_visit(node)

    def _extract_argument_info(self, node: ast.Call):
        """Extract information from add_argument call"""
        # Extract argument names
        arg_names = []
        for arg in node.args:
            if isinstance(arg, ast.Constant):
                arg_names.append(arg.value)

        if not arg_names:
            return

        self.current_arg["names"] = arg_names

        # Extract keyword arguments
        for keyword in node.keywords:
            if keyword.arg == "help":
                if isinstance(keyword.value, ast.Constant):
                    self.current_arg["help"] = keyword.value.value
            elif keyword.arg == "type":
                if isinstance(keyword.value, ast.Name):
                    self.current_arg["type"] = keyword.value.id
            elif keyword.arg == "default":
                if isinstance(keyword.value, ast.Constant):
                    self.current_arg["default"] = keyword.value.value
            elif keyword.arg == "required":
                if isinstance(keyword.value, ast.Constant):
                    self.current_arg["required"] = keyword.value.value


class EnvMappingVisitor(ast.NodeVisitor):
    """AST visitor to extract environment variable mappings"""

    def __init__(self):
        self.env_mappings: Dict[str, str] = {}
        self.in_apply_args = False

    def visit_FunctionDef(self, node):
        """Visit function definitions to find apply_args_to_env"""
        if node.name == "apply_args_to_env":
            self.in_apply_args = True
            self.generic_visit(node)
            self.in_apply_args = False

    def visit_If(self, node):
        """Visit if statements to find environment variable assignments"""
        if self.in_apply_args:
            # Check if this is checking args.something
            if (
                isinstance(node.test, ast.Attribute)
                and isinstance(node.test.value, ast.Name)
                and node.test.value.id == "args"
            ):
                arg_name = node.test.attr

                # Look for os.environ assignment in the body
                for stmt in node.body:
                    if (
                        isinstance(stmt, ast.Assign)
                        and len(stmt.targets) == 1
                        and isinstance(stmt.targets[0], ast.Subscript)
                        and isinstance(stmt.targets[0].value, ast.Attribute)
                        and stmt.targets[0].value.attr == "environ"
                    ):
                        # Extract environment variable name
                        if isinstance(stmt.targets[0].slice, ast.Constant):
                            env_var = stmt.targets[0].slice.value
                            self.env_mappings[arg_name] = env_var

        self.generic_visit(node)


def parse_mcp_server() -> Tuple[List[Dict[str, any]], Dict[str, str]]:
    """Parse mcp_server.py and extract argument information"""
    mcp_server_path = (
        Path(__file__).parent.parent / "bq_mcp_server" / "adapters" / "mcp_server.py"
    )

    with open(mcp_server_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    # Extract arguments
    arg_visitor = ArgParseVisitor()
    arg_visitor.visit(tree)

    # Extract environment variable mappings
    env_visitor = EnvMappingVisitor()
    env_visitor.visit(tree)

    return arg_visitor.arguments, env_visitor.env_mappings


def get_env_var_info() -> Dict[str, Dict[str, any]]:
    """Get environment variable information from entities.py Settings class"""
    entities_path = (
        Path(__file__).parent.parent / "bq_mcp_server" / "core" / "entities.py"
    )

    # Parse entities.py to find Settings class
    with open(entities_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    # Look for Settings class
    class SettingsVisitor(ast.NodeVisitor):
        def __init__(self):
            self.in_settings_class = False
            self.env_vars = {}

        def visit_ClassDef(self, node):
            if node.name == "Settings":
                self.in_settings_class = True
                self.generic_visit(node)
                self.in_settings_class = False
            else:
                self.generic_visit(node)

        def visit_AnnAssign(self, node):
            """Visit annotated assignments (field definitions)"""
            if self.in_settings_class and isinstance(node.target, ast.Name):
                field_name = node.target.id
                field_type = self._get_type_name(node.annotation)
                default_value = None
                description = ""

                # Get default value and description from Field() call
                if isinstance(node.value, ast.Call):
                    if (
                        isinstance(node.value.func, ast.Name)
                        and node.value.func.id == "Field"
                    ):
                        # Get positional arguments (first arg is usually default)
                        if node.value.args:
                            first_arg = node.value.args[0]
                            # Check if it's Ellipsis (...)
                            if (
                                isinstance(first_arg, ast.Constant)
                                and first_arg.value is ...
                            ):
                                default_value = "..."
                            else:
                                default_value = self._extract_value(first_arg)

                        # Get keyword arguments
                        for keyword in node.value.keywords:
                            if keyword.arg == "default":
                                default_value = self._extract_value(keyword.value)
                            elif keyword.arg == "description":
                                if isinstance(keyword.value, ast.Constant):
                                    description = keyword.value.value

                # Convert field name to environment variable name
                env_name = field_name.upper()
                self.env_vars[env_name] = {
                    "default": default_value,
                    "type": field_type,
                    "description": description,
                    "field_name": field_name,
                }

            self.generic_visit(node)

        def _get_type_name(self, annotation):
            """Extract type name from annotation"""
            if isinstance(annotation, ast.Name):
                return annotation.id
            elif isinstance(annotation, ast.Subscript):
                # Handle Optional[str], List[str], etc.
                if isinstance(annotation.value, ast.Name):
                    base = annotation.value.id
                    if isinstance(annotation.slice, ast.Name):
                        return f"{base}[{annotation.slice.id}]"
                    elif isinstance(annotation.slice, ast.Subscript):
                        # Handle nested types like List[Dict[str, Any]]
                        inner = self._get_type_name(annotation.slice)
                        return f"{base}[{inner}]"
                    return base
            return "Any"

        def _extract_value(self, node):
            """Extract value from AST node"""
            if isinstance(node, ast.Constant):
                return node.value
            elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
                # Handle negative numbers
                if isinstance(node.operand, ast.Constant):
                    return -node.operand.value
            elif isinstance(node, ast.BinOp):
                # Handle expressions like 1024 * 1024 * 1024
                if isinstance(node.op, ast.Mult):
                    left = self._extract_value(node.left)
                    right = self._extract_value(node.right)
                    if isinstance(left, (int, float)) and isinstance(
                        right, (int, float)
                    ):
                        return left * right
            elif isinstance(node, ast.Name) and node.id == "None":
                return None
            elif hasattr(node, "elts"):  # List or Tuple
                return []
            elif isinstance(node, ast.Attribute):
                # Handle Ellipsis (...) which is used for required fields
                if isinstance(node.value, ast.Constant) and node.attr == "__class__":
                    return "..."
            elif isinstance(node, ast.Constant) and node.value == ...:
                return "..."
            return None

    visitor = SettingsVisitor()
    visitor.visit(tree)

    return visitor.env_vars


def arg_to_env_name(arg_name: str) -> str:
    """Convert argument name to environment variable name"""
    # Remove leading dashes and convert to uppercase with underscores
    clean_name = arg_name.lstrip("-").replace("-", "_").upper()
    return clean_name


def generate_documentation(
    arguments: List[Dict[str, any]],
    env_mappings: Dict[str, str],
    env_var_info: Dict[str, Dict[str, any]],
) -> str:
    """Generate markdown documentation"""
    lines = [
        "# BQ MCP Server Settings",
        "",
        "This document describes all available settings for the BQ MCP server.",
        "Settings can be configured via command-line arguments or environment variables.",
        "",
        "## Priority",
        "",
        "When both command-line arguments and environment variables are set, command-line arguments take priority.",
        "",
        "## Settings",
        "",
    ]

    for arg in arguments:
        names = arg.get("names", [])
        if not names:
            continue

        # Get the main argument name (usually the long form)
        main_name = next((n for n in names if n.startswith("--")), names[0])
        arg_key = main_name.lstrip("-").replace("-", "_")

        # Create section header
        lines.append(f"### {main_name}")
        lines.append("")

        # Add description
        if "help" in arg:
            lines.append(f"**Description**: {arg['help']}")
            lines.append("")

        # Add type information
        if "type" in arg:
            lines.append(f"**Type**: `{arg['type']}`")
            lines.append("")

        # Add default value
        if "default" in arg:
            lines.append(f"**Default**: `{arg['default']}`")
            lines.append("")

        # Add environment variable
        env_var = env_mappings.get(arg_key)
        if not env_var:
            # Try to find it based on naming convention
            env_var = arg_to_env_name(main_name)

        if env_var in env_var_info:
            env_info = env_var_info[env_var]
            lines.append(f"**Environment Variable**: `{env_var}`")

            # Format default value for display
            env_default = env_info.get("default")
            if env_default == "...":
                lines.append("  - Default: Required (no default value)")
            elif env_default is not None:
                if (
                    isinstance(env_default, int)
                    and env_var == "MAX_SCAN_BYTES"
                    and env_default >= 1024 * 1024 * 1024
                ):
                    lines.append(
                        f"  - Default: `{env_default // (1024 * 1024 * 1024)}GB ({env_default:,} bytes)`"
                    )
                elif isinstance(env_default, int) and env_var == "CACHE_TTL_SECONDS":
                    lines.append(f"  - Default: `{env_default} seconds`")
                elif (
                    isinstance(env_default, int) and env_var == "QUERY_TIMEOUT_SECONDS"
                ):
                    lines.append(f"  - Default: `{env_default} seconds`")
                elif isinstance(env_default, list) and len(env_default) == 0:
                    lines.append("  - Default: `[]`")
                else:
                    lines.append(f"  - Default: `{env_default}`")
            lines.append("")
        elif env_var:
            lines.append(f"**Environment Variable**: `{env_var}`")
            lines.append("")

        # Add command-line usage examples
        lines.append("**Usage**:")
        lines.append("```bash")
        if "type" in arg:
            if arg["type"] == "int":
                lines.append(
                    f"python -m bq_mcp_server.adapters.mcp_server {main_name} 3600"
                )
            else:
                lines.append(
                    f'python -m bq_mcp_server.adapters.mcp_server {main_name} "value"'
                )
        else:
            lines.append(
                f'python -m bq_mcp_server.adapters.mcp_server {main_name} "value"'
            )
        lines.append("```")
        lines.append("")

        # Add all name variants
        if len(names) > 1:
            lines.append("**Aliases**:")
            for name in names:
                if name != main_name:
                    lines.append(f"- `{name}`")
            lines.append("")

    # Add environment variables section
    lines.extend(
        [
            "## Environment Variables Reference",
            "",
            "The following environment variables are used by the BQ MCP server:",
            "",
            "| Variable | Description | Type | Default |",
            "| --- | --- | --- | --- |",
        ]
    )

    # Get all unique environment variables
    all_env_vars = set(env_mappings.values())
    all_env_vars.update(env_var_info.keys())

    for env_var in sorted(all_env_vars):
        info = env_var_info.get(env_var, {})
        default = info.get("default", "None")
        var_type = info.get("type", "string")

        # Format type information
        if var_type:
            # Simplify type names for display
            if var_type.startswith("Optional["):
                var_type = var_type[9:-1]  # Remove "Optional[" and "]"
            elif var_type.startswith("List["):
                inner_type = var_type[5:-1]  # Get inner type
                var_type = f"list[{inner_type}]"

        # Format default values
        if default == "...":
            default = "Required"
        elif isinstance(default, list) and len(default) == 0:
            default = "[]"
        elif isinstance(default, int):
            # Special handling for specific variables
            if env_var == "API_PORT":
                default = f"{default}"
            elif env_var == "CACHE_TTL_SECONDS":
                default = f"{default} seconds"
            elif env_var == "QUERY_TIMEOUT_SECONDS":
                default = f"{default} seconds"
            elif env_var == "MAX_SCAN_BYTES" and default >= 1024 * 1024 * 1024:
                # Format large byte values
                default = f"{default // (1024 * 1024 * 1024)}GB ({default:,} bytes)"
            elif default >= 1024 * 1024:
                default = f"{default // (1024 * 1024)}MB ({default:,} bytes)"
            elif default >= 1024:
                default = f"{default // 1024}KB ({default:,} bytes)"
        elif default == "computed":
            default = "Computed at runtime"
        elif default is None:
            default = "None"

        # Try to find description from corresponding argument
        description = ""
        for arg_key, mapped_env in env_mappings.items():
            if mapped_env == env_var:
                # Find the argument with this key
                for arg in arguments:
                    names = arg.get("names", [])
                    main_name = next((n for n in names if n.startswith("--")), names[0])
                    if main_name.lstrip("-").replace("-", "_") == arg_key:
                        description = arg.get("help", "")
                        break

        # Use description from entities.py if not found from arguments
        if not description and "description" in info:
            description = info["description"]

        lines.append(f"| `{env_var}` | {description} | {var_type} | `{default}` |")

    lines.append("")

    return "\n".join(lines)


def main():
    """Main function"""
    print("Parsing MCP server arguments...")
    arguments, env_mappings = parse_mcp_server()

    print(f"Found {len(arguments)} arguments")
    print(f"Found {len(env_mappings)} environment variable mappings")

    print("Getting environment variable information...")
    env_var_info = get_env_var_info()
    print(f"Found {len(env_var_info)} environment variables in config")

    print("Generating documentation...")
    documentation = generate_documentation(arguments, env_mappings, env_var_info)

    # Save to docs/settings.md
    docs_dir = Path(__file__).parent.parent / "docs"
    docs_dir.mkdir(exist_ok=True)

    output_path = docs_dir / "settings.md"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(documentation)

    print(f"Documentation generated successfully: {output_path}")


if __name__ == "__main__":
    main()
