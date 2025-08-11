"""Tests for file_exporter module"""

import csv
import json
import os
import tempfile
from datetime import datetime

import pytest

from bq_mcp_server.core import file_exporter


class TestValidateOutputPath:
    """Tests for validate_output_path function"""

    def test_accepts_valid_absolute_path(self):
        """Test that valid absolute paths are accepted"""
        with tempfile.TemporaryDirectory() as tmpdir:
            valid_path = os.path.join(tmpdir, "output.csv")
            result = file_exporter.validate_output_path(valid_path)
            assert result == os.path.abspath(valid_path)

    def test_converts_relative_path_to_absolute(self):
        """Test that relative paths are converted to absolute"""
        relative_path = "./output.csv"
        result = file_exporter.validate_output_path(relative_path)
        assert os.path.isabs(result)

    def test_rejects_path_traversal_attempts(self):
        """Test that path traversal attempts are rejected"""
        dangerous_paths = [
            "../../../etc/passwd",
            "/tmp/../etc/passwd",
            "/tmp/./../../etc/passwd",
        ]
        for path in dangerous_paths:
            with pytest.raises(ValueError, match="Path traversal"):
                file_exporter.validate_output_path(path)

    def test_rejects_write_to_system_directories(self):
        """Test that writing to system directories is rejected"""
        system_paths = [
            "/etc/output.csv",
            "/usr/bin/output.csv",
            "/System/output.csv",
        ]
        for path in system_paths:
            with pytest.raises(ValueError, match="system directory"):
                file_exporter.validate_output_path(path)

    def test_creates_parent_directory_if_not_exists(self):
        """Test that parent directory is created if it doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = os.path.join(tmpdir, "new_dir", "output.csv")
            result = file_exporter.validate_output_path(nested_path)
            assert os.path.exists(os.path.dirname(result))


class TestExportToCsv:
    """Tests for export_to_csv function"""

    @pytest.mark.asyncio
    async def test_exports_data_with_header(self):
        """Test exporting data to CSV with header"""
        rows = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            file_size = await file_exporter.export_to_csv(
                rows, output_path, include_header=True
            )

            # Verify file exists and has content
            assert os.path.exists(output_path)
            assert file_size > 0

            # Verify CSV content
            with open(output_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                read_rows = list(reader)
                assert len(read_rows) == 2
                assert read_rows[0]["name"] == "Alice"
                assert read_rows[1]["age"] == "25"  # CSV stores as strings
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_exports_data_without_header(self):
        """Test exporting data to CSV without header"""
        rows = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            file_size = await file_exporter.export_to_csv(
                rows, output_path, include_header=False
            )

            # Verify file exists
            assert os.path.exists(output_path)
            assert file_size > 0

            # Verify no header in CSV
            with open(output_path, "r", newline="") as f:
                lines = f.readlines()
                assert len(lines) == 2
                assert "id,name" not in lines[0]
                assert "1,Alice" in lines[0]
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_handles_special_characters_in_csv(self):
        """Test handling of special characters in CSV export"""
        rows = [
            {"text": 'Hello, "World"', "value": "Line\nBreak"},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            await file_exporter.export_to_csv(rows, output_path, include_header=True)

            with open(output_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                read_rows = list(reader)
                assert read_rows[0]["text"] == 'Hello, "World"'
                assert read_rows[0]["value"] == "Line\nBreak"
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_handles_datetime_types(self):
        """Test handling of datetime types in CSV export"""
        test_datetime = datetime(2024, 1, 15, 10, 30, 45)
        rows = [
            {"timestamp": test_datetime, "name": "Event"},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            await file_exporter.export_to_csv(rows, output_path, include_header=True)

            with open(output_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                read_rows = list(reader)
                # Should be ISO format
                assert "2024-01-15" in read_rows[0]["timestamp"]
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_handles_none_values(self):
        """Test handling of None values in CSV export"""
        rows = [
            {"id": 1, "name": None, "value": "test"},
            {"id": 2, "name": "Bob", "value": None},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            await file_exporter.export_to_csv(rows, output_path, include_header=True)

            with open(output_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                read_rows = list(reader)
                assert read_rows[0]["name"] == ""  # None becomes empty string
                assert read_rows[1]["value"] == ""
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_returns_correct_file_size(self):
        """Test that the function returns correct file size"""
        rows = [{"id": i, "name": f"Name{i}"} for i in range(10)]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            file_size = await file_exporter.export_to_csv(
                rows, output_path, include_header=True
            )
            actual_size = os.path.getsize(output_path)
            assert file_size == actual_size
        finally:
            os.unlink(output_path)


class TestExportToJsonl:
    """Tests for export_to_jsonl function"""

    @pytest.mark.asyncio
    async def test_exports_data_to_jsonl(self):
        """Test exporting data to JSONL format"""
        rows = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            file_size = await file_exporter.export_to_jsonl(rows, output_path)

            # Verify file exists and has content
            assert os.path.exists(output_path)
            assert file_size > 0

            # Verify JSONL content
            with open(output_path, "r") as f:
                lines = f.readlines()
                assert len(lines) == 2

                row1 = json.loads(lines[0])
                assert row1["name"] == "Alice"
                assert row1["active"] is True

                row2 = json.loads(lines[1])
                assert row2["name"] == "Bob"
                assert row2["active"] is False
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_handles_datetime_in_jsonl(self):
        """Test handling of datetime types in JSONL export"""
        test_datetime = datetime(2024, 1, 15, 10, 30, 45)
        rows = [
            {"timestamp": test_datetime, "event": "login"},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            await file_exporter.export_to_jsonl(rows, output_path)

            with open(output_path, "r") as f:
                line = f.readline()
                data = json.loads(line)
                # Should be ISO format string
                assert data["timestamp"] == "2024-01-15T10:30:45"
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_handles_none_values_in_jsonl(self):
        """Test handling of None values in JSONL export"""
        rows = [
            {"id": 1, "name": None, "value": 100},
            {"id": 2, "name": "Test", "value": None},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            await file_exporter.export_to_jsonl(rows, output_path)

            with open(output_path, "r") as f:
                lines = f.readlines()
                row1 = json.loads(lines[0])
                row2 = json.loads(lines[1])

                assert row1["name"] is None
                assert row2["value"] is None
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_handles_nested_structures(self):
        """Test handling of nested structures in JSONL export"""
        rows = [
            {
                "id": 1,
                "metadata": {"tags": ["python", "bigquery"], "version": 1.0},
                "items": [1, 2, 3],
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            await file_exporter.export_to_jsonl(rows, output_path)

            with open(output_path, "r") as f:
                line = f.readline()
                data = json.loads(line)
                assert data["metadata"]["tags"] == ["python", "bigquery"]
                assert data["items"] == [1, 2, 3]
        finally:
            os.unlink(output_path)

    @pytest.mark.asyncio
    async def test_returns_correct_file_size_jsonl(self):
        """Test that the function returns correct file size for JSONL"""
        rows = [{"id": i, "data": f"Value{i}"} for i in range(10)]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            file_size = await file_exporter.export_to_jsonl(rows, output_path)
            actual_size = os.path.getsize(output_path)
            assert file_size == actual_size
        finally:
            os.unlink(output_path)
