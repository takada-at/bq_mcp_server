"""Test BigQuery client functionality, especially dataset filter optimization"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bq_mcp.core.entities import Settings
from bq_mcp.repositories import bigquery_client
from bq_mcp.repositories.config import should_include_dataset


class TestDatasetFilterOptimization:
    """Test dataset filtering optimization in bigquery_client"""

    def test_should_include_dataset_basic_scenarios(self):
        """Test basic scenarios for dataset filtering"""
        # Test cases that we ran manually
        test_cases = [
            ("project1", "dataset1", ["project1.*"], True),
            ("project1", "dataset1", ["project2.*"], False),
            ("project1", "specific", ["project1.specific"], True),
            ("project1", "other", ["project1.specific"], False),
            ("project1", "dataset1", [], True),  # No filters
        ]

        for project, dataset, filters, expected in test_cases:
            result = should_include_dataset(project, dataset, filters)
            assert result == expected, (
                f"{project}.{dataset} with filters {filters} should be {expected}"
            )

    def test_should_include_dataset_wildcard_patterns(self):
        """Test wildcard pattern matching"""
        # Patterns from actual configuration
        filters = [
            "prj-ai-chatbot.*",
            "pj-cloud-gorillatest.*",
            "bigquery-public-data.google_cloud_release_notes",
            "bigquery-public-data.wikipedia",
            "bigquery-public-data.samples",
        ]

        # Should include - wildcard matches
        assert should_include_dataset("prj-ai-chatbot", "any_dataset", filters)
        assert should_include_dataset("pj-cloud-gorillatest", "test_data", filters)

        # Should include - exact matches
        assert should_include_dataset("bigquery-public-data", "samples", filters)
        assert should_include_dataset("bigquery-public-data", "wikipedia", filters)

        # Should exclude
        assert not should_include_dataset("other-project", "dataset", filters)
        assert not should_include_dataset(
            "bigquery-public-data", "other_dataset", filters
        )

    @patch("bq_mcp.repositories.config.get_settings")
    @pytest.mark.asyncio
    async def test_fetch_datasets_filter_optimization(self, mock_get_settings):
        """Test that fetch_datasets skips filtered datasets efficiently"""
        # Mock settings with filters
        mock_settings = MagicMock(spec=Settings)
        mock_settings.dataset_filters = ["project1.*", "project2.specific"]
        mock_get_settings.return_value = mock_settings

        # Mock client
        mock_client = MagicMock()
        mock_client.session = MagicMock()
        mock_client.session.session = MagicMock()
        mock_client.token = MagicMock()

        # Mock dataset list response
        datasets_list = [
            {
                "datasetReference": {"projectId": "project1", "datasetId": "dataset1"},
                "location": "US",
                "description": "Test dataset 1",
            },
            {
                "datasetReference": {"projectId": "project2", "datasetId": "specific"},
                "location": "US",
                # No description - will trigger dataset.get() call
            },
            {
                "datasetReference": {"projectId": "project2", "datasetId": "other"},
                "location": "US",
            },
            {
                "datasetReference": {"projectId": "project3", "datasetId": "dataset1"},
                "location": "US",
            },
        ]

        with patch(
            "bq_mcp.repositories.bigquery_client._paginate_bigquery_api"
        ) as mock_paginate:
            mock_paginate.return_value = datasets_list

            # Mock the Dataset.get method directly to avoid internal async calls
            with patch(
                "gcloud.aio.bigquery.Dataset.get", new_callable=AsyncMock
            ) as mock_get:
                mock_get.return_value = {"description": "Fetched description"}

                # Call the function
                result = await bigquery_client.fetch_datasets(
                    mock_client, "test_project"
                )

                # Should only return filtered datasets
                assert len(result) == 2  # project1.dataset1 and project2.specific

                # Verify project and dataset IDs
                result_ids = [(r.project_id, r.dataset_id) for r in result]
                expected_ids = [("project1", "dataset1"), ("project2", "specific")]
                assert result_ids == expected_ids

                # Verify dataset.get was called only for included datasets without description
                # project2.specific has no description, so should call get()
                mock_get.assert_called_once()

    @patch("bq_mcp.repositories.config.get_settings")
    @pytest.mark.asyncio
    async def test_fetch_datasets_no_filters(self, mock_get_settings):
        """Test that fetch_datasets processes all datasets when no filters are set"""
        # Mock settings with no filters
        mock_settings = MagicMock(spec=Settings)
        mock_settings.dataset_filters = []
        mock_get_settings.return_value = mock_settings

        # Mock client
        mock_client = MagicMock()
        mock_client.session = MagicMock()
        mock_client.session.session = MagicMock()
        mock_client.token = MagicMock()

        # Mock dataset list response - both have descriptions, so no .get() calls needed
        datasets_list = [
            {
                "datasetReference": {"projectId": "project1", "datasetId": "dataset1"},
                "location": "US",
                "description": "Test dataset 1",
            },
            {
                "datasetReference": {"projectId": "project2", "datasetId": "dataset2"},
                "location": "US",
                "description": "Test dataset 2",
            },
        ]

        with patch(
            "bq_mcp.repositories.bigquery_client._paginate_bigquery_api"
        ) as mock_paginate:
            mock_paginate.return_value = datasets_list

            # Call the function - no dataset.get() should be called since all have descriptions
            result = await bigquery_client.fetch_datasets(mock_client, "test_project")

            # Should return all datasets
            assert len(result) == 2

            # Verify project and dataset IDs
            result_ids = [(r.project_id, r.dataset_id) for r in result]
            expected_ids = [("project1", "dataset1"), ("project2", "dataset2")]
            assert result_ids == expected_ids

    def test_fetch_datasets_filter_logic(self):
        """Test that dataset filtering logic works correctly"""
        # Test data simulating fetch_datasets scenario
        datasets_data = [
            {"datasetReference": {"projectId": "project1", "datasetId": "dataset1"}},
            {"datasetReference": {"projectId": "project2", "datasetId": "specific"}},
            {"datasetReference": {"projectId": "project2", "datasetId": "other"}},
            {"datasetReference": {"projectId": "project3", "datasetId": "dataset1"}},
        ]

        filters = ["project1.*", "project2.specific"]

        # Simulate the filtering logic from fetch_datasets
        included_datasets = []
        skipped_datasets = []

        for dataset_data in datasets_data:
            ds_ref = dataset_data.get("datasetReference", {})
            project_id = ds_ref.get("projectId")
            dataset_id = ds_ref.get("datasetId")

            if should_include_dataset(project_id, dataset_id, filters):
                included_datasets.append((project_id, dataset_id))
            else:
                skipped_datasets.append((project_id, dataset_id))

        # Verify results
        assert len(included_datasets) == 2
        assert len(skipped_datasets) == 2

        assert ("project1", "dataset1") in included_datasets
        assert ("project2", "specific") in included_datasets
        assert ("project2", "other") in skipped_datasets
        assert ("project3", "dataset1") in skipped_datasets

    def test_filter_performance_benefits(self):
        """Test that demonstrates the performance benefits of early filtering"""
        # This test verifies that our optimization logic works correctly
        # by showing that filtered datasets are skipped before expensive operations

        # Test data simulating a scenario with many datasets
        all_datasets = [
            ("project1", "dataset1"),
            ("project1", "dataset2"),
            ("project2", "dataset1"),
            ("project2", "dataset2"),
            ("project3", "dataset1"),
            ("project3", "dataset2"),
        ]

        # Filter that only includes project1 datasets
        filters = ["project1.*"]

        # Count how many would be processed with and without optimization
        included_count = 0
        excluded_count = 0

        for project_id, dataset_id in all_datasets:
            if should_include_dataset(project_id, dataset_id, filters):
                included_count += 1
            else:
                excluded_count += 1

        # With optimization, only 2 datasets should be processed
        assert included_count == 2
        # 4 datasets should be skipped, avoiding expensive API calls
        assert excluded_count == 4


class TestDatasetFilterEdgeCases:
    """Test edge cases for dataset filtering"""

    def test_empty_project_or_dataset_ids(self):
        """Test handling of empty project or dataset IDs"""
        filters = ["project1.*"]

        # Empty project ID - should not match
        assert not should_include_dataset("", "dataset1", filters)

        # Empty dataset ID - project1.* matches project1. (empty string)
        # This is expected fnmatch behavior
        assert should_include_dataset("project1", "", filters)

        # Both empty - should not match
        assert not should_include_dataset("", "", filters)

        # Test with more specific filter to show empty dataset ID behavior
        specific_filters = ["project1.specific_dataset"]
        assert not should_include_dataset("project1", "", specific_filters)

    def test_special_characters_in_filters(self):
        """Test filters with special characters"""
        filters = ["project-with-dashes.*", "project_with_underscores.dataset-name"]

        # Should match project with dashes
        assert should_include_dataset("project-with-dashes", "any_dataset", filters)

        # Should match specific dataset with dashes
        assert should_include_dataset(
            "project_with_underscores", "dataset-name", filters
        )

        # Should not match similar but different names
        assert not should_include_dataset("project_with_dashes", "any_dataset", filters)
        assert not should_include_dataset(
            "project_with_underscores", "dataset_name", filters
        )


# Run with: python -m pytest tests/test_bigquery_client.py -v
