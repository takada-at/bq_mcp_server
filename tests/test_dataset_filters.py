"""Test dataset filtering functionality"""
import pytest
from bq_meta_api.repositories.config import should_include_dataset


class TestDatasetFiltering:
    """Test cases for dataset filtering functionality"""

    def test_should_include_dataset_empty_filters(self):
        """Test that empty filters include all datasets"""
        assert should_include_dataset("project1", "dataset1", []) is True
        assert should_include_dataset("project2", "dataset2", []) is True

    def test_should_include_dataset_wildcard_project(self):
        """Test wildcard project filtering"""
        filters = ["pj1.*", "pj2.dataset1"]
        
        # pj1.* should match any dataset in pj1
        assert should_include_dataset("pj1", "dataset1", filters) is True
        assert should_include_dataset("pj1", "dataset2", filters) is True
        assert should_include_dataset("pj1", "anydataset", filters) is True
        
        # pj2.dataset1 should match only dataset1 in pj2
        assert should_include_dataset("pj2", "dataset1", filters) is True
        assert should_include_dataset("pj2", "dataset2", filters) is False
        
        # pj3 should not match anything
        assert should_include_dataset("pj3", "dataset1", filters) is False

    def test_should_include_dataset_specific_dataset(self):
        """Test specific dataset filtering"""
        filters = ["project1.dataset1", "project2.dataset2"]
        
        assert should_include_dataset("project1", "dataset1", filters) is True
        assert should_include_dataset("project1", "dataset2", filters) is False
        assert should_include_dataset("project2", "dataset1", filters) is False
        assert should_include_dataset("project2", "dataset2", filters) is True
        assert should_include_dataset("project3", "dataset1", filters) is False

    def test_should_include_dataset_complex_patterns(self):
        """Test complex pattern matching"""
        filters = ["prod-*.*", "test-project.test_*", "analytics.daily_*"]
        
        # prod-* should match any project starting with prod-
        assert should_include_dataset("prod-1", "dataset1", filters) is True
        assert should_include_dataset("prod-analytics", "users", filters) is True
        assert should_include_dataset("staging-1", "dataset1", filters) is False
        
        # test-project.test_* should match datasets starting with test_ in test-project
        assert should_include_dataset("test-project", "test_users", filters) is True
        assert should_include_dataset("test-project", "test_orders", filters) is True
        assert should_include_dataset("test-project", "users", filters) is False
        
        # analytics.daily_* should match datasets starting with daily_ in analytics
        assert should_include_dataset("analytics", "daily_reports", filters) is True
        assert should_include_dataset("analytics", "weekly_reports", filters) is False

    def test_should_include_dataset_case_sensitivity(self):
        """Test case sensitivity in pattern matching"""
        filters = ["Project1.Dataset1"]
        
        # Should be case sensitive
        assert should_include_dataset("Project1", "Dataset1", filters) is True
        assert should_include_dataset("project1", "dataset1", filters) is False
        assert should_include_dataset("PROJECT1", "DATASET1", filters) is False

    def test_should_include_dataset_example_from_issue(self):
        """Test the exact example from the GitHub issue"""
        filters = ["pj1.*", "pj2.dataset1", "pj2.dataset2"]
        
        # pj1 - all datasets should be included
        assert should_include_dataset("pj1", "any_dataset", filters) is True
        assert should_include_dataset("pj1", "another_dataset", filters) is True
        
        # pj2 - only dataset1 and dataset2 should be included
        assert should_include_dataset("pj2", "dataset1", filters) is True
        assert should_include_dataset("pj2", "dataset2", filters) is True
        assert should_include_dataset("pj2", "dataset3", filters) is False
        
        # pj3 - nothing should be included
        assert should_include_dataset("pj3", "dataset1", filters) is False