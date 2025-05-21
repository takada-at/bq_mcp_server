import unittest
from unittest.mock import MagicMock
from typing import List

from bq_meta_api.application.services import ListDatasetsService
# In previous steps, DatasetMetadata was aliased as Dataset in services and interfaces.
# We should align with that or use DatasetMetadata directly. For clarity, using DatasetMetadata.
from bq_meta_api.domain.entities import DatasetMetadata
from bq_meta_api.domain.repositories import IBigQueryRepository

class TestListDatasetsService(unittest.TestCase):

    def setUp(self):
        # Create a mock for the IBigQueryRepository
        self.mock_bigquery_repo = MagicMock(spec=IBigQueryRepository)
        
        # Instantiate the service with the mocked repository
        self.list_datasets_service = ListDatasetsService(bigquery_repository=self.mock_bigquery_repo)

    def test_execute_returns_list_of_datasets(self):
        # Arrange: Configure the mock repository to return sample data
        project_id = "test_project"
        # Adjusting attributes to match DatasetMetadata
        expected_datasets = [
            DatasetMetadata(project_id=project_id, dataset_id="dataset1", description="Test dataset 1", location="US"),
            DatasetMetadata(project_id=project_id, dataset_id="dataset2", description="Test dataset 2", location="EU")
        ]
        self.mock_bigquery_repo.list_datasets.return_value = expected_datasets

        # Act: Call the execute method of the service
        actual_datasets = self.list_datasets_service.execute(project_id=project_id)

        # Assert: Check that the repository method was called correctly
        self.mock_bigquery_repo.list_datasets.assert_called_once_with(project_id=project_id)
        
        # Assert: Check that the service returned the expected data
        self.assertEqual(actual_datasets, expected_datasets)
        self.assertIsInstance(actual_datasets, List)
        if actual_datasets: # If list is not empty, check type of first element
            self.assertIsInstance(actual_datasets[0], DatasetMetadata)

    def test_execute_returns_empty_list_when_repo_returns_empty(self):
        # Arrange: Configure the mock repository to return an empty list
        project_id = "empty_project"
        self.mock_bigquery_repo.list_datasets.return_value = []

        # Act: Call the execute method
        actual_datasets = self.list_datasets_service.execute(project_id=project_id)

        # Assert: Check that the repository method was called
        self.mock_bigquery_repo.list_datasets.assert_called_once_with(project_id=project_id)
        
        # Assert: Check that an empty list is returned
        self.assertEqual(actual_datasets, [])

if __name__ == '__main__':
    unittest.main()
