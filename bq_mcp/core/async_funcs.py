"""Async utility functions for batch processing."""

import asyncio
from typing import Any, Coroutine, List, TypeVar

T = TypeVar("T")


async def gather_in_batches(
    tasks: List[Coroutine[Any, Any, T]], batch_size: int
) -> List[T]:
    """
    Execute async tasks in batches and return all results.

    Args:
        tasks: List of coroutines to execute
        batch_size: Number of tasks to run concurrently

    Returns:
        List of results from all tasks in the same order
    """
    results = []

    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)

    return results
