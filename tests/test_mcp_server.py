import os

import pytest

from bq_mcp.adapters import mcp_server


@pytest.mark.asyncio
async def test_app_lifespan_error():
    """Test the application lifespan context manager
    assert raise error if project IDs are not configured.
    """
    app = mcp_server.mcp
    os.environ.pop("PROJECT_IDS", None)  # Ensure no project IDs are set
    with pytest.raises(ValueError, match="Project IDs must be configured in settings."):
        async with mcp_server.app_lifespan(app):
            pass
    os.environ["PROJECT_IDS"] = ""  # Simulate an empty-string case for project IDs
    with pytest.raises(ValueError, match="Project IDs must be configured in settings."):
        async with mcp_server.app_lifespan(app):
            pass
