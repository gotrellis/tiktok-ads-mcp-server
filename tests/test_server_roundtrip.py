"""End-to-end MCP round trips against the server, over real stdio.

The rest of the suite exercises the tool classes directly; this drives the
server the way clio-idx does -- spawn it as a subprocess, speak MCP over its
stdin/stdout -- which is the whole of the contract between them. clio-idx
installs this package into its own virtualenv and only ever execs the console
script, so a break in the wiring (registration, entry point, startup) shows up
here and nowhere else in the suite.

No TikTok account is needed. ``initialize`` only requires an app id/secret to
build the OAuth client; without an access token it stops there, so no request
ever leaves the process. ``list_tools`` is answered from the static tool table,
and ``tiktok_ads_auth_status`` is one of the auth tools that answer before the
"are we authenticated" gate, so it needs no token either.
"""

import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

SRC = str(Path(__file__).resolve().parents[1] / "src")


def server_params() -> StdioServerParameters:
    env = dict(os.environ)
    # The subprocess is a fresh interpreter: point it at the checkout rather
    # than requiring an editable install for the suite to run.
    env["PYTHONPATH"] = SRC + os.pathsep + env.get("PYTHONPATH", "")
    env["TIKTOK_APP_ID"] = "test-app-id"
    env["TIKTOK_APP_SECRET"] = "test-app-secret"
    # Leave TIKTOK_ACCESS_TOKEN unset so initialize() stops before any HTTP.
    env.pop("TIKTOK_ACCESS_TOKEN", None)
    env.pop("TIKTOK_ADVERTISER_ID", None)
    return StdioServerParameters(
        # ``-c`` rather than ``-m``: the package __init__ already imports
        # ``server``, which makes ``-m`` warn about a double import.
        command=sys.executable,
        args=["-c", "from tiktok_ads_mcp.server import run; run()"],
        env=env,
    )


@asynccontextmanager
async def connected():
    """Open a session inline rather than as a fixture.

    An async-generator fixture enters and exits the anyio scopes in different
    tasks, which anyio rejects; doing it inside the test keeps one task.
    """
    async with stdio_client(server_params()) as (read, write):
        async with ClientSession(read, write) as client:
            await client.initialize()
            yield client


class TestStdioRoundTrip:
    async def test_list_tools_returns_the_registered_tools(self):
        async with connected() as session:
            result = await session.list_tools()

        names = {tool.name for tool in result.tools}
        assert "tiktok_entity_get" in names
        assert "tiktok_ads_auth_status" in names

    async def test_a_tool_call_reaches_the_handler(self):
        """Round-trips call_tool, proving it is registered.

        Asserts on ``app_id`` rather than the auth status: the status branch
        also consults any token file on the host, but the app id always comes
        back from the environment this test set.
        """
        async with connected() as session:
            result = await session.call_tool("tiktok_ads_auth_status", {})

        payload = json.loads(result.content[0].text)
        assert payload["success"] is True
        assert payload["data"]["app_id"] == "test-app-id"
