"""Smoke tests for the MCP surface itself.

Importing ``tiktok_ads_mcp.server`` runs the ``@app.list_tools()`` decorator, so
these also fail loudly if the installed ``mcp`` SDK drops the 1.x decorator API
the server is written against.
"""

import pytest

from tiktok_ads_mcp import server as server_module


@pytest.fixture(scope="module")
async def tools():
    return await server_module.list_tools()


@pytest.fixture(scope="module")
async def tools_by_name(tools):
    return {tool.name: tool for tool in tools}


def enum_of(tool, field):
    return tool.inputSchema["properties"][field]["enum"]


class TestToolSurface:
    async def test_the_consolidated_tools_are_all_registered(self, tools_by_name):
        for name in (
            "tiktok_entity_get", "tiktok_entity_manage", "tiktok_report",
            "tiktok_audience", "tiktok_creative", "tiktok_comment",
            "tiktok_intelligence",
        ):
            assert name in tools_by_name

    async def test_every_tool_has_a_description_and_schema(self, tools):
        for tool in tools:
            assert tool.description, f"{tool.name} has no description"
            assert tool.inputSchema.get("type") == "object", tool.name

    async def test_tool_names_are_unique(self, tools):
        names = [tool.name for tool in tools]
        assert len(names) == len(set(names))


class TestNewOptionsAreExposed:
    async def test_entity_get_advertises_the_new_entity_types(self, tools_by_name):
        entity_types = enum_of(tools_by_name["tiktok_entity_get"], "entity_type")
        assert "pixel_event_stats" in entity_types
        assert "location_info" in entity_types

    async def test_intelligence_advertises_the_audit(self, tools_by_name):
        assert "wasted_spend_audit" in enum_of(
            tools_by_name["tiktok_intelligence"], "analysis_type"
        )

    async def test_the_new_entity_types_have_their_parameters_declared(self, tools_by_name):
        properties = tools_by_name["tiktok_entity_get"].inputSchema["properties"]
        for field in ("pixel_ids", "location_ids", "start_date", "end_date",
                      "include_spend_history"):
            assert field in properties, field

    async def test_the_audit_thresholds_are_declared(self, tools_by_name):
        properties = tools_by_name["tiktok_intelligence"].inputSchema["properties"]
        for field in ("min_spend", "min_clicks", "high_ctr", "high_cpc",
                      "include_adgroup_breakdown", "max_adgroup_campaigns"):
            assert field in properties, field


class TestSchemaMatchesTheHandlers:
    async def test_declared_entity_types_all_have_a_handler(self, tools_by_name):
        from tiktok_ads_mcp.tools.entity_get import EntityGetTool

        declared = set(enum_of(tools_by_name["tiktok_entity_get"], "entity_type"))
        assert declared - set(EntityGetTool._handlers) == set()

    async def test_declared_analysis_types_all_have_a_handler(self, tools_by_name):
        from tiktok_ads_mcp.tools.intelligence import IntelligenceTool

        declared = set(enum_of(tools_by_name["tiktok_intelligence"], "analysis_type"))
        assert declared - set(IntelligenceTool._handlers) == set()

    async def test_no_handler_is_left_unreachable(self, tools_by_name):
        from tiktok_ads_mcp.tools.entity_get import EntityGetTool
        from tiktok_ads_mcp.tools.intelligence import IntelligenceTool

        entity_declared = set(enum_of(tools_by_name["tiktok_entity_get"], "entity_type"))
        assert set(EntityGetTool._handlers) - entity_declared == set()

        analysis_declared = set(enum_of(tools_by_name["tiktok_intelligence"], "analysis_type"))
        assert set(IntelligenceTool._handlers) - analysis_declared == set()


class TestArgumentSanitisation:
    def test_large_numeric_ids_survive_as_strings(self):
        sanitized = server_module._sanitize_arguments({"campaign_id": 1234567890123456789})
        assert sanitized["campaign_id"] == "1234567890123456789"

    def test_the_new_id_lists_are_sanitised_too(self):
        sanitized = server_module._sanitize_arguments({
            "pixel_ids": [1234567890123456789],
            "location_ids": [6252001],
        })
        assert sanitized["pixel_ids"] == ["1234567890123456789"]
        assert sanitized["location_ids"] == ["6252001"]

    def test_response_ids_are_stringified_on_the_way_out(self):
        sanitized = server_module._sanitize_response(
            {"data": {"list": [{"campaign_id": 1234567890123456789}]}}
        )
        assert sanitized["data"]["list"][0]["campaign_id"] == "1234567890123456789"
