# TikTok Ads MCP Server

A Model Context Protocol (MCP) server for TikTok Ads API integration. This server enables AI assistants like Claude to interact with TikTok advertising campaigns, providing comprehensive campaign management, analytics, and optimization capabilities.

This project is part of the [AdsMCP AI advertising automation platform](https://adsmcp.com), an AI-powered platform designed to simplify and automate ad campaign management across multiple advertising networks. The platform emphasizes efficiency, data-driven insights, and intelligent automation for marketers and agencies. Additionally, AdsMCP also supports integration with other advertising platforms, such as **Google Ads** and **Meta (Facebook) Ads**, making it easier to manage campaigns across multiple networks from a single interface. 


## Features

- **Campaign Management**: Create, read, and update campaigns and ad groups
- **Performance Analytics**: Retrieve detailed performance metrics and insights
- **Audience Management**: Handle custom audiences and targeting options
- **Creative Management**: Upload and manage ad creatives
- **Reporting**: Generate and download custom performance reports

## Installation

### Prerequisites

- Python 3.10+
- TikTok For Business account with API access
- TikTok Ads Developer account and app registration

### Local Installation

```bash
# Clone the repository
git clone <repository-url>
cd adsmcp-server

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .
```

### Using uv (recommended)

```bash
# Install with uv
uv sync
```

## Remote MCP Server Option

If you don’t want to host the server and set it up manually yourself, [AdsMCP](https://adsmcp.com) provides a remote MCP server. You can easily connect your ad accounts within **one minute**, without worrying about server configuration or dependencies. Visit the **[AdsMCP Remote MCP Server Setup Guide](https://adsmcp.com/onboarding)** for a step-by-step tutorial to quickly connect your ad accounts.
  

### MCP Client Configuration

Add to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "tiktok-ads": {
      "command": "python",
      "args": ["/path/to/adsmcp-server/run_server.py"],
      "cwd": "/path/to/adsmcp-server",
      "env": {
        "TIKTOK_APP_ID": "your_app_id",
        "TIKTOK_APP_SECRET": "your_app_secret",
      }
    }
  }
}
```

## Available Tools

### Authentication
- `tiktok_ads_login` - Start TikTok Ads OAuth authentication flow
- `tiktok_ads_complete_auth` - Complete OAuth authentication with authorization code
- `tiktok_ads_auth_status` - Check current authentication status
- `tiktok_ads_switch_ad_account` - Switch to a different advertiser account

### Campaign Management
- `tiktok_ads_get_campaigns` - Retrieve all campaigns for the advertiser account
- `tiktok_ads_get_campaign_details` - Get detailed information about a specific campaign
- `tiktok_ads_get_adgroups` - Retrieve ad groups for a campaign

### Performance & Analytics
- `tiktok_ads_get_campaign_performance` - Get performance metrics for campaigns with detailed metrics support
- `tiktok_ads_get_adgroup_performance` - Get performance metrics for ad groups with breakdowns

### Consolidated tools (this fork)

Seven action-based tools cover the full surface, including writes. Upstream is
read-only by design; **every write here returns a preview unless you pass
`confirm=true`.**

| Tool | Selector | Covers |
|---|---|---|
| `tiktok_entity_get` | `entity_type` | campaigns, adgroups, ads (+ details), `account_info`, `pixels`, `pixel_event_stats`, catalogs, product sets, `interest_categories`, `regions`, `location_info`, `action_categories`, identities, audiences, lead downloads, Business Center info/assets |
| `tiktok_entity_manage` | `action` | create/update campaigns, ad groups and ads; enable/disable/delete in batch; create/update pixels; track events |
| `tiktok_report` | `report_type` | performance, audience, catalog, GMV Max, plus async task create/check/download |
| `tiktok_audience` | `action` | list, reach estimate, create CRM / lookalike / engagement audiences, delete |
| `tiktok_creative` | `action` | list/search videos and images, uploads, Spark Ads authorization, AI ad text |
| `tiktok_comment` | `action` | list, reply, hide/unhide |
| `tiktok_intelligence` | `analysis_type` | `funnel_overview`, `anomaly_check`, `optimization_actions`, `scaling_readiness`, `wasted_spend_audit`, plus interest/region/behaviour targeting lookups |

Requests are validated against `constraints.json` and the validators in
`validators/` before they reach TikTok — objective/optimization-goal pairs,
billing events, placement rules and CTA restrictions are captured from live
v1.3 API responses rather than TikTok's documentation, which disagrees with
the API in several places.

#### Diagnosing spend that isn't converting

`tiktok_intelligence` with `analysis_type="wasted_spend_audit"` ranks
zero-conversion campaigns (and, for the worst offenders, their ad groups) by
money at risk. Each finding carries a likely cause, a confidence level, and one
of three recommendations — `pause_or_reduce`, `keep_small_retest_budget`,
`needs_more_data`. Thresholds (`min_spend`, `min_clicks`, `high_ctr`,
`high_cpc`) are all overridable.

Zero conversions often means broken tracking rather than a bad campaign, so
cross-check with `tiktok_entity_get` `entity_type="pixel_event_stats"`: pixel
events far below reported conversions points at the pixel, not the creative.

## Development

Copy `.env.example` to `.env` and fill in your TikTok app credentials — `.env`
is gitignored and must never be committed.

```bash
uv sync
uv run pytest
```

The `mcp` dependency is capped at `<2.0.0`: `mcp` 2.x removed the
`Server.list_tools()` decorator this server registers its handlers with, so
importing `server.py` fails under 2.x. `tests/test_server_schema.py` imports
the server and cross-checks every declared enum value against its handler, so
both that incompatibility and a schema/handler drift fail the suite.

## Relationship to upstream

This is a fork of [AdsMCP/tiktok-ads-mcp-server](https://github.com/AdsMCP/tiktok-ads-mcp-server),
branched at upstream's 2025-10-20 commit. Upstream rewrote its history
afterwards, so the two share no merge base and cannot be merged directly —
changes are ported across by hand in either direction.

## Authentication

### TikTok Ads API Setup

1. **Register as Developer**
   - Visit [TikTok For Business Developer Portal](https://business-api.tiktok.com/)
   - Create a developer account
   - Register your application

2. **Get API Credentials**
   - App ID and App Secret from your registered app
   - Generate access token through OAuth flow
   - Note your Advertiser ID

3. **OAuth Flow** (for production)
   - Implement OAuth 2.0 flow for user authorization
   - Handle token refresh automatically
   - Store tokens securely

## Security Best Practices

- Never commit API credentials to version control
- Use environment variables for sensitive data
- Implement proper token rotation
- Monitor API usage and rate limits
- Use HTTPS for all communications

## API Rate Limits

TikTok Ads API has the following limits:
- 1000 requests per hour per app
- 10 concurrent requests
- Specific endpoint limits may apply

The server includes built-in rate limiting and retry logic.

## Error Handling

The server provides comprehensive error handling:
- API rate limit management
- Token expiration handling
- Network connectivity issues
- Invalid parameter validation

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please create an issue in this repository.

## Changelog

### v0.1.0 (Initial Release)
- Basic TikTok Ads API integration
- Campaign and ad group management
- Performance reporting
- OAuth authentication support