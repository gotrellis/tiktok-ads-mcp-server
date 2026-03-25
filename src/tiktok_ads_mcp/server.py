"""TikTok Ads MCP Server implementation."""

import argparse
import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    ServerCapabilities,
    TextContent,
    Tool,
    ToolsCapability,
    LoggingCapability,
)
from pydantic import BaseModel

from .tiktok_client import TikTokAdsClient
from .oauth_simple import SimpleTikTokOAuth, start_manual_oauth
from .tools import (
    CampaignTools,
    CreativeTools,
    PerformanceTools,
    AudienceTools,
    ReportingTools,
)
# New consolidated tool imports
from .api.marketing_client import MarketingClient
from .cache.cache_manager import CacheManager
from .tools.entity_get import EntityGetTool
from .tools.entity_manage import EntityManageTool
from .tools.report import ReportTool
from .tools.audience import AudienceTool
from .tools.creative import CreativeTool
from .tools.comment import CommentTool
from .tools.intelligence import IntelligenceTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MCP server
app = Server("tiktok-ads-mcp")


class TikTokMCPServer:
    """TikTok Ads MCP Server class."""
    
    def __init__(self):
        self.client: Optional[TikTokAdsClient] = None
        self.campaign_tools: Optional[CampaignTools] = None
        self.creative_tools: Optional[CreativeTools] = None
        self.performance_tools: Optional[PerformanceTools] = None
        self.audience_tools: Optional[AudienceTools] = None
        self.reporting_tools: Optional[ReportingTools] = None
        self.app_id: Optional[str] = None
        self.app_secret: Optional[str] = None
        self.is_authenticated: bool = False
        self.primary_advertiser_id: Optional[str] = None
        self.available_advertiser_ids: List[str] = []
        self.oauth_client: Optional[SimpleTikTokOAuth] = None
        # New consolidated tools
        self.marketing_client: Optional[MarketingClient] = None
        self.cache: CacheManager = CacheManager()
        self.entity_get_tool: Optional[EntityGetTool] = None
        self.entity_manage_tool: Optional[EntityManageTool] = None
        self.report_tool: Optional[ReportTool] = None
        self.audience_tool: Optional[AudienceTool] = None
        self.creative_tool: Optional[CreativeTool] = None
        self.comment_tool: Optional[CommentTool] = None
        self.intelligence_tool: Optional[IntelligenceTool] = None
        
    async def initialize(self):
        """Initialize the TikTok Ads MCP Server with credentials check."""
        load_dotenv()
        try:
            # Store app credentials for OAuth login
            self.app_id = os.getenv("TIKTOK_APP_ID")
            self.app_secret = os.getenv("TIKTOK_APP_SECRET")
            access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
            advertiser_id = os.getenv("TIKTOK_ADVERTISER_ID")
            available_advertiser_ids = os.getenv("TIKTOK_AVAILABLE_ADVERTISER_IDS", "")
            available_advertiser_ids = [x.strip() for x in available_advertiser_ids.split(",") if x.strip()]
            if advertiser_id not in available_advertiser_ids:
                available_advertiser_ids.append(advertiser_id)
            
            if not self.app_id or not self.app_secret:
                raise ValueError(
                    "Missing TikTok API credentials. Provide TIKTOK_APP_ID and TIKTOK_APP_SECRET environment variables."
                )
            
            # Initialize OAuth client
            self.oauth_client = SimpleTikTokOAuth(self.app_id, self.app_secret)
            
            # If access token is provided, authenticate immediately (legacy mode)
            if access_token and advertiser_id:
                logger.info("Using direct token authentication...")
                await self._authenticate_with_tokens(access_token, advertiser_id, available_advertiser_ids)
            else:
                logger.info("OAuth credentials configured. Use the 'tiktok_ads_login' tool to authenticate.")
            
            logger.info("TikTok Ads MCP Server initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize TikTok Ads MCP Server: {e}")
            raise
    
    async def _authenticate_with_tokens(self, access_token: str, advertiser_id: str, available_advertiser_ids: list[str]):
        """Authenticate using provided tokens."""
        # Initialize TikTok client
        self.client = TikTokAdsClient(
            app_id=self.app_id,
            app_secret=self.app_secret,
            access_token=access_token,
            advertiser_id=advertiser_id,
            available_advertiser_ids=available_advertiser_ids,
        )
        
        # Initialize existing tool modules
        self.campaign_tools = CampaignTools(self.client)
        self.creative_tools = CreativeTools(self.client)
        self.performance_tools = PerformanceTools(self.client)
        self.audience_tools = AudienceTools(self.client)
        self.reporting_tools = ReportingTools(self.client)

        # Initialize new consolidated tools
        self.marketing_client = MarketingClient(
            access_token=access_token,
            advertiser_id=advertiser_id,
        )
        self.entity_get_tool = EntityGetTool(self.marketing_client, self.cache)
        self.entity_manage_tool = EntityManageTool(self.marketing_client)
        self.report_tool = ReportTool(self.marketing_client)
        self.audience_tool = AudienceTool(self.marketing_client, self.cache)
        self.creative_tool = CreativeTool(self.marketing_client)
        self.comment_tool = CommentTool(self.marketing_client)
        self.intelligence_tool = IntelligenceTool(self.marketing_client, self.cache)

        self.is_authenticated = True
        self.primary_advertiser_id = advertiser_id
        self.available_advertiser_ids = available_advertiser_ids
        
    async def start_oauth_flow(self, force_reauth: bool = False) -> Dict[str, Any]:
        """Start OAuth flow (non-blocking)."""
        if not self.oauth_client:
            return {"success": False, "error": "OAuth client not initialized"}
        
        try:
            result, token_data = start_manual_oauth(self.app_id, self.app_secret, force_reauth=force_reauth)
            if token_data:
                await self._authenticate_with_tokens(
                    token_data['access_token'], 
                    token_data['primary_advertiser_id'],
                    token_data['advertiser_ids'],
                )
            return {"success": True, "data": result}
        except Exception as e:
            logger.error(f"Failed to start OAuth flow: {e}")
            return {"success": False, "data": {"error": str(e)}}
    
    async def complete_oauth(self, auth_code: str) -> Dict[str, Any]:
        """Complete OAuth flow with authorization code."""
        if not self.oauth_client:
            return {"success": False, "data": {"error": "OAuth client not initialized"}}
        
        try:
            token_data = await self.oauth_client.exchange_code_for_token(auth_code)
            
            if not token_data:
                return {"success": False, "data": {"error": "Failed to exchange authorization code for tokens"}}
            
            if "error_message" in token_data:
                return {"success": False, "data": {"error": token_data["error_message"]}}
            
            # Authenticate with the tokens
            await self._authenticate_with_tokens(
                token_data['access_token'], 
                token_data['primary_advertiser_id'],
                token_data['advertiser_ids'],
            )
            self.available_advertiser_ids = token_data['advertiser_ids']
            self.primary_advertiser_id = token_data['primary_advertiser_id']
            
            logger.info(f"OAuth completed successfully. Using advertiser ID: {token_data['primary_advertiser_id']}")
            
            return {
                "success": True,
                "data": {
                    "message": "Authentication completed successfully",
                    "primary_advertiser_id": token_data['primary_advertiser_id'],
                    "available_advertiser_ids": token_data['advertiser_ids']
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to complete OAuth: {e}")
            return {"success": False, "error": str(e)}
    
    async def get_auth_status(self) -> Dict[str, Any]:
        """Get current authentication status."""
        if self.is_authenticated:
            # check runtime 
            return {
                'success': True,
                'data': {
                    'status': 'authenticated',
                    "app_id": self.app_id,
                    "available_advertiser_ids": self.available_advertiser_ids,
                    "primary_advertiser_id": self.primary_advertiser_id,
                    "message": "Already Authenticated"
                }
            }
        else:
            oauth_client = SimpleTikTokOAuth(self.app_id, self.app_secret)
            saved_tokens = oauth_client.load_saved_tokens()
            if saved_tokens and saved_tokens.get('access_token'):
                await self._authenticate_with_tokens(
                    saved_tokens['access_token'], 
                    saved_tokens['primary_advertiser_id'],
                    saved_tokens['advertiser_ids'],
                )
                return {
                    'success': True,
                    'data': {
                        'status': 'authenticated',
                        'app_id': self.app_id,
                        'available_advertiser_ids': saved_tokens.get('advertiser_ids', []),
                        'primary_advertiser_id': saved_tokens.get('primary_advertiser_id'),
                        'message': 'Already authenticated with saved tokens',
                    }
                }
            else:
                return {
                    'success': True,
                    'data': {
                        'status': 'not_authenticated',
                        'app_id': self.app_id,
                        'message': 'No saved tokens found. Please use tiktok_ads_login tool to authenticate.'
                    }
                }
    
    async def switch_ad_account(self, advertiser_id: str) -> Dict[str, Any]:
        """Switch to a different advertiser account."""
        if not self.is_authenticated:
            return {"success": False, "error": "Not authenticated. Please login first."}
        
        warning_message = ""
        if advertiser_id not in self.available_advertiser_ids:
            warning_message = f"Warn: Advertiser ID {advertiser_id} maybe not available."
        
        try:
            # Get the current access token
            if self.client:
                access_token = self.client.access_token
                # Re-authenticate with the new advertiser ID using existing method
                await self._authenticate_with_tokens(access_token, advertiser_id, self.available_advertiser_ids)
                
                logger.info(f"Switched to advertiser account: {advertiser_id}")
                
                return {
                    "success": True,
                    "data": {
                        "message": f"switched to advertiser account {advertiser_id}. {warning_message}",
                        "current_advertiser_id": advertiser_id,
                        "available_advertiser_ids": self.available_advertiser_ids
                    }
                }
            else:
                return {"success": False, "error": "Client not initialized"}
                
        except Exception as e:
            logger.error(f"Failed to switch advertiser account: {e}")
            return {"success": False, "error": str(e)}


# Global server instance
tiktok_server = TikTokMCPServer()


@app.list_tools()
async def list_tools() -> List[Tool]:
    """List all available TikTok Ads tools."""
    tools = []
    
    # Authentication tools (always available)
    tools.extend([
        Tool(
            name="tiktok_ads_login",
            description="Start TikTok Ads OAuth authentication flow",
            inputSchema={
                "type": "object",
                "properties": {
                    "force_reauth": {
                        "type": "boolean",
                        "description": "Whether to force reauthentication even if tokens exist. Use when access_token is expired or when operations fail due to lack of permissions and require user re-authorization to complete specific actions."
                    }
                },
                "additionalProperties": False
            }
        ),
        Tool(
            name="tiktok_ads_complete_auth",
            description="Complete OAuth authentication with authorization code",
            inputSchema={
                "type": "object",
                "properties": {
                    "auth_code": {
                        "type": "string",
                        "description": "Authorization code from OAuth redirect"
                    }
                },
                "required": ["auth_code"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="tiktok_ads_auth_status",
            description="Check current authentication status with TikTok Ads API",
            inputSchema={
                "type": "object",
                "properties": {},
                "additionalProperties": False
            }
        ),
        Tool(
            name="tiktok_ads_switch_ad_account",
            description="Switch to a different advertiser account, DO NOT try to switch advertiser account automatically, only if user ask to switch",
            inputSchema={
                "type": "object",
                "properties": {
                    "advertiser_id": {
                        "type": "string",
                        "description": "The advertiser ID to switch to"
                    }
                },
                "required": ["advertiser_id"],
                "additionalProperties": False
            }
        )
    ])
    
    # Campaign management tools
    tools.extend([
        Tool(
            name="tiktok_ads_get_campaigns",
            description="Retrieve all campaigns for the advertiser account",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ['STATUS_ALL', 'STATUS_NOT_DELETE', 'STATUS_NOT_DELIVERY', 'STATUS_DELIVERY_OK', 'STATUS_DISABLE', 'STATUS_DELETE'],
                        "description": "Filter campaigns by status"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10,
                        "description": "Maximum number of campaigns to return"
                    }
                }
            }
        ),
        Tool(
            name="tiktok_ads_get_campaign_details",
            description="Get detailed information about a specific campaign",
            inputSchema={
                "type": "object",
                "properties": {
                    "campaign_id": {
                        "type": "string",
                        "description": "The campaign ID to retrieve details for"
                    }
                },
                "required": ["campaign_id"]
            }
        ),
        Tool(
            name="tiktok_ads_get_adgroups",
            description="Retrieve ad groups for a campaign",
            inputSchema={
                "type": "object",
                "properties": {
                    "campaign_id": {
                        "type": "string",
                        "description": "Campaign ID to get ad groups for"
                    },
                    "status": {
                        "type": "string",
                        "enum": ['STATUS_ALL', 'STATUS_NOT_DELETE', 'STATUS_NOT_DELIVERY', 'STATUS_DELIVERY_OK', 'STATUS_DISABLE', 'STATUS_DELETE'],
                        "description": "Filter ad groups by status"
                    }
                },
                "required": ["campaign_id"]
            }
        ),
        Tool(
            name="tiktok_ads_get_adgroup_details",
            description="Get detailed information about a specific ad group",
            inputSchema={
                "type": "object",
                "properties": {
                    "adgroup_id": {
                        "type": "string",
                        "description": "The ad group ID to retrieve details for"
                    }
                },
                "required": ["adgroup_id"]
            }
        ),
        Tool(
            name="tiktok_ads_get_ads",
            description="Retrieve ads, optionally filtered by ad group or campaign",
            inputSchema={
                "type": "object",
                "properties": {
                    "adgroup_id": {
                        "type": "string",
                        "description": "Filter ads by ad group ID"
                    },
                    "campaign_id": {
                        "type": "string",
                        "description": "Filter ads by campaign ID"
                    },
                    "status": {
                        "type": "string",
                        "enum": ['STATUS_ALL', 'STATUS_NOT_DELETE', 'STATUS_NOT_DELIVERY', 'STATUS_DELIVERY_OK', 'STATUS_DISABLE', 'STATUS_DELETE'],
                        "description": "Filter ads by status"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10,
                        "description": "Maximum number of ads to return"
                    }
                }
            }
        ),
        Tool(
            name="tiktok_ads_get_ad_details",
            description="Get detailed information about a specific ad",
            inputSchema={
                "type": "object",
                "properties": {
                    "ad_id": {
                        "type": "string",
                        "description": "The ad ID to retrieve details for"
                    }
                },
                "required": ["ad_id"]
            }
        )
    ])
    
    # Performance analytics tools
    tools.extend([
        Tool(
            name="tiktok_ads_get_campaign_performance",
            description="Get performance metrics for campaigns",
            inputSchema={
                "type": "object",
                "properties": {
                    "campaign_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of campaign IDs to analyze"
                    },
                    "date_range": {
                        "type": "string",
                        "enum": ["today", "yesterday", "last_7_days", "last_14_days", "last_30_days"],
                        "description": "Date range for performance data"
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": """
## Metrics to include: 

### attribute metrics:
Attribute metrics, such as ad group names and promotion types, are basic attributes of your campaigns, ad groups, or ads. Attribute metrics are valid only when an ID type of dimension is used.

Field	Type	Description	Detail
advertiser_name	string	Advertiser account name	Supported at Advertiser, Campaign, Ad Group, and Ad level.
advertiser_id	string	Advertiser account ID	Supported at Advertiser, Campaign, Ad Group, and Ad level.
campaign_name	string	Campaign name	Supported at Campaign, Ad Group and Ad level.
campaign_id	string	Campaign ID	Supported at Ad Group and Ad level.
objective_type	string	Advertising objective	Supported at Campaign, Ad Group and Ad level.
split_test	string	Split test status	Supported at Campaign, Ad Group and Ad level.
campaign_budget	string	Campaign budget	Supported at Campaign, Ad Group and Ad level.
campaign_dedicate_type	string	Campaign type	iOS14 Dedicated Campaign or regular campaign. Supported at Campaign, Ad Group and Ad level.
app_promotion_type	string	App promotion type	"Supported at Campaign, Ad Group and Ad level. Enum values: APP_INSTALL, APP_RETARGETING.
APP_INSTALL and APP_RETARGETING will be returned when objective_type is APP_PROMOTION. Otherwise, UNSET will be returned."
adgroup_name	string	Ad group name	Supported at Ad Group and Ad level.
adgroup_id	string	Ad group ID	Supported at Ad level.
placement_type	string	Placement type	Supported at Ad Group and Ad level.
promotion_type	string	Promotion type	It can be app, website, or others. Supported at Ad Group and Ad levels in both synchronous and asynchronous reports.
opt_status	string	Automated creative optimization	Supported at Ad Group and Ad level.
adgroup_download_url	string	Download URL/Website URL	Supported at Ad Group, and Ad level.
profile_image	string	Profile image	Supported at Ad Group and Ad level.
dpa_target_audience_type	string	Target audience type for DPA	The Audience that DPA products target. Supported at Ad Group or Ad levels in both synchronous and asynchronous reports.
budget	string	Ad group budget	Supported at Ad Group and Ad level.
smart_target	string	Optimization goal	Supported at Ad Group and Ad level.
pricing_categoryTo be deprecated	string	Billing Event	"Supported at Ad Group and Ad level.
If you want to retrieve the billing event of your ads, use the new metric billing_event."
billing_event	string	Billing Event	"Supported at Ad Group and Ad level.
Example: ""Clicks"", ""Impression""."
bid_strategy	string	Bid strategy	Supported at Ad Group and Ad level.
bid	string	Bid	Supported at Ad Group and Ad level.
bid_secondary_goal	string	Bid for secondary goal	Supported at Ad Group and Ad level.
aeo_type	string	App Event Optimization Type	Supported at Ad Group and Ad level. (Already supported at Ad Group level, and will be supported at Ad level)
ad_name	string	Ad name	Supported at Ad level.
ad_id	string	Ad ID	Supported at Ad level.
ad_text	string	Ad title	Supported at Ad level.
call_to_action	string	Call to action	Supported at Ad level.
ad_profile_image	string	Profile image (Ad level)	Supported at Ad level.
ad_url	string	URL (Ad level)	Supported at Ad level.
tt_app_id	string	TikTok App ID	TikTok App ID, the App ID you used when creating an Ad Group. Supported at Ad Group and Ad level. Returned if the promotion type of one Ad Group is App.
tt_app_name	string	TikTok App Name	The name of your TikTok App. Supported at Ad Group and Ad level. Returned if the promotion type of one Ad Group is App.
mobile_app_id	string	Mobile App ID	Mobile App ID.Examples are, App Store: https://apps.apple.com/us/app/angry-birds/id343200656; Google Play：https://play.google.com/store/apps/details?id=com.rovio.angrybirds.Supported at Ad Group and Ad level. Returned if the promotion type of one Ad Group is App.
image_mode	string	Format	Supported at Ad level.
currency	string	currency	The currency code, e. g. USD. Note that if you want to use currency as metrics, then the dimensions field in your request must include adgroup_id/ad_id/campaign_id/advertiser_id.
is_aco	boolean	Whether the ad is an automated ad or a Smart Creative ad. Set to True for an automated ad or Smart Creative ad.	Supported at AUCTION_ADGROUP level.
is_smart_creative	boolean	Whether the ad is a Smart Creative ad.	Supported at AUCTION_AD level.

### Core metrics
Core metrics provide fundamental insights into your advertising performance, covering essential aspects such as cost and impressions.

Field	Type	Description	Detail
spend	string	Cost	Sum of your total ad spend.
billed_cost	string	Net cost	"Sum of your total ad spend, excluding ad credit or coupons used. Note:This metric is only supported in synchronous basic reports. This metric might delay up to 11 hours, with records only available from September 1, 2023."
cash_spend	string	Cost Charged by Cash	The estimated amount of money you've spent on your campaign, ad group, or ad during its schedule charged by cash. (This metric can be required at the advertiser level only and lifetime, hourly breakdown is not supported.) Please note that there may be a delay from 24h to 48h between when you were charged and when you can see it through the API.
voucher_spend	string	Cost Charged by Voucher	The estimated amount of money you've spent on your campaign, ad group, or ad during its schedule charged by voucher. (This metric can be required at the advertiser level only and lifetime, hourly breakdown is not supported.) Please note that there may be a delay from 24h to 48h between when you were charged and when you can see it through the API.
cpc	string	CPC (destination)	Average cost of each click to a specified destination.
cpm	string	CPM	Average amount you spent per 1,000 impressions.
impressions	string	Impressions	Number of times your ads were shown.
gross_impressions	string	Gross Impressions (Includes Invalid Impressions)	Number of times your ads were shown, including invalid impressions.
clicks	string	Clicks (destination)	Number of clicks from your ads to a specified destination.
ctr	string	CTR (destination)	Percentage of impressions that resulted in a destination click out of all impressions.
reach	string	Reach	Number of unique users who saw your ads at least once.
cost_per_1000_reached	string	Cost per 1,000 people reached	Average cost to reach 1,000 unique users.
frequency	string	Frequency	The average number of times each user saw your ad over a given time period.
conversion	string	Conversions	Number of times your ad resulted in the optimization event you selected.
cost_per_conversion	string	Cost per conversion	Average amount spent on a conversion.
conversion_rate	string	Conversion rate (CVR, clicks)	"Percentage of results you received out of all destination clicks on your ads. Note: Starting late October, 2023, the calculation logic for this metric will be updated to be impression-based (the same as conversion_rate_v2). To ensure a smooth API integration and avoid disruptions caused by the change in calculation logic, we recommend you switch to using the impression-based metric conversion_rate_v2 as soon as possible."
conversion_rate_v2	string	Conversion rate (CVR)	Percentage of results you received out of all impressions on your ads.
real_time_conversion	string	Conversions by conversion time	Number of times your ad resulted in the optimization event you selected.
real_time_cost_per_conversion	string	Cost per conversion by conversion time	Average amount spent on a conversion.
real_time_conversion_rate	string	Real-time conversion rate (CVR, clicks)	"Percentage of conversions you received out of all destination clicks on your ads. Note: Starting late October, 2023, the calculation logic for this metric will be updated to be impression-based (the same as real_time_conversion_rate_v2). To ensure a smooth API integration and avoid disruptions caused by the change in calculation logic, we recommend you switch to using the impression-based metric real_time_conversion_rate_v2 as soon as possible."
real_time_conversion_rate_v2	string	Conversion rate (CVR) by conversion time	Percentage of conversions you received out of all impressions on your ads.
result	string	Results	Number of times your ad resulted in an intended outcome based on your campaign objective and optimization goal.
cost_per_result	string	Cost per result	Average cost per each result from your ads.
result_rate	string	Result rate	Percentage of results that happened out of all impressions on your ads.
real_time_result	string	Real-time results	Number of times your ad resulted in an intended outcome based on your campaign objective and optimization goal.
real_time_cost_per_result	string	Real-time cost per result	Average cost per each result from your ads.
real_time_result_rate	string	Real-time result rate	Percentage of results that happened out of all impressions on your ads.
secondary_goal_result	string	Deep funnel result	Number of times your ad resulted in an intended outcome based on the deep funnel event you selected.
cost_per_secondary_goal_result	string	Cost per deep funnel result	Average cost per each deep funnel result from your ads.
secondary_goal_result_rate	string	Deep funnel result rate	Percentage of deep funnel results out of total impressions on your ads.
"""
                    }
                },
                "required": ["campaign_ids", "date_range"]
            }
        ),
        Tool(
            name="tiktok_ads_get_adgroup_performance",
            description="Get performance metrics for ad groups",
            inputSchema={
                "type": "object",
                "properties": {
                    "adgroup_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of ad group IDs to analyze"
                    },
                    "date_range": {
                        "type": "string",
                        "enum": ["today", "yesterday", "last_7_days", "last_14_days", "last_30_days"],
                        "description": "Date range for performance data"
                    },
                    "breakdowns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Data breakdowns: age, gender, country, placement"
                    }
                },
                "required": ["adgroup_ids", "date_range"]
            }
        )
    ])
    
    # Reporting tools
    tools.extend([
        Tool(
            name="tiktok_ads_generate_report",
            description="Generate a custom performance report with flexible dimensions, metrics, and date range. Use dimension 'stat_time_day' for daily breakdowns. Supports campaign, ad group, and ad level reporting. Returns a task_id for async retrieval.",
            inputSchema={
                "type": "object",
                "properties": {
                    "report_type": {
                        "type": "string",
                        "enum": ["BASIC", "AUDIENCE", "PLACEMENT", "DPA"],
                        "description": "Type of report to generate"
                    },
                    "dimensions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Report dimensions for grouping data (e.g. campaign_id, adgroup_id, ad_id, stat_time_day)"
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Metrics to include: impressions, clicks, conversion, spend, ctr, cpm, cpc, conversion_rate, cost_per_conversion, reach, frequency, video_play_actions, video_watched_2s, video_watched_6s, profile_visits, likes, comments, shares, follows"
                    },
                    "date_range": {
                        "type": "object",
                        "properties": {
                            "start_date": {
                                "type": "string",
                                "description": "Start date in YYYY-MM-DD format"
                            },
                            "end_date": {
                                "type": "string",
                                "description": "End date in YYYY-MM-DD format"
                            }
                        },
                        "required": ["start_date", "end_date"],
                        "description": "Date range for the report"
                    },
                    "filtering": {
                        "type": "object",
                        "description": "Optional filters (e.g. campaign_ids, adgroup_ids, ad_ids)"
                    }
                },
                "required": ["report_type", "dimensions", "metrics", "date_range"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="tiktok_ads_generate_quick_report",
            description="Generate a synchronous performance report that returns daily data directly. Supports campaign, ad group, and ad level daily breakdowns with standard metrics.",
            inputSchema={
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": ["campaign", "adgroup", "ad"],
                        "description": "Type of entity to report on"
                    },
                    "entity_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of entity IDs to filter by. If omitted, includes all."
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (defaults to 7 days ago)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (defaults to yesterday)"
                    },
                    "include_breakdowns": {
                        "type": "boolean",
                        "description": "Whether to include age and gender breakdowns"
                    }
                },
                "additionalProperties": False
            }
        ),
    ])

    # ── New consolidated tools ──────────────────────────────────────────
    tools.extend([
        Tool(
            name="tiktok_entity_get",
            description="Read any TikTok Ads entity: campaigns, adgroups, ads, account_info, pixels, catalogs, catalog_products, product_sets, interest_categories, regions, action_categories, identities, audiences, lead_forms, lead_download_task (create task to download leads from a form), lead_download (download leads from completed task), bc_info (Business Center info), bc_assets (list advertisers under a BC). Use entity_type to select what to read. Supports filtering, pagination, and caching for slow-changing data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": [
                            "campaigns", "campaign_details",
                            "adgroups", "adgroup_details",
                            "ads", "ad_details",
                            "account_info", "pixels",
                            "catalogs", "catalog_products", "product_sets",
                            "interest_categories", "regions", "action_categories",
                            "identities", "audiences",
                            "lead_forms",  # returns guidance to use bc_assets instead
                            "lead_download_task", "lead_download",
                            "bc_info", "bc_assets",
                        ],
                        "description": "Type of entity to retrieve"
                    },
                    "campaign_id": {"type": "string", "description": "Single campaign ID (for campaign_details or filtering adgroups/ads)"},
                    "campaign_ids": {"type": "array", "items": {"type": "string"}, "description": "List of campaign IDs to filter by"},
                    "adgroup_id": {"type": "string", "description": "Single ad group ID (for adgroup_details or filtering ads)"},
                    "adgroup_ids": {"type": "array", "items": {"type": "string"}, "description": "List of ad group IDs to filter by"},
                    "ad_id": {"type": "string", "description": "Single ad ID (for ad_details)"},
                    "ad_ids": {"type": "array", "items": {"type": "string"}, "description": "List of ad IDs to filter by"},
                    "catalog_id": {"type": "string", "description": "Catalog ID (for catalog_products, product_sets)"},
                    "bc_id": {"type": "string", "description": "Business Center ID (required for catalogs, bc_info, bc_assets)"},
                    "asset_type": {"type": "string", "description": "Asset type for bc_assets (default: ADVERTISER)"},
                    "status": {"type": "string", "enum": ["STATUS_ALL", "STATUS_NOT_DELETE", "STATUS_NOT_DELIVERY", "STATUS_DELIVERY_OK", "STATUS_DISABLE", "STATUS_DELETE"], "description": "Filter by status"},
                    "page": {"type": "integer", "description": "Page number (default 1)"},
                    "page_size": {"type": "integer", "description": "Page size (default 200)"},
                    "placement": {"type": "string", "description": "Placement for regions (default PLACEMENT_TIKTOK)"},
                    "version": {"type": "integer", "description": "Interest category version (default 2)"},
                    "form_id": {"type": "string", "description": "Lead form ID (for lead_download_task)"},
                    "task_id": {"type": "string", "description": "Task ID (for lead_download — from lead_download_task response)"},
                },
                "required": ["entity_type"],
            }
        ),
        Tool(
            name="tiktok_entity_manage",
            description="Create, update, enable/disable/delete TikTok Ads entities. All write operations require confirm=true to execute; without it, returns a preview.\n\nIMPORTANT for create_campaign: You MUST ask the user for these fields — do NOT guess or use defaults:\n  - campaign_name (user must name it)\n  - objective_type: valid values are REACH, RF_REACH, TRAFFIC, ENGAGEMENT, APP_PROMOTION, LEAD_GENERATION, PRODUCT_SALES, WEB_CONVERSIONS, SEARCH. NOTE: VIDEO_VIEWS is DEPRECATED — use ENGAGEMENT instead.\n  - budget (daily or total amount)\n  - budget_mode (BUDGET_MODE_DAY or BUDGET_MODE_TOTAL)\n\nIMPORTANT for create_adgroup — MUST ask user, never guess:\n  - campaign_id, adgroup_name, placement_type, budget, budget_mode, schedule_type, location_ids\n  - optimization_goal and billing_event depend on campaign objective:\n      TRAFFIC:     goals=CLICK/REACH/VIDEO_PLAY/CONVERT — billing=CPC/CPM/CPV/OCPM\n      ENGAGEMENT:  goals=FOLLOWERS→billing=OCPM, PROFILE_VIEWS→billing=CPC (NOT CPM or OCPM)\n      REACH:       goals=REACH — billing=CPM/OCPM — also requires frequency + frequency_schedule\n      LEAD_GENERATION: goals=LEAD — billing=OCPM\n      APP_PROMOTION:   goals=INSTALL/CLICK/CONVERT — billing=CPC/OCPM\n      WEB_CONVERSIONS: goals=CONVERT/LANDING_PAGE_VIEW — billing=OCPM\n  - For PLACEMENT_TYPE_NORMAL you must also provide placements (e.g. ['PLACEMENT_TIKTOK'])\n  - ENGAGEMENT and REACH campaigns require PLACEMENT_TYPE_NORMAL with ['PLACEMENT_TIKTOK']\n\nIMPORTANT for create_ad — CTA rules (verified via live API):\n  - ENGAGEMENT campaigns: do NOT set call_to_action — TikTok rejects ALL explicit CTAs for ENGAGEMENT. Omit it and TikTok auto-assigns.\n  - TRAFFIC, REACH, and most other objectives: any of these 29 CTAs are accepted: APPLY_NOW, BOOK_NOW, CALL_NOW, CHECK_AVAILABILITY, CONTACT_US, DOWNLOAD_NOW, EXPERIENCE_NOW, GET_QUOTE, GET_SHOWTIMES, GET_TICKETS_NOW, INSTALL_NOW, INTERESTED, JOIN_THIS_HASHTAG, LEARN_MORE, LISTEN_NOW, ORDER_NOW, PLAY_GAME, PREORDER_NOW, READ_MORE, SEND_MESSAGE, SHOOT_WITH_THIS_EFFECT, SHOP_NOW, SIGN_UP, SUBSCRIBE, VIEW_NOW, VIEW_VIDEO_WITH_THIS_EFFECT, VISIT_STORE, WATCH_LIVE, WATCH_NOW\n  - VIEW_PROFILE is rejected for Spark Ads (ads using tiktok_item_id)\n  - DEPRECATED CTAs (always rejected): DOWNLOAD, BUY_NOW, WATCH_MORE, SEE_MORE, GET_OFFER\n  - For Spark Ads (tiktok_item_id set): use ad_format=SINGLE_VIDEO, provide identity_id + identity_type + identity_authorized_bc_id (for BC_AUTH_TT). Do NOT set video_id.\n  - TRAFFIC + non-Spark ads require landing_page_url. LEAD_GENERATION requires page_id.\n\nIf any required fields are missing, ask the user before calling.\n\nActions: create_campaign, update_campaign, create_adgroup, update_adgroup, create_ad, update_ad, enable_campaigns, disable_campaigns, delete_campaigns, enable_adgroups, disable_adgroups, delete_adgroups, enable_ads, disable_ads, delete_ads, create_pixel, update_pixel, track_event.",
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "create_campaign", "update_campaign",
                            "create_adgroup", "update_adgroup",
                            "create_ad", "update_ad",
                            "enable_campaigns", "disable_campaigns", "delete_campaigns",
                            "enable_adgroups", "disable_adgroups", "delete_adgroups",
                            "enable_ads", "disable_ads", "delete_ads",
                            "create_pixel", "update_pixel", "track_event",
                        ],
                        "description": "Action to perform"
                    },
                    "params": {
                        "type": "object",
                        "description": "Entity parameters. create_campaign: campaign_name, objective_type (TRAFFIC/ENGAGEMENT/REACH/LEAD_GENERATION/APP_PROMOTION/WEB_CONVERSIONS/PRODUCT_SALES/SEARCH/RF_REACH — NOT VIDEO_VIEWS, deprecated), budget, budget_mode. create_adgroup: campaign_id, adgroup_name, placement_type, billing_event, budget, budget_mode, schedule_type, optimization_goal, location_ids; ENGAGEMENT+FOLLOWERS needs billing_event=OCPM; ENGAGEMENT+PROFILE_VIEWS needs billing_event=CPC; REACH needs frequency+frequency_schedule. create_ad: adgroup_id, ad_name, ad_format (SINGLE_VIDEO/SINGLE_IMAGE/CAROUSEL/CAROUSEL_ADS/LIVE_CONTENT/CATALOG_CAROUSEL), ad_text (not needed for Spark Ads); SINGLE_VIDEO→video_id, SINGLE_IMAGE→image_ids, CAROUSEL→image_ids(2-10); Spark Ads (organic post): ad_format=SINGLE_VIDEO + tiktok_item_id + identity_id + identity_type + identity_authorized_bc_id (no video_id, no ad_text needed); call_to_action: OMIT for ENGAGEMENT (all CTAs rejected), for other objectives use any of 29 valid values (LEARN_MORE, SHOP_NOW, WATCH_NOW, etc.) — do NOT use VIEW_PROFILE for Spark Ads, do NOT use deprecated DOWNLOAD/BUY_NOW/WATCH_MORE/SEE_MORE/GET_OFFER."
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Set to true to execute the operation. Default false returns a preview."
                    },
                    "campaign_ids": {"type": "array", "items": {"type": "string"}, "description": "Campaign IDs for status updates"},
                    "adgroup_ids": {"type": "array", "items": {"type": "string"}, "description": "Ad group IDs for status updates"},
                    "ad_ids": {"type": "array", "items": {"type": "string"}, "description": "Ad IDs for status updates"},
                    "campaign_objective": {"type": "string", "description": "Parent campaign objective — pass when creating adgroups/ads so validation uses correct rules (e.g. TRAFFIC, ENGAGEMENT, REACH, LEAD_GENERATION, APP_PROMOTION, WEB_CONVERSIONS, PRODUCT_SALES)"},
                    "pixel_code": {"type": "string", "description": "Pixel code (for update_pixel, track_event)"},
                    "pixel_name": {"type": "string", "description": "Pixel name (for create_pixel, update_pixel)"},
                    "event": {"type": "string", "description": "Event name (for track_event)"},
                    "event_id": {"type": "string", "description": "Event ID for dedup (for track_event)"},
                    "properties": {"type": "object", "description": "Event properties (for track_event)"},
                },
                "required": ["action"],
            }
        ),
        Tool(
            name="tiktok_report",
            description="Pull any TikTok Ads report. Types: performance (basic metrics, time-based via stat_time_day/stat_time_hour), audience (demographics/geo/device/placement — use 'breakdown' param for easy analysis: demographic, gender, age, country, province, device, device_brand, placement, language, interest, network), catalog (product performance), async_report (create task), check_task, download_task, gmv_max (Smart+ reports). Supports date_range presets or explicit start_date/end_date.",
            inputSchema={
                "type": "object",
                "properties": {
                    "report_type": {
                        "type": "string",
                        "enum": ["performance", "audience", "catalog", "async_report", "check_task", "download_task", "gmv_max"],
                        "description": "Type of report to generate"
                    },
                    "entity_level": {"type": "string", "enum": ["campaign", "adgroup", "ad"], "description": "Entity level for the report (default: campaign)"},
                    "data_level": {"type": "string", "enum": ["AUCTION_CAMPAIGN", "AUCTION_ADGROUP", "AUCTION_AD"], "description": "Explicit data level (overrides entity_level)"},
                    "breakdown": {
                        "type": "string",
                        "enum": ["demographic", "gender", "age", "country", "geo", "province", "device", "device_brand", "placement", "language", "interest", "network"],
                        "description": "Audience breakdown shortcut (for report_type=audience). Auto-sets dimensions. demographic=gender+age, country/geo=country_code, province=country+province, device=platform, device_brand=platform+device_brand_id, placement, language, interest, network=connection type. For time-based analysis use report_type=performance with stat_time_day/stat_time_hour dimension."
                    },
                    "dimensions": {"type": "array", "items": {"type": "string"}, "description": "Report dimensions (e.g. campaign_id, adgroup_id, ad_id, stat_time_day, gender, age, country_code, platform, placement)"},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "Metrics to include (e.g. spend, impressions, clicks, ctr, cpc, cpm, conversion, cost_per_conversion, reach, complete_payment_roas)"},
                    "date_range": {"type": "string", "enum": ["today", "yesterday", "last_3_days", "last_7_days", "last_14_days", "last_30_days", "last_60_days", "last_90_days", "this_month", "last_month"], "description": "Named date range preset"},
                    "start_date": {"type": "string", "description": "Explicit start date (YYYY-MM-DD)"},
                    "end_date": {"type": "string", "description": "Explicit end date (YYYY-MM-DD)"},
                    "campaign_ids": {"type": "array", "items": {"type": "string"}, "description": "Filter by campaign IDs"},
                    "adgroup_ids": {"type": "array", "items": {"type": "string"}, "description": "Filter by ad group IDs"},
                    "ad_ids": {"type": "array", "items": {"type": "string"}, "description": "Filter by ad IDs"},
                    "campaign_status": {"type": "string", "enum": ["STATUS_ALL", "STATUS_NOT_DELETE", "STATUS_DELIVERY_OK", "STATUS_DISABLE", "STATUS_DELETE", "STATUS_NOT_DELIVERY", "STATUS_TIME_DONE", "STATUS_CLOSED", "STATUS_FROZEN", "STATUS_RF_CLOSED"], "description": "Filter by campaign status (e.g. STATUS_DELIVERY_OK for active only)"},
                    "objective_type": {"type": "string", "enum": ["REACH", "RF_REACH", "TRAFFIC", "ENGAGEMENT", "APP_PROMOTION", "LEAD_GENERATION", "PRODUCT_SALES"], "description": "Filter by campaign objective type"},
                    "filtering": {"type": "array", "description": "Explicit filtering array for advanced use"},
                    "task_id": {"type": "string", "description": "Task ID for check_task/download_task"},
                    "api_report_type": {"type": "string", "enum": ["BASIC", "AUDIENCE", "CATALOG", "PLAYABLE"], "description": "TikTok API report type for async_report"},
                },
                "required": ["report_type"],
            }
        ),
        Tool(
            name="tiktok_audience",
            description="Audience intelligence and management. Actions: list (list audiences), estimate_reach (get targeting intelligence — available regions, bid recommendations for audience planning; NOTE: exact reach numbers are only in TikTok Ads Manager UI), create_crm (CRM audience), create_lookalike, create_engagement (rule-based audience), delete. Write actions require confirm=true.",
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "estimate_reach", "create_crm", "create_lookalike", "create_engagement", "delete"],
                        "description": "Action to perform"
                    },
                    "placements": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Placements for estimate_reach (default: ['PLACEMENT_TIKTOK']). Options: PLACEMENT_TIKTOK, PLACEMENT_PANGLE, PLACEMENT_TOPBUZZ, etc.",
                    },
                    "objective_type": {
                        "type": "string",
                        "enum": ["REACH", "TRAFFIC", "VIDEO_VIEWS", "ENGAGEMENT", "APP_PROMOTION", "LEAD_GENERATION", "PRODUCT_SALES"],
                        "description": "Campaign objective for estimate_reach (default: TRAFFIC)",
                    },
                    "location_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Location/country IDs for estimate_reach. Use tiktok_intelligence action=regions to get valid IDs (e.g., '6252001' for US, '2635167' for UK).",
                    },
                    "age_groups": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["AGE_13_17", "AGE_18_24", "AGE_25_34", "AGE_35_44", "AGE_45_54", "AGE_55_100"]},
                        "description": "Age groups for estimate_reach",
                    },
                    "gender": {
                        "type": "string",
                        "enum": ["GENDER_MALE", "GENDER_FEMALE", "GENDER_UNLIMITED"],
                        "description": "Gender targeting for estimate_reach (default: unlimited)",
                    },
                    "languages": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Language codes for estimate_reach",
                    },
                    "interest_category_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Interest category IDs for estimate_reach. Use tiktok_intelligence action=interests to get valid IDs.",
                    },
                    "operating_systems": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["ANDROID", "IOS"]},
                        "description": "OS targeting for estimate_reach",
                    },
                    "params": {"type": "object", "description": "Parameters for create actions (custom_audience_name, source_audience_ids, rules, etc.)"},
                    "custom_audience_ids": {"type": "array", "items": {"type": "string"}, "description": "Audience IDs for list (filter) or delete"},
                    "confirm": {"type": "boolean", "description": "Set to true to execute write operations"},
                    "page": {"type": "integer", "description": "Page number"},
                    "page_size": {"type": "integer", "description": "Page size"},
                },
                "required": ["action"],
            }
        ),
        Tool(
            name="tiktok_creative",
            description="Creative and asset management. Actions: list_videos, search_videos, list_images, list_creatives (filterable by status, campaign_ids, adgroup_ids), list_tt_posts (list TikTok posts from a connected identity for Spark Ads — use tiktok_entity_get identities first to get identity_id), upload_image, upload_video (by URL), spark_authorize (generate/renew Spark Ad auth), spark_status (check auth code status), generate_ad_text (AI ad copy suggestions — requires adgroup_id, not available for US/CA advertisers). Upload actions require confirm=true.",
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_videos", "search_videos", "list_images", "list_creatives", "list_tt_posts", "upload_image", "upload_video", "spark_authorize", "spark_status", "generate_ad_text"],
                        "description": "Action to perform"
                    },
                    "video_ids": {"type": "array", "items": {"type": "string"}, "description": "Video IDs for list_videos"},
                    "image_ids": {"type": "array", "items": {"type": "string"}, "description": "Image IDs for list_images"},
                    "keyword": {"type": "string", "description": "Search keyword for search_videos"},
                    "status": {"type": "string", "enum": ["STATUS_ALL", "STATUS_NOT_DELETE", "STATUS_DELIVERY_OK", "STATUS_DISABLE", "STATUS_DELETE"], "description": "Filter list_creatives by status"},
                    "campaign_ids": {"type": "array", "items": {"type": "string"}, "description": "Filter list_creatives by campaign IDs"},
                    "adgroup_ids": {"type": "array", "items": {"type": "string"}, "description": "Filter list_creatives by ad group IDs"},
                    "identity_id": {"type": "string", "description": "Identity ID for list_tt_posts (get from tiktok_entity_get entity_type='identities')"},
                    "identity_type": {"type": "string", "enum": ["TT_USER", "BC_AUTH_TT"], "description": "Identity type for list_tt_posts (default: BC_AUTH_TT)"},
                    "identity_authorized_bc_id": {"type": "string", "description": "Business Center ID that authorized this identity (get from identities response field 'identity_authorized_bc_id'). Required for BC_AUTH_TT identities."},
                    "cursor": {"type": "integer", "description": "Cursor for pagination of list_tt_posts (from metadata.next_cursor in previous response)"},
                    "start_date": {"type": "string", "description": "Filter list_tt_posts by start date (YYYY-MM-DD). Auto-paginates through all results when set."},
                    "end_date": {"type": "string", "description": "Filter list_tt_posts by end date (YYYY-MM-DD). Auto-paginates through all results when set."},
                    "image_path": {"type": "string", "description": "Local file path for upload_image (use image_path OR image_url)"},
                    "image_url": {"type": "string", "description": "URL for upload_image by URL (use image_path OR image_url)"},
                    "video_url": {"type": "string", "description": "URL for upload_video"},
                    "video_name": {"type": "string", "description": "Video name for upload_video"},
                    "tiktok_item_id": {"type": "string", "description": "TikTok item ID for spark_authorize"},
                    "authorized_days": {"type": "integer", "description": "Authorization days for spark_authorize (default: 30)"},
                    "params": {"type": "object", "description": "Parameters for spark_authorize (tiktok_item_id, authorized_days) — can also pass these at top level"},
                    "auth_codes": {"type": "array", "items": {"type": "string"}, "description": "Auth codes for spark_status"},
                    "confirm": {"type": "boolean", "description": "Set to true to execute upload/authorize operations"},
                    "page": {"type": "integer", "description": "Page number"},
                    "page_size": {"type": "integer", "description": "Page size"},
                    "upload_type": {"type": "string", "description": "Upload type for images"},
                    "adgroup_id": {"type": "string", "description": "Ad group ID for generate_ad_text (required)"},
                    "language": {"type": "string", "description": "Language code for generate_ad_text (default: EN, uppercase)"},
                    "num_results": {"type": "integer", "description": "Number of suggestions for generate_ad_text (max 10)"},
                    "brand_name": {"type": "string", "description": "Brand name for generate_ad_text"},
                    "keywords": {"type": "array", "items": {"type": "string"}, "description": "Keywords for generate_ad_text"},
                },
                "required": ["action"],
            }
        ),
        Tool(
            name="tiktok_comment",
            description="Comment management for TikTok Ads. Actions: list_comments (requires search_field, search_value, start_time, end_time), reply_comment (requires confirm=true), hide_comment (requires confirm=true).",
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_comments", "reply_comment", "hide_comment"],
                        "description": "Action to perform"
                    },
                    "search_field": {"type": "string", "enum": ["ADGROUP_ID", "AD_ID"], "description": "Search by ADGROUP_ID or AD_ID for list_comments"},
                    "search_value": {"type": "string", "description": "The adgroup or ad ID value for list_comments"},
                    "start_time": {"type": "string", "description": "Start date (YYYY-MM-DD) for list_comments"},
                    "end_time": {"type": "string", "description": "End date (YYYY-MM-DD) for list_comments"},
                    "ad_id": {"type": "string", "description": "Ad ID for reply_comment/hide_comment"},
                    "comment_id": {"type": "string", "description": "Comment ID for reply_comment"},
                    "comment_ids": {"type": "array", "items": {"type": "string"}, "description": "Comment IDs for hide_comment"},
                    "text": {"type": "string", "description": "Reply text for reply_comment"},
                    "hidden": {"type": "boolean", "description": "True to hide, false to unhide (default: true)"},
                    "status": {"type": "string", "description": "Filter comments by status for list_comments"},
                    "confirm": {"type": "boolean", "description": "Set to true to execute write operations"},
                    "page": {"type": "integer", "description": "Page number"},
                    "page_size": {"type": "integer", "description": "Page size"},
                },
                "required": ["action"],
            }
        ),
        Tool(
            name="tiktok_intelligence",
            description="Cross-system analysis, optimization insights, and targeting intelligence. Analysis: funnel_overview, anomaly_check, optimization_actions, scaling_readiness. Targeting: interests (browse/search interest categories — use keyword to filter, e.g. 'apparel'), regions (get targetable locations with location_ids — filter by country_code, level, keyword), action_categories (behavioral targeting options like 'followed comedy creators').",
            inputSchema={
                "type": "object",
                "properties": {
                    "analysis_type": {
                        "type": "string",
                        "enum": ["funnel_overview", "anomaly_check", "optimization_actions", "scaling_readiness", "interests", "regions", "action_categories"],
                        "description": "Type of analysis or targeting data to retrieve"
                    },
                    "date_range": {"type": "string", "enum": ["today", "yesterday", "last_3_days", "last_7_days", "last_14_days", "last_30_days"], "description": "Date range for analysis (default: last_7_days)"},
                    "campaign_ids": {"type": "array", "items": {"type": "string"}, "description": "Filter by campaign IDs (for scaling_readiness)"},
                    "threshold": {"type": "number", "description": "Anomaly detection threshold as decimal (default: 0.3 = 30%)"},
                    "keyword": {"type": "string", "description": "Search keyword for interests or regions (e.g., 'apparel', 'United States')"},
                    "language": {"type": "string", "description": "Language code for interests (default: 'en')"},
                    "objective_type": {"type": "string", "enum": ["REACH", "TRAFFIC", "VIDEO_VIEWS", "ENGAGEMENT", "APP_PROMOTION", "LEAD_GENERATION", "PRODUCT_SALES"], "description": "Objective type for regions (default: TRAFFIC)"},
                    "placements": {"type": "array", "items": {"type": "string"}, "description": "Placements for regions/interests (default: ['PLACEMENT_TIKTOK'])"},
                    "level": {"type": "string", "enum": ["COUNTRY", "PROVINCE", "CITY"], "description": "Region level filter (default: COUNTRY)"},
                    "country_code": {"type": "string", "description": "Filter regions by country code (e.g., 'US', 'GB', 'DE')"},
                },
                "required": ["analysis_type"],
            }
        ),
    ])

    return tools


def _sanitize_response(data: Any) -> Any:
    """Recursively stringify numeric ID-like fields in API response data.

    TikTok's API *usually* returns IDs as strings, but some endpoints
    return them as JSON numbers.  When the MCP client reads those numbers
    and later sends them back as arguments, large 18-19 digit IDs can
    lose precision (int → float → scientific notation).  By stringifying
    them on the way *out*, we ensure the client always receives strings.
    """
    _ID_SUFFIXES = ("_id", "_ids", "_code", "_codes")
    _EXTRA_ID_KEYS = {
        "identity_authorized_bc_id", "advertiser_id", "campaign_id",
        "adgroup_id", "ad_id", "pixel_code", "bc_id", "identity_id",
        "catalog_id", "form_id", "task_id", "comment_id", "event_id",
    }

    if isinstance(data, dict):
        result = {}
        for key, val in data.items():
            is_id_field = key in _EXTRA_ID_KEYS or key.endswith(_ID_SUFFIXES)
            if is_id_field and isinstance(val, (int, float)):
                result[key] = str(int(val))
            elif is_id_field and isinstance(val, list):
                result[key] = [str(int(v)) if isinstance(v, (int, float)) else v for v in val]
            else:
                result[key] = _sanitize_response(val)
        return result
    elif isinstance(data, list):
        return [_sanitize_response(item) for item in data]
    return data


def _sanitize_arguments(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce numeric values to strings for ID-like fields.

    MCP clients (especially LLM agents) sometimes send large TikTok IDs as
    JSON numbers instead of strings.  Python's ``json.loads`` preserves
    precision for integers, so ``int → str`` is lossless.  Floats (from
    scientific-notation JSON like ``7.61e+18``) have already lost precision;
    we convert them but log a warning.
    """
    # Fields that should always be strings (IDs, codes, etc.)
    _ID_FIELDS = {
        "advertiser_id", "campaign_id", "adgroup_id", "ad_id", "pixel_code",
        "bc_id", "identity_id", "identity_authorized_bc_id", "comment_id",
        "form_id", "task_id", "catalog_id", "search_value", "event_id",
    }
    _ID_LIST_FIELDS = {
        "campaign_ids", "adgroup_ids", "ad_ids", "comment_ids", "video_ids",
        "auth_codes", "custom_audience_ids",
    }
    sanitized = dict(arguments)
    for key, val in sanitized.items():
        if key in _ID_FIELDS and isinstance(val, (int, float)):
            if isinstance(val, float):
                logger.warning(
                    f"Argument '{key}' received as float ({val}) — precision may be lost. "
                    "Callers should pass TikTok IDs as strings."
                )
            sanitized[key] = str(int(val))
        elif key in _ID_LIST_FIELDS and isinstance(val, list):
            sanitized[key] = [
                str(int(v)) if isinstance(v, (int, float)) else v for v in val
            ]
    return sanitized


@app.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls for TikTok Ads operations."""
    try:
        arguments = _sanitize_arguments(arguments)
        result = None
        
        # Authentication tools (always available)
        if name == "tiktok_ads_login":
            force_reauth = arguments.get("force_reauth")
            result =  await tiktok_server.start_oauth_flow(force_reauth=force_reauth)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        elif name == "tiktok_ads_complete_auth":
            auth_code = arguments.get("auth_code")
            result = await tiktok_server.complete_oauth(auth_code)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        elif name == "tiktok_ads_auth_status":
            result =  await tiktok_server.get_auth_status()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        elif name == "tiktok_ads_switch_ad_account":
            advertiser_id = arguments.get("advertiser_id")
            result = await tiktok_server.switch_ad_account(advertiser_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # Check if authenticated for other tools
        if not tiktok_server.client or not tiktok_server.is_authenticated:
            return [TextContent(
                type="text",
                text="Error: Not authenticated with TikTok Ads API. Please use the 'tiktok_ads_login' tool first." + str(tiktok_server.client) + str(tiktok_server.is_authenticated)
            )]
        
        # Campaign management tools
        if name == "tiktok_ads_get_campaigns":
            result = await tiktok_server.campaign_tools.get_campaigns(**arguments)
        elif name == "tiktok_ads_get_campaign_details":
            result = await tiktok_server.campaign_tools.get_campaign_details(**arguments)
        elif name == "tiktok_ads_create_campaign":
            result = await tiktok_server.campaign_tools.create_campaign(**arguments)
        elif name == "tiktok_ads_get_adgroups":
            result = await tiktok_server.campaign_tools.get_adgroups(**arguments)
        elif name == "tiktok_ads_get_adgroup_details":
            result = await tiktok_server.campaign_tools.get_adgroup_details(**arguments)
        elif name == "tiktok_ads_get_ads":
            result = await tiktok_server.campaign_tools.get_ads(**arguments)
        elif name == "tiktok_ads_get_ad_details":
            result = await tiktok_server.campaign_tools.get_ad_details(**arguments)
        elif name == "tiktok_ads_create_adgroup":
            result = await tiktok_server.campaign_tools.create_adgroup(**arguments)
            
        # Performance tools
        elif name == "tiktok_ads_get_campaign_performance":
            result = await tiktok_server.performance_tools.get_campaign_performance(**arguments)
        elif name == "tiktok_ads_get_adgroup_performance":
            result = await tiktok_server.performance_tools.get_adgroup_performance(**arguments)
            
        # Creative tools
        elif name == "tiktok_ads_get_ad_creatives":
            result = await tiktok_server.creative_tools.get_ad_creatives(**arguments)
        elif name == "tiktok_ads_upload_image":
            result = await tiktok_server.creative_tools.upload_image(**arguments)
            
        # Audience tools
        elif name == "tiktok_ads_get_custom_audiences":
            result = await tiktok_server.audience_tools.get_custom_audiences(**arguments)
        elif name == "tiktok_ads_get_targeting_options":
            result = await tiktok_server.audience_tools.get_targeting_options(**arguments)
            
        # Reporting tools
        elif name == "tiktok_ads_generate_report":
            result = await tiktok_server.reporting_tools.generate_report(**arguments)
        elif name == "tiktok_ads_generate_quick_report":
            result = await tiktok_server.reporting_tools.generate_quick_report(**arguments)

        # ── New consolidated tools ──────────────────────────────────────
        elif name == "tiktok_entity_get":
            result = await tiktok_server.entity_get_tool.execute(arguments)
        elif name == "tiktok_entity_manage":
            result = await tiktok_server.entity_manage_tool.execute(arguments)
        elif name == "tiktok_report":
            result = await tiktok_server.report_tool.execute(arguments)
        elif name == "tiktok_audience":
            result = await tiktok_server.audience_tool.execute(arguments)
        elif name == "tiktok_creative":
            result = await tiktok_server.creative_tool.execute(arguments)
        elif name == "tiktok_comment":
            result = await tiktok_server.comment_tool.execute(arguments)
        elif name == "tiktok_intelligence":
            result = await tiktok_server.intelligence_tool.execute(arguments)

        else:
            return [TextContent(
                type="text",
                text=f"Error: Unknown tool '{name}'"
            )]
        
        return [TextContent(
            type="text",
            text=json.dumps(_sanitize_response(result))
        )]

    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(
            type="text",
            text=f"Error executing {name}: {str(e)}"
        )]


async def main(transport: str = "stdio", port: int = 8000):
    """Main entry point for the TikTok Ads MCP server."""
    try:
        # Initialize the server
        await tiktok_server.initialize()

        if transport == "http":
            from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
            from starlette.applications import Starlette
            from starlette.routing import Mount
            import uvicorn

            session_manager = StreamableHTTPSessionManager(app=app)

            starlette_app = Starlette(
                routes=[Mount("/mcp", app=session_manager.handle_request)],
            )

            logger.info(f"Starting HTTP transport on port {port}")
            config = uvicorn.Config(starlette_app, host="0.0.0.0", port=port)
            server = uvicorn.Server(config)
            async with session_manager.run():
                await server.serve()
        else:
            # Default: stdio transport
            async with stdio_server() as (read_stream, write_stream):
                await app.run(
                    read_stream,
                    write_stream,
                    InitializationOptions(
                        server_name="tiktok-ads-mcp",
                        server_version="1.0.0",
                        capabilities=ServerCapabilities(
                            tools=ToolsCapability(listChanged=True),
                            logging=LoggingCapability()
                        )
                    )
                )
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        raise


def run():
    """Sync entry point for the console script."""
    parser = argparse.ArgumentParser(description="TikTok Ads MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="Transport type (default: stdio)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP transport (default: 8000)",
    )
    args = parser.parse_args()
    asyncio.run(main(transport=args.transport, port=args.port))


if __name__ == "__main__":
    run()