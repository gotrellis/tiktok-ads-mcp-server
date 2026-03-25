#!/usr/bin/env python3
"""Entry point script for TikTok Ads MCP Server."""

import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

try:
    from tiktok_ads_mcp.server import run

    if __name__ == "__main__":
        run()

except Exception as e:
    print(f"Error starting TikTok Ads MCP server: {e}", file=sys.stderr)
    sys.exit(1)
