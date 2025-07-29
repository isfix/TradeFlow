#!/usr/bin/env python3
"""
Trading Platform Startup Script

This script provides a clear entry point for users and directs them
to use the proper CLI for managing the microservices architecture.
"""

import sys
from pathlib import Path


def main():
    """Main entry point that directs users to the CLI."""
    print("🏦 Institutional Trading Platform")
    print("=" * 50)
    print()
    print("⚠️  IMPORTANT: This platform uses a microservices architecture.")
    print("   Each service runs in its own Docker container.")
    print()
    print("✅ To start the platform, use the CLI:")
    print("   python cli.py start")
    print()
    print("📊 To check service status:")
    print("   python cli.py status")
    print()
    print("📋 To view logs:")
    print("   python cli.py logs")
    print()
    print("🛑 To stop the platform:")
    print("   python cli.py stop")
    print()
    print("❓ For all available commands:")
    print("   python cli.py --help")
    print()
    print("📚 For detailed documentation, see README.md")
    print()
    print("=" * 50)
    print("🚀 Ready to start? Run: python cli.py start --build")


if __name__ == "__main__":
    main()
