#!/usr/bin/env python3
"""
Trading Platform CLI

Command-line interface for managing and interacting with the deployed
microservices-based trading platform.

This CLI works with the containerized services and does NOT run services
directly in the current process.
"""

import argparse
import json
import sys
import subprocess
import time
import requests
from typing import Dict, List, Any, Optional
from pathlib import Path
import docker
import yaml
from datetime import datetime


class TradingPlatformCLI:
    """
    Command-line interface for the trading platform.
    
    Provides commands to manage, monitor, and interact with the
    microservices-based trading platform.
    """
    
    def __init__(self):
        """Initialize the CLI."""
        self.project_root = Path(__file__).parent
        self.docker_client = docker.from_env()
        self.compose_file = self.project_root / "docker" / "docker-compose.yml"
        
        # Service endpoints (when running locally)
        self.service_endpoints = {
            "market-data-service": "http://localhost:8001",
            "news-service": "http://localhost:8002",
            "feature-engineering-service": "http://localhost:8003",
            "sentiment-analysis-service": "http://localhost:8004",
            "signal-aggregation-service": "http://localhost:8005",
            "risk-management-service": "http://localhost:8006"
        }
    
    def start_platform(self, detached: bool = True, build: bool = False) -> bool:
        """Start the trading platform using Docker Compose."""
        print("🚀 Starting Trading Platform...")
        
        if not self.compose_file.exists():
            print(f"❌ Docker Compose file not found: {self.compose_file}")
            return False
        
        cmd = ["docker-compose", "-f", str(self.compose_file), "up"]
        
        if detached:
            cmd.append("-d")
        
        if build:
            cmd.append("--build")
        
        try:
            result = subprocess.run(cmd, cwd=self.project_root, check=True)
            
            if detached:
                print("✅ Trading Platform started successfully")
                print("📊 Use 'python cli.py status' to check service health")
                print("📋 Use 'python cli.py logs' to view service logs")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to start platform: {e}")
            return False
    
    def stop_platform(self) -> bool:
        """Stop the trading platform."""
        print("🛑 Stopping Trading Platform...")
        
        cmd = ["docker-compose", "-f", str(self.compose_file), "down"]
        
        try:
            subprocess.run(cmd, cwd=self.project_root, check=True)
            print("✅ Trading Platform stopped successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to stop platform: {e}")
            return False
    
    def restart_platform(self) -> bool:
        """Restart the trading platform."""
        print("🔄 Restarting Trading Platform...")
        
        if self.stop_platform():
            time.sleep(2)
            return self.start_platform()
        
        return False
    
    def get_platform_status(self) -> Dict[str, Any]:
        """Get the status of all platform services."""
        try:
            # Get container status
            containers = self.docker_client.containers.list(
                filters={"label": "com.docker.compose.project=trading_platform"}
            )
            
            status = {
                "timestamp": datetime.utcnow().isoformat(),
                "services": {},
                "overall_health": "healthy"
            }
            
            for container in containers:
                service_name = container.labels.get("com.docker.compose.service", "unknown")
                
                # Get container health
                container_status = {
                    "status": container.status,
                    "health": "unknown",
                    "uptime": None,
                    "image": container.image.tags[0] if container.image.tags else "unknown"
                }
                
                # Check health if container is running
                if container.status == "running":
                    try:
                        # Try to get health check status
                        health_status = container.attrs.get("State", {}).get("Health", {}).get("Status")
                        if health_status:
                            container_status["health"] = health_status
                        else:
                            container_status["health"] = "running"
                        
                        # Calculate uptime
                        started_at = container.attrs.get("State", {}).get("StartedAt")
                        if started_at:
                            container_status["uptime"] = started_at
                        
                    except Exception:
                        container_status["health"] = "unknown"
                else:
                    status["overall_health"] = "unhealthy"
                
                status["services"][service_name] = container_status
            
            return status
            
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "overall_health": "error"
            }
    
    def show_status(self, detailed: bool = False) -> None:
        """Display platform status."""
        print("📊 Trading Platform Status")
        print("=" * 50)
        
        status = self.get_platform_status()
        
        if "error" in status:
            print(f"❌ Error getting status: {status['error']}")
            return
        
        print(f"Overall Health: {status['overall_health'].upper()}")
        print(f"Last Updated: {status['timestamp']}")
        print()
        
        if not status["services"]:
            print("⚠️  No services found. Platform may not be running.")
            print("💡 Use 'python cli.py start' to start the platform")
            return
        
        # Display service status
        for service_name, service_info in status["services"].items():
            health_icon = {
                "healthy": "✅",
                "running": "🟢",
                "unhealthy": "❌",
                "unknown": "⚠️"
            }.get(service_info["health"], "❓")
            
            print(f"{health_icon} {service_name}")
            print(f"   Status: {service_info['status']}")
            print(f"   Health: {service_info['health']}")
            
            if detailed:
                print(f"   Image: {service_info['image']}")
                if service_info['uptime']:
                    print(f"   Started: {service_info['uptime']}")
            
            print()
    
    def show_logs(self, service: Optional[str] = None, follow: bool = False, tail: int = 100) -> None:
        """Show service logs."""
        cmd = ["docker-compose", "-f", str(self.compose_file), "logs"]
        
        if follow:
            cmd.append("-f")
        
        cmd.extend(["--tail", str(tail)])
        
        if service:
            cmd.append(service)
        
        try:
            subprocess.run(cmd, cwd=self.project_root)
        except KeyboardInterrupt:
            print("\n📋 Log viewing stopped")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error viewing logs: {e}")
    
    def scale_service(self, service: str, replicas: int) -> bool:
        """Scale a specific service."""
        print(f"⚖️  Scaling {service} to {replicas} replicas...")
        
        cmd = [
            "docker-compose", "-f", str(self.compose_file),
            "up", "-d", "--scale", f"{service}={replicas}", service
        ]
        
        try:
            subprocess.run(cmd, cwd=self.project_root, check=True)
            print(f"✅ {service} scaled to {replicas} replicas")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to scale {service}: {e}")
            return False
    
    def exec_command(self, service: str, command: str) -> None:
        """Execute a command in a service container."""
        cmd = [
            "docker-compose", "-f", str(self.compose_file),
            "exec", service, "sh", "-c", command
        ]
        
        try:
            subprocess.run(cmd, cwd=self.project_root)
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to execute command: {e}")
    
    def show_config(self) -> None:
        """Show the current Docker Compose configuration."""
        cmd = ["docker-compose", "-f", str(self.compose_file), "config"]
        
        try:
            subprocess.run(cmd, cwd=self.project_root)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error showing config: {e}")
    
    def pull_images(self) -> bool:
        """Pull the latest images for all services."""
        print("📥 Pulling latest images...")
        
        cmd = ["docker-compose", "-f", str(self.compose_file), "pull"]
        
        try:
            subprocess.run(cmd, cwd=self.project_root, check=True)
            print("✅ Images pulled successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to pull images: {e}")
            return False
    
    def cleanup(self, volumes: bool = False) -> bool:
        """Clean up stopped containers and unused resources."""
        print("🧹 Cleaning up...")
        
        # Stop and remove containers
        cmd = ["docker-compose", "-f", str(self.compose_file), "down"]
        
        if volumes:
            cmd.append("-v")
            print("⚠️  This will also remove volumes and data!")
        
        try:
            subprocess.run(cmd, cwd=self.project_root, check=True)
            
            # Clean up unused Docker resources
            subprocess.run(["docker", "system", "prune", "-f"], check=True)
            
            print("✅ Cleanup completed")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Cleanup failed: {e}")
            return False


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Trading Platform CLI - Manage microservices-based trading platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py start                    # Start the platform
  python cli.py start --build            # Start with rebuild
  python cli.py status                   # Show service status
  python cli.py status --detailed        # Show detailed status
  python cli.py logs                     # Show all logs
  python cli.py logs news-service        # Show specific service logs
  python cli.py logs --follow            # Follow logs in real-time
  python cli.py scale market-data-service 3  # Scale service to 3 replicas
  python cli.py exec postgres psql       # Execute command in container
  python cli.py stop                     # Stop the platform
  python cli.py cleanup --volumes        # Clean up including volumes
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Start command
    start_parser = subparsers.add_parser("start", help="Start the trading platform")
    start_parser.add_argument("--build", action="store_true", help="Rebuild images before starting")
    start_parser.add_argument("--foreground", action="store_true", help="Run in foreground (not detached)")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show platform status")
    status_parser.add_argument("--detailed", action="store_true", help="Show detailed information")
    
    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Show service logs")
    logs_parser.add_argument("service", nargs="?", help="Specific service to show logs for")
    logs_parser.add_argument("--follow", "-f", action="store_true", help="Follow logs in real-time")
    logs_parser.add_argument("--tail", type=int, default=100, help="Number of lines to show")
    
    # Scale command
    scale_parser = subparsers.add_parser("scale", help="Scale a service")
    scale_parser.add_argument("service", help="Service name to scale")
    scale_parser.add_argument("replicas", type=int, help="Number of replicas")
    
    # Exec command
    exec_parser = subparsers.add_parser("exec", help="Execute command in service container")
    exec_parser.add_argument("service", help="Service name")
    exec_parser.add_argument("command", help="Command to execute")
    
    # Other commands
    subparsers.add_parser("stop", help="Stop the trading platform")
    subparsers.add_parser("restart", help="Restart the trading platform")
    subparsers.add_parser("config", help="Show Docker Compose configuration")
    subparsers.add_parser("pull", help="Pull latest images")
    
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up containers and resources")
    cleanup_parser.add_argument("--volumes", action="store_true", help="Also remove volumes")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    cli = TradingPlatformCLI()
    
    # Execute commands
    if args.command == "start":
        success = cli.start_platform(
            detached=not args.foreground,
            build=args.build
        )
        sys.exit(0 if success else 1)
    
    elif args.command == "stop":
        success = cli.stop_platform()
        sys.exit(0 if success else 1)
    
    elif args.command == "restart":
        success = cli.restart_platform()
        sys.exit(0 if success else 1)
    
    elif args.command == "status":
        cli.show_status(detailed=args.detailed)
    
    elif args.command == "logs":
        cli.show_logs(
            service=args.service,
            follow=args.follow,
            tail=args.tail
        )
    
    elif args.command == "scale":
        success = cli.scale_service(args.service, args.replicas)
        sys.exit(0 if success else 1)
    
    elif args.command == "exec":
        cli.exec_command(args.service, args.command)
    
    elif args.command == "config":
        cli.show_config()
    
    elif args.command == "pull":
        success = cli.pull_images()
        sys.exit(0 if success else 1)
    
    elif args.command == "cleanup":
        success = cli.cleanup(volumes=args.volumes)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
