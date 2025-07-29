"""
Configuration Management

Utilities for loading and managing configuration files with
environment variable substitution and validation.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import re


class ConfigLoader:
    """
    Configuration loader with environment variable substitution.
    
    Supports loading YAML configuration files with environment variable
    substitution using the format ${VAR_NAME:-default_value}.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the configuration loader.
        
        Args:
            config_dir: Directory containing configuration files.
                       Defaults to the directory of this file.
        """
        if config_dir is None:
            config_dir = Path(__file__).parent
        self.config_dir = Path(config_dir)
    
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """
        Load a configuration file with environment variable substitution.
        
        Args:
            config_name: Name of the configuration file (without .yaml extension)
            
        Returns:
            Dictionary containing the configuration
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML is invalid
        """
        config_path = self.config_dir / f"{config_name}.yaml"
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Substitute environment variables
        content = self._substitute_env_vars(content)
        
        # Parse YAML
        config = yaml.safe_load(content)
        
        return config
    
    def _substitute_env_vars(self, content: str) -> str:
        """
        Substitute environment variables in the format ${VAR_NAME:-default}.
        
        Args:
            content: String content with environment variable placeholders
            
        Returns:
            String with environment variables substituted
        """
        # Pattern to match ${VAR_NAME:-default_value} or ${VAR_NAME}
        pattern = r'\$\{([^}]+)\}'
        
        def replace_var(match):
            var_expr = match.group(1)
            
            if ':-' in var_expr:
                var_name, default_value = var_expr.split(':-', 1)
                return os.getenv(var_name.strip(), default_value)
            else:
                var_name = var_expr.strip()
                value = os.getenv(var_name)
                if value is None:
                    raise ValueError(f"Environment variable {var_name} is required but not set")
                return value
        
        return re.sub(pattern, replace_var, content)


# Global configuration loader instance
config_loader = ConfigLoader()


def load_database_config() -> Dict[str, Any]:
    """Load database configuration."""
    return config_loader.load_config('database')


def load_kafka_topics() -> Dict[str, Any]:
    """Load Kafka topics configuration."""
    return config_loader.load_config('kafka_topics')


def load_services_config() -> Dict[str, Any]:
    """Load external services configuration."""
    return config_loader.load_config('services')


def get_topic_name(category: str, topic: str) -> str:
    """
    Get a topic name from the Kafka topics configuration.
    
    Args:
        category: Topic category (e.g., 'data_ingestion', 'processing')
        topic: Topic name within the category
        
    Returns:
        Full topic name
        
    Raises:
        KeyError: If the topic is not found
    """
    topics_config = load_kafka_topics()
    
    if category not in topics_config:
        raise KeyError(f"Topic category '{category}' not found")
    
    if topic not in topics_config[category]:
        raise KeyError(f"Topic '{topic}' not found in category '{category}'")
    
    return topics_config[category][topic]
