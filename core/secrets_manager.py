"""
Secrets Manager

Secure handling of secrets and sensitive configuration data with support
for multiple backends including environment variables, HashiCorp Vault,
AWS Secrets Manager, and Azure Key Vault.
"""

import os
import logging
import json
import base64
from typing import Dict, Any, Optional, Union
from pathlib import Path
from dataclasses import dataclass
from abc import ABC, abstractmethod

try:
    import hvac  # HashiCorp Vault client
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

try:
    import boto3  # AWS SDK
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.keyvault.secrets import SecretClient
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


@dataclass
class SecretConfig:
    """Configuration for secret management."""
    backend: str  # 'env', 'vault', 'aws', 'azure', 'file'
    vault_url: Optional[str] = None
    vault_token: Optional[str] = None
    vault_mount_point: Optional[str] = None
    aws_region: Optional[str] = None
    azure_vault_url: Optional[str] = None
    encryption_key: Optional[str] = None
    secrets_file_path: Optional[str] = None


class SecretBackend(ABC):
    """Abstract base class for secret backends."""
    
    @abstractmethod
    def get_secret(self, key: str) -> Optional[str]:
        """Get a secret value by key."""
        pass
    
    @abstractmethod
    def set_secret(self, key: str, value: str) -> bool:
        """Set a secret value."""
        pass
    
    @abstractmethod
    def delete_secret(self, key: str) -> bool:
        """Delete a secret."""
        pass
    
    @abstractmethod
    def list_secrets(self) -> list:
        """List available secret keys."""
        pass


class EnvironmentBackend(SecretBackend):
    """Environment variables backend for secrets."""
    
    def __init__(self):
        """Initialize environment backend."""
        self.logger = logging.getLogger(f"{__name__}.EnvironmentBackend")
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from environment variable."""
        return os.getenv(key)
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set environment variable (runtime only)."""
        try:
            os.environ[key] = value
            return True
        except Exception as e:
            self.logger.error(f"Error setting environment variable {key}: {e}")
            return False
    
    def delete_secret(self, key: str) -> bool:
        """Delete environment variable."""
        try:
            if key in os.environ:
                del os.environ[key]
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error deleting environment variable {key}: {e}")
            return False
    
    def list_secrets(self) -> list:
        """List environment variables (filtered for security)."""
        # Only return keys that look like secrets
        secret_patterns = ['_KEY', '_SECRET', '_TOKEN', '_PASSWORD', '_API_KEY']
        return [key for key in os.environ.keys() 
                if any(pattern in key.upper() for pattern in secret_patterns)]


class VaultBackend(SecretBackend):
    """HashiCorp Vault backend for secrets."""
    
    def __init__(self, config: SecretConfig):
        """Initialize Vault backend."""
        if not VAULT_AVAILABLE:
            raise ImportError("hvac package required for Vault backend")
        
        self.logger = logging.getLogger(f"{__name__}.VaultBackend")
        self.config = config
        
        try:
            self.client = hvac.Client(
                url=config.vault_url,
                token=config.vault_token
            )
            
            if not self.client.is_authenticated():
                raise ValueError("Vault authentication failed")
                
            self.mount_point = config.vault_mount_point or 'secret'
            
        except Exception as e:
            self.logger.error(f"Error initializing Vault client: {e}")
            raise
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from Vault."""
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=key,
                mount_point=self.mount_point
            )
            
            if response and 'data' in response and 'data' in response['data']:
                return response['data']['data'].get('value')
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting secret {key} from Vault: {e}")
            return None
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in Vault."""
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path=key,
                secret={'value': value},
                mount_point=self.mount_point
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting secret {key} in Vault: {e}")
            return False
    
    def delete_secret(self, key: str) -> bool:
        """Delete secret from Vault."""
        try:
            self.client.secrets.kv.v2.delete_metadata_and_all_versions(
                path=key,
                mount_point=self.mount_point
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting secret {key} from Vault: {e}")
            return False
    
    def list_secrets(self) -> list:
        """List secrets in Vault."""
        try:
            response = self.client.secrets.kv.v2.list_secrets(
                path='',
                mount_point=self.mount_point
            )
            
            if response and 'data' in response and 'keys' in response['data']:
                return response['data']['keys']
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error listing secrets from Vault: {e}")
            return []


class AWSSecretsBackend(SecretBackend):
    """AWS Secrets Manager backend."""
    
    def __init__(self, config: SecretConfig):
        """Initialize AWS Secrets Manager backend."""
        if not AWS_AVAILABLE:
            raise ImportError("boto3 package required for AWS backend")
        
        self.logger = logging.getLogger(f"{__name__}.AWSSecretsBackend")
        self.config = config
        
        try:
            self.client = boto3.client(
                'secretsmanager',
                region_name=config.aws_region or 'us-east-1'
            )
            
        except Exception as e:
            self.logger.error(f"Error initializing AWS Secrets Manager client: {e}")
            raise
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from AWS Secrets Manager."""
        try:
            response = self.client.get_secret_value(SecretId=key)
            return response.get('SecretString')
            
        except Exception as e:
            self.logger.error(f"Error getting secret {key} from AWS: {e}")
            return None
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in AWS Secrets Manager."""
        try:
            # Try to update existing secret first
            try:
                self.client.update_secret(
                    SecretId=key,
                    SecretString=value
                )
            except self.client.exceptions.ResourceNotFoundException:
                # Create new secret if it doesn't exist
                self.client.create_secret(
                    Name=key,
                    SecretString=value
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting secret {key} in AWS: {e}")
            return False
    
    def delete_secret(self, key: str) -> bool:
        """Delete secret from AWS Secrets Manager."""
        try:
            self.client.delete_secret(
                SecretId=key,
                ForceDeleteWithoutRecovery=True
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting secret {key} from AWS: {e}")
            return False
    
    def list_secrets(self) -> list:
        """List secrets in AWS Secrets Manager."""
        try:
            response = self.client.list_secrets()
            return [secret['Name'] for secret in response.get('SecretList', [])]
            
        except Exception as e:
            self.logger.error(f"Error listing secrets from AWS: {e}")
            return []


class EncryptedFileBackend(SecretBackend):
    """Encrypted file backend for secrets."""
    
    def __init__(self, config: SecretConfig):
        """Initialize encrypted file backend."""
        self.logger = logging.getLogger(f"{__name__}.EncryptedFileBackend")
        self.config = config
        self.secrets_file = Path(config.secrets_file_path or "secrets.enc")
        
        # Initialize encryption
        self.cipher = self._init_encryption(config.encryption_key)
        
        # Load existing secrets
        self.secrets = self._load_secrets()
    
    def _init_encryption(self, key: Optional[str]) -> Fernet:
        """Initialize encryption cipher."""
        if key:
            # Use provided key
            key_bytes = base64.urlsafe_b64decode(key.encode())
        else:
            # Generate key from password (for development)
            password = os.getenv('SECRETS_PASSWORD', 'default_dev_password').encode()
            salt = os.getenv('SECRETS_SALT', 'default_salt').encode()
            
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            key_bytes = base64.urlsafe_b64encode(kdf.derive(password))
        
        return Fernet(key_bytes)
    
    def _load_secrets(self) -> Dict[str, str]:
        """Load secrets from encrypted file."""
        if not self.secrets_file.exists():
            return {}
        
        try:
            with open(self.secrets_file, 'rb') as f:
                encrypted_data = f.read()
            
            decrypted_data = self.cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
            
        except Exception as e:
            self.logger.error(f"Error loading secrets file: {e}")
            return {}
    
    def _save_secrets(self) -> bool:
        """Save secrets to encrypted file."""
        try:
            data = json.dumps(self.secrets).encode()
            encrypted_data = self.cipher.encrypt(data)
            
            with open(self.secrets_file, 'wb') as f:
                f.write(encrypted_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving secrets file: {e}")
            return False
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from encrypted file."""
        return self.secrets.get(key)
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in encrypted file."""
        self.secrets[key] = value
        return self._save_secrets()
    
    def delete_secret(self, key: str) -> bool:
        """Delete secret from encrypted file."""
        if key in self.secrets:
            del self.secrets[key]
            return self._save_secrets()
        return False
    
    def list_secrets(self) -> list:
        """List secrets in encrypted file."""
        return list(self.secrets.keys())


class SecretsManager:
    """
    Centralized secrets management with multiple backend support.
    
    Supports environment variables, HashiCorp Vault, AWS Secrets Manager,
    Azure Key Vault, and encrypted files.
    """
    
    def __init__(self, config: Optional[SecretConfig] = None):
        """
        Initialize secrets manager.
        
        Args:
            config: Secret configuration. If None, uses environment backend.
        """
        self.logger = logging.getLogger(f"{__name__}.SecretsManager")
        
        if config is None:
            config = SecretConfig(backend='env')
        
        self.config = config
        self.backend = self._create_backend(config)
        
        self.logger.info(f"Initialized secrets manager with {config.backend} backend")
    
    def _create_backend(self, config: SecretConfig) -> SecretBackend:
        """Create appropriate backend based on configuration."""
        if config.backend == 'env':
            return EnvironmentBackend()
        elif config.backend == 'vault':
            return VaultBackend(config)
        elif config.backend == 'aws':
            return AWSSecretsBackend(config)
        elif config.backend == 'file':
            return EncryptedFileBackend(config)
        else:
            raise ValueError(f"Unsupported secret backend: {config.backend}")
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value.
        
        Args:
            key: Secret key
            default: Default value if secret not found
            
        Returns:
            Secret value or default
        """
        try:
            value = self.backend.get_secret(key)
            return value if value is not None else default
            
        except Exception as e:
            self.logger.error(f"Error getting secret {key}: {e}")
            return default
    
    def get_secret_required(self, key: str) -> str:
        """
        Get a required secret value.
        
        Args:
            key: Secret key
            
        Returns:
            Secret value
            
        Raises:
            ValueError: If secret is not found
        """
        value = self.get_secret(key)
        if value is None:
            raise ValueError(f"Required secret '{key}' not found")
        return value
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set a secret value."""
        return self.backend.set_secret(key, value)
    
    def delete_secret(self, key: str) -> bool:
        """Delete a secret."""
        return self.backend.delete_secret(key)
    
    def list_secrets(self) -> list:
        """List available secret keys."""
        return self.backend.list_secrets()
    
    def get_database_config(self, db_type: str) -> Dict[str, Any]:
        """
        Get database configuration with secrets resolved.
        
        Args:
            db_type: Database type ('postgres', 'influxdb', 'mongodb', 'redis')
            
        Returns:
            Database configuration dictionary
        """
        if db_type == 'postgres':
            return {
                'host': self.get_secret('POSTGRES_HOST', 'localhost'),
                'port': int(self.get_secret('POSTGRES_PORT', '5432')),
                'database': self.get_secret('POSTGRES_DB', 'trading_platform'),
                'username': self.get_secret('POSTGRES_USER', 'trading_user'),
                'password': self.get_secret_required('POSTGRES_PASSWORD'),
                'ssl_mode': self.get_secret('POSTGRES_SSL_MODE', 'prefer'),
                'max_connections': int(self.get_secret('POSTGRES_MAX_CONNECTIONS', '20'))
            }
        elif db_type == 'influxdb':
            return {
                'host': self.get_secret('INFLUXDB_HOST', 'localhost'),
                'port': int(self.get_secret('INFLUXDB_PORT', '8086')),
                'org': self.get_secret('INFLUXDB_ORG', 'trading_org'),
                'bucket': self.get_secret('INFLUXDB_BUCKET', 'market_data'),
                'token': self.get_secret_required('INFLUXDB_TOKEN'),
                'ssl': self.get_secret('INFLUXDB_SSL', 'false').lower() == 'true'
            }
        elif db_type == 'mongodb':
            return {
                'host': self.get_secret('MONGODB_HOST', 'localhost'),
                'port': int(self.get_secret('MONGODB_PORT', '27017')),
                'database': self.get_secret('MONGODB_DB', 'trading_platform'),
                'username': self.get_secret('MONGODB_USER'),
                'password': self.get_secret('MONGODB_PASSWORD'),
                'auth_source': self.get_secret('MONGODB_AUTH_SOURCE', 'admin'),
                'ssl': self.get_secret('MONGODB_SSL', 'false').lower() == 'true'
            }
        elif db_type == 'redis':
            return {
                'host': self.get_secret('REDIS_HOST', 'localhost'),
                'port': int(self.get_secret('REDIS_PORT', '6379')),
                'password': self.get_secret('REDIS_PASSWORD'),
                'db': int(self.get_secret('REDIS_DB', '0')),
                'ssl': self.get_secret('REDIS_SSL', 'false').lower() == 'true',
                'max_connections': int(self.get_secret('REDIS_MAX_CONNECTIONS', '10'))
            }
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


# Global secrets manager instance
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """Get the global secrets manager instance."""
    global _secrets_manager
    
    if _secrets_manager is None:
        # Determine backend from environment
        backend = os.getenv('SECRETS_BACKEND', 'env')
        
        config = SecretConfig(
            backend=backend,
            vault_url=os.getenv('VAULT_URL'),
            vault_token=os.getenv('VAULT_TOKEN'),
            vault_mount_point=os.getenv('VAULT_MOUNT_POINT'),
            aws_region=os.getenv('AWS_DEFAULT_REGION'),
            azure_vault_url=os.getenv('AZURE_VAULT_URL'),
            encryption_key=os.getenv('ENCRYPTION_KEY'),
            secrets_file_path=os.getenv('SECRETS_FILE_PATH')
        )
        
        _secrets_manager = SecretsManager(config)
    
    return _secrets_manager


def initialize_secrets_manager(config: SecretConfig) -> SecretsManager:
    """Initialize the global secrets manager with custom configuration."""
    global _secrets_manager
    _secrets_manager = SecretsManager(config)
    return _secrets_manager
