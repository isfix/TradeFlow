"""
Setup script for the Institutional Trading Platform.
"""

from setuptools import setup, find_packages
import os

# Read the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

# Version
VERSION = "1.0.0"

setup(
    name="institutional-trading-platform",
    version=VERSION,
    author="Trading Platform Team",
    author_email="team@trading-platform.com",
    description="Institutional-grade algorithmic trading platform with real-time data processing and advanced analytics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/trading-platform",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Environment :: Console",
        "Framework :: AsyncIO",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.4.0",
            "pre-commit>=3.3.0",
        ],
        "jupyter": [
            "jupyter>=1.0.0",
            "jupyterlab>=4.0.0",
            "ipywidgets>=8.0.0",
            "matplotlib>=3.7.0",
            "seaborn>=0.12.0",
            "plotly>=5.15.0",
        ],
        "brokers": [
            "alpaca-trade-api>=3.0.0",
            "ib-insync>=0.9.86",
        ],
        "cloud": [
            "boto3>=1.28.0",
            "google-cloud-storage>=2.10.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "trading-platform=trading_platform.cli:main",
            "trading-platform-start=trading_platform.start:main",
        ],
    },
    include_package_data=True,
    package_data={
        "trading_platform": [
            "configs/*.yaml",
            "configs/example.*.yaml",
        ],
    },
    zip_safe=False,
    keywords="trading, algorithmic, finance, institutional, real-time, machine-learning",
    project_urls={
        "Bug Reports": "https://github.com/your-org/trading-platform/issues",
        "Source": "https://github.com/your-org/trading-platform",
        "Documentation": "https://trading-platform.readthedocs.io/",
    },
)
