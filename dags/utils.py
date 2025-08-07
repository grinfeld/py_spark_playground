"""
Utility functions for portable path handling in Airflow DAGs.
"""

import os
import sys
from pathlib import Path


def add_project_root_to_path():
    """
    Add the project root to Python path in a portable way.
    Works in Docker containers, local development, and Kubernetes.
    
    This ensures that imports like 'from config_manager import config_manager'
    work from any location (root directory, dags directory, etc.).
    """
    # Get the directory containing this file (dags directory)
    dags_dir = Path(__file__).parent.absolute()
    
    # Get the project root (parent of dags directory)
    project_root = dags_dir.parent
    
    # Add to Python path if not already there
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    return str(project_root)


def get_project_root():
    """
    Get the project root directory path.
    """
    dags_dir = Path(__file__).parent.absolute()
    return str(dags_dir.parent)


def get_config_manager():
    """
    Get the config_manager instance in a portable way.
    """
    add_project_root_to_path()
    from config_manager import config_manager
    return config_manager
