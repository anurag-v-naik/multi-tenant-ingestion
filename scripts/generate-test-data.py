#!/usr/bin/env python3
"""Generate test data for development environment"""

import random
import uuid
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def generate_test_organizations():
    """Generate sample organizations"""
    return [
        {"name": "finance", "display_name": "Finance Department"},
        {"name": "retail", "display_name": "Retail Division"},
        {"name": "healthcare", "display_name": "Healthcare Division"}
    ]

def generate_test_pipelines():
    """Generate sample pipelines"""
    # Implementation here
    pass

if __name__ == "__main__":
    main()