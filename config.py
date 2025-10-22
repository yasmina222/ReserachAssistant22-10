"""
Protocol Education CI System - Configuration Module
Handles all system configuration, API keys, and model selection
"""

import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_ORG_ID = os.getenv('OPENAI_ORG_ID', None)

# Model Selection Strategy (optimized for $150/month budget)
MODELS = {
    'html_extraction': 'gpt-4o',  # Best for HTML/vision tasks
    'pdf_extraction': 'gpt-4o-mini',  # Cost-effective for PDFs
    'pdf_text_only': 'gpt-3.5-turbo',  # Cheapest for text-only PDFs
    'fallback': 'o1-mini',  # When GPT-4o struggles
    'embeddings': 'text-embedding-3-small',  # For competitor matching
    'batch_processing': 'gpt-4o-mini'  # For bulk operations
}

# Rate Limits (requests per minute)
RATE_LIMITS = {
    'gpt-4o': 30,
    'gpt-4o-mini': 60,
    'gpt-3.5-turbo': 120,
    'o1-mini': 20
}

# Budget Controls
DAILY_BUDGET_USD = 5.0  # $150/month ÷ 30 days
MAX_TOKENS_PER_REQUEST = {
    'gpt-4o': 4000,
    'gpt-4o-mini': 8000,
    'gpt-3.5-turbo': 4000,
    'o1-mini': 4000
}

# Cache Configuration
CACHE_DIR = 'cache'
CACHE_TTL_HOURS = 24
ENABLE_CACHE = True

# Web Scraping Configuration
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
]
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_DELAY = 2

# Verification Settings
SMTP_TIMEOUT = 5
PHONE_REGIONS = ['GB', 'UK']
MIN_CONFIDENCE_SCORE = 0.7

# School Data Sources
SCHOOL_SEARCH_DOMAINS = [
    'www.google.co.uk',
    'www.bing.com',
    'find-school-performance-data.service.gov.uk'
]

# Contact Patterns
EMAIL_PATTERNS = [
    '{firstname}.{lastname}@{domain}',
    '{f}.{lastname}@{domain}',
    '{firstname}{lastname}@{domain}',
    '{f}{lastname}@{domain}',
    'admin@{domain}',
    'office@{domain}'
]

# Competitor Keywords
COMPETITOR_KEYWORDS = {
    'zen_educate': ['zen educate', 'zeneducate', 'zen-educate'],
    'hays': ['hays education', 'hays teaching'],
    'supply_desk': ['supply desk', 'supplydesk'],
    'teach_first': ['teach first', 'teachfirst'],
    'protocol': ['protocol education', 'protocol recruitment']
}

# Output Configuration
OUTPUT_DIR = 'outputs'
EXPORT_FORMATS = ['csv', 'xlsx', 'json']

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FILE = 'protocol_ci.log'

# Create necessary directories
for directory in [CACHE_DIR, OUTPUT_DIR]:
    os.makedirs(directory, exist_ok=True)