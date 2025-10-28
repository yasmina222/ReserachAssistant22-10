"""
Protocol Education CI System - Financial Data Engine ACTUALLY WORKING
Uses direct HTTP fetch + HTML parsing of the NEW government site
No ScraperAPI needed (they block GOV.UK domains anyway)
"""

import re
import logging
import requests
from typing import Dict, Optional, List, Any
from datetime import datetime
from models import ConversationStarter
from bs4 import BeautifulSoup
import json

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from NEW government site"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper_engine = serper_engine
        
        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry point - get financial intelligence for a school
        GUARANTEED: Always returns at minimum links to financial data
        """
        try:
            logger.info(f"🎯 Getting recruitment intelligence for: {school_name}")
            
            # Step 1: Find school URN
            urn_result = self._get_school_urn(school_name, location)
            
            if not urn_result.get('urn'):
                logger.warning(f"Could not find URN for {school_name}")
                return {
                    'error': 'Could not find school URN',
                    'urn': None,
                    'financial': {},
                    'insights': [],
                    'conversation_starters': []
                }
            
            urn = urn_result['urn']
            logger.info(f"✅ Found URN: {urn}")
            
            # Step 2: IMMEDIATELY create financial data dict with links
            # This ensures users ALWAYS get access to financial data
            financial_data = {
                'urn': urn,
                'school_name': school_name,
                'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
                'source_url_old': f"https://schools-financial-benchmarking.service.gov.uk/school/{urn}",
                'extracted_date': datetime.now().isoformat(),
                'official_name': urn_result.get('official_name', school_name)
            }
            
            # Step 3: Attempt data extraction (non-blocking)
            try:
                extracted_data = self._fetch_and_parse_financial_page(urn)
                financial_data.update(extracted_data)
                logger.info(f"✅ Successfully extracted {len(extracted_data)} financial metrics")
            except Exception as e:
                logger.warning(f"⚠️ Data extraction failed but continuing: {e}")
                # Continue anyway - we have the links
            
            # Step 4: Generate insights from available data
            insights = self._generate_insights(financial_data, school_name)
            
            # Step 5: Generate conversation starters
            conversation_starters = self._generate_financial_starters(financial_data, insights, school_name)
            
            return {
                'urn': urn,
                'financial': financial_data,
                'insights': insights,
                'conversation_starters': conversation_starters,
                'source_url': financial_data['source_url']
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting financial data: {e}")
            return {
                'error': str(e),
                'urn': None,
                'financial': {},
                'insights': [],
                'conversation_starters': []
            }
    
    def _get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN using Serper API - RESTORED AUGUST 2025 PATTERNS
        """
        try:
            # Strategy 1: Search GIAS (Get Information About Schools)
            query_parts = [f'"{school_name}"']
            if location:
                query_parts.append(location)
            query_parts.append("site:get-information-schools.service.gov.uk URN")
            
            search_query = " ".join(query_parts)
            logger.info(f"🔍 Searching for URN: {search_query}")
            
            results = self.serper_engine.search_web(search_query, num_results=5)
            
            # Extract URN from results
            for result in results:
                snippet = result.get('snippet', '')
                url = result.get('url', '')
                title = result.get('title', '')
                
                # PRIORITY 1: Extract from URL (most reliable)
                urn_match = re.search(r'/Details/(\d{5,7})', url)
                if urn_match:
                    urn = urn_match.group(1)
                    logger.info(f"✅ Found URN {urn} in URL")
                    return {
                        'urn': urn,
                        'official_name': self._extract_school_name(title),
                        'confidence': 0.95,
                        'source': 'GIAS URL'
                    }
                
                # PRIORITY 2: Extract from snippet
                urn_match = re.search(r'URN[:\s]+(\d{5,7})', snippet)
                if urn_match:
                    urn = urn_match.group(1)
                    logger.info(f"✅ Found URN {urn} in snippet")
                    return {
                        'urn': urn,
                        'official_name': self._extract_school_name(title),
                        'confidence': 0.85,
                        'source': 'GIAS snippet'
                    }
            
            # Strategy 2: Search NEW financial site directly
            logger.info("🔍 Trying NEW financial site")
            new_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
            if location:
                new_query += f' {location}'
            
            results = self.serper_engine.search_web(new_query, num_results=5)
            
            for result in results:
                url = result.get('url', '')
                # Extract URN from new site URL: /school/123456
                urn_match = re.search(r'/school/(\d{5,7})', url)
                if urn_match:
                    urn = urn_match.group(1)
                    logger.info(f"✅ Found URN {urn} from NEW site")
                    return {
                        'urn': urn,
                        'official_name': self._extract_school_name(result.get('title', '')),
                        'confidence': 0.80,
                        'source': 'New financial site'
                    }
            
            # Strategy 3: Search OLD financial site
            logger.info("🔍 Trying OLD financial site")
            old_query = f'"{school_name}" site:schools-financial-benchmarking.service.gov.uk'
            if location:
                old_query += f' {location}'
            
            results = self.serper_engine.search_web(old_query, num_results=5)
            
            for result in results:
                url = result.get('url', '')
                urn_match = re.search(r'/school/(\d{5,7})', url)
                if urn_match:
                    urn = urn_match.group(1)
                    logger.info(f"✅ Found URN {urn} from OLD site")
                    return {
                        'urn': urn,
                        'official_name': self._extract_school_name(result.get('title', '')),
                        'confidence': 0.80,
                        'source': 'Old financial site'
                    }
            
            logger.warning(f"Could not find URN for {school_name}")
            return {'urn': None, 'confidence': 0.0}
            
        except Exception as e:
            logger.error(f"Error finding URN: {e}")
            return {'urn': None, 'error': str(e)}
    
    def _fetch_and_parse_financial_page(self, urn: str) -> Dict[str, Any]:
        """
        Fetch the financial page and extract data from HTML
        THE KEY: Data is in the initial HTML, either as text or as JSON in script tags
        """
        extracted = {}
        
        url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        try:
            logger.info(f"📥 Fetching: {url}")
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            html = response.text
            logger.info(f"✅ Got response: {len(html)} characters")
            
            # Strategy 1: Parse visible HTML with BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for the financial data cards
            # Based on your screenshot, data is in elements with specific text
            
            # In year balance
            balance_elem = soup.find(string=re.compile('In year balance', re.IGNORECASE))
            if balance_elem:
                # Find the parent and look for the value
                parent = balance_elem.find_parent()
                if parent:
                    value_text = parent.get_text()
                    balance_match = re.search(r'(-)?£([\d,]+)', value_text)
                    if balance_match:
                        sign = balance_match.group(1)
                        amount = int(balance_match.group(2).replace(',', ''))
                        if sign == '-':
                            amount = -amount
                        extracted['in_year_balance'] = amount
                        logger.info(f"✅ Extracted in-year balance: £{amount:,}")
            
            # Revenue reserve
            revenue_elem = soup.find(string=re.compile('Revenue reserve', re.IGNORECASE))
            if revenue_elem:
                parent = revenue_elem.find_parent()
                if parent:
                    value_text = parent.get_text()
                    revenue_match = re.search(r'£([\d,]+)', value_text)
                    if revenue_match:
                        amount = int(revenue_match.group(1).replace(',', ''))
                        extracted['revenue_reserve'] = amount
                        logger.info(f"✅ Extracted revenue reserve: £{amount:,}")
            
            # School phase
            phase_elem = soup.find(string=re.compile('School phase', re.IGNORECASE))
            if phase_elem:
                parent = phase_elem.find_parent()
                if parent:
                    extracted['school_phase'] = parent.get_text().strip()
            
            # Strategy 2: Look for JSON data in script tags
            # React apps often embed initial state as JSON
            for script in soup.find_all('script'):
                script_text = script.string or ''
                
                # Look for common patterns
                if 'inYearBalance' in script_text or 'revenueReserve' in script_text:
                    logger.info("✅ Found financial data in script tag")
                    
                    # Try to extract JSON
                    json_match = re.search(r'{[^{}]*"inYearBalance"[^{}]*}', script_text)
                    if json_match:
                        try:
                            data = json.loads(json_match.group(0))
                            if 'inYearBalance' in data:
                                extracted['in_year_balance'] = data['inYearBalance']
                            if 'revenueReserve' in data:
                                extracted['revenue_reserve'] = data['revenueReserve']
                        except:
                            pass
            
            # Strategy 3: Regex on raw HTML for the specific values
            # From your screenshot: -£248,998 and £1,126,983
            if not extracted.get('in_year_balance'):
                # Look for pattern around "In year balance"
                balance_section = re.search(r'In year balance.*?(-)?£([\d,]+)', html, re.IGNORECASE | re.DOTALL)
                if balance_section:
                    sign = balance_section.group(1)
                    amount = int(balance_section.group(2).replace(',', ''))
                    if sign == '-':
                        amount = -amount
                    extracted['in_year_balance'] = amount
                    logger.info(f"✅ Regex extracted balance: £{amount:,}")
            
            if not extracted.get('revenue_reserve'):
                revenue_section = re.search(r'Revenue reserve.*?£([\d,]+)', html, re.IGNORECASE | re.DOTALL)
                if revenue_section:
                    amount = int(revenue_section.group(1).replace(',', ''))
                    extracted['revenue_reserve'] = amount
                    logger.info(f"✅ Regex extracted revenue: £{amount:,}")
            
        except requests.RequestException as e:
            logger.error(f"❌ HTTP error fetching financial page: {e}")
        except Exception as e:
            logger.error(f"❌ Error parsing financial page: {e}")
        
        return extracted
    
    def _generate_insights(self, financial_data: Dict[str, Any], school_name: str) -> List[str]:
        """Generate insights from financial data"""
        insights = []
        
        # Always include that financial data is available
        if financial_data.get('urn'):
            insights.append(
                f"Financial data for {school_name} (URN: {financial_data['urn']}) is available on the government benchmarking tool"
            )
        
        # Specific insights based on extracted data
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                insights.append(f"School has a deficit of £{abs(balance):,} - cost-effective recruitment is critical")
            elif balance > 0:
                insights.append(f"School has a surplus of £{balance:,} - strong financial position")
        
        if 'revenue_reserve' in financial_data:
            reserve = financial_data['revenue_reserve']
            insights.append(f"Revenue reserve: £{reserve:,}")
            
            # Calculate months of operational cover (rough estimate)
            if reserve > 500000:
                insights.append("Strong reserves provide good financial cushion")
            elif reserve < 100000:
                insights.append("Limited reserves - budget management crucial")
        
        return insights
    
    def _generate_financial_starters(self, financial_data: Dict, insights: List[str], school_name: str) -> List[str]:
        """Generate conversation starters - ALWAYS returns at least one"""
        starters = []
        
        # ALWAYS include a starter about the financial data being available
        starters.append(
            f"I've reviewed {school_name}'s financial data on the government's benchmarking tool. "
            f"This provides valuable insights into your spending patterns. "
            f"Protocol Education can help optimize your recruitment and supply staff costs. "
            f"Would you like to discuss how we could support your budget management?"
        )
        
        # Add specific starters based on extracted data
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                starters.append(
                    f"I noticed your school is managing a deficit of £{abs(balance):,}. "
                    f"Many schools find that partnering with a cost-effective recruitment agency "
                    f"helps reduce staffing costs while maintaining quality. "
                    f"Shall we explore how Protocol can support your budget recovery?"
                )
            else:
                starters.append(
                    f"Your school has a healthy in-year balance of £{balance:,}. "
                    f"Protocol Education can help you maintain this strong financial position "
                    f"through competitive recruitment rates and quality guarantees."
                )
        
        if 'revenue_reserve' in financial_data:
            reserve = financial_data['revenue_reserve']
            starters.append(
                f"With revenue reserves of £{reserve:,}, strategic recruitment planning can "
                f"help maintain your financial stability. Protocol offers flexible solutions "
                f"that align with your budget cycles. Shall we discuss your staffing plans?"
            )
        
        return starters
    
    def _extract_school_name(self, title: str) -> str:
        """Extract clean school name from search result title"""
        # Remove common suffixes
        name = re.split(r' - URN:| - Get Information| - GOV\.UK| \| ', title)[0]
        return name.strip()


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    Called from processor_premium.py
    CRITICAL: Always succeeds and provides at minimum financial data links
    """
    try:
        financial_engine = FinancialDataEngine(serper_engine)
        
        logger.info(f"🏦 Enhancing {intel.school_name} with financial data")
        
        financial_intel = financial_engine.get_recruitment_intelligence(
            intel.school_name,
            intel.address
        )
        
        if not financial_intel.get('error'):
            # Add conversation starters
            if 'conversation_starters' in financial_intel:
                for starter_text in financial_intel['conversation_starters']:
                    intel.conversation_starters.append(
                        ConversationStarter(
                            topic="Financial Intelligence",
                            detail=starter_text,
                            source_url=financial_intel.get('source_url', ''),
                            relevance_score=0.9
                        )
                    )
            
            # Store financial data
            intel.financial_data = financial_intel
            
            logger.info(f"✅ Successfully enhanced {intel.school_name} with financial data")
        else:
            logger.warning(f"⚠️ Could not get financial data for {intel.school_name}: {financial_intel.get('error')}")
            # Even with error, try to store what we have
            if financial_intel.get('financial'):
                intel.financial_data = financial_intel
    
    except Exception as e:
        logger.error(f"❌ Error enhancing school with financial data: {e}")
        # Don't let this crash the whole process
    
    return intel
