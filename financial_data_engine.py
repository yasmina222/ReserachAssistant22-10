"""
Protocol Education CI System - Financial Data Engine FIXED
RESTORED: August 2025 working patterns with graceful degradation
CRITICAL FIX: Always provides financial link even if extraction fails
"""

import re
import logging
import requests
import os
from typing import Dict, Optional, List, Any
from datetime import datetime
from models import ConversationStarter

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper_engine = serper_engine
        
        # Get ScraperAPI key if available (not required)
        try:
            import streamlit as st
            self.scraper_api_key = st.secrets.get('SCRAPER_API_KEY', os.getenv('SCRAPER_API_KEY'))
        except:
            self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry point - get financial intelligence for a school
        CRITICAL: Always returns at minimum a link to financial data
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
            
            # Step 2: IMMEDIATELY create financial data dict with links to BOTH sites
            # This ensures users always get access to financial data
            financial_data = {
                'urn': urn,
                'school_name': school_name,
                'source_url': f"https://schools-financial-benchmarking.service.gov.uk/school/{urn}",
                'source_url_new': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
                'extracted_date': datetime.now().isoformat(),
                'official_name': urn_result.get('official_name', school_name)
            }
            
            # Step 3: Attempt data extraction (non-blocking)
            try:
                extracted_data = self._attempt_data_extraction(urn, school_name)
                financial_data.update(extracted_data)
            except Exception as e:
                logger.warning(f"Data extraction failed but continuing: {e}")
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
                link = result.get('url', '') or result.get('link', '')
                title = result.get('title', '')
                
                # PRIORITY 1: Extract from URL (most reliable)
                urn_match = re.search(r'/Details/(\d{5,7})', link)
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
            
            # Strategy 2: Search OLD financial site directly (fallback)
            logger.info("🔍 Trying OLD financial benchmarking site")
            fallback_query = f'"{school_name}" site:schools-financial-benchmarking.service.gov.uk'
            if location:
                fallback_query += f' {location}'
            
            results = self.serper_engine.search_web(fallback_query, num_results=5)
            
            for result in results:
                link = result.get('url', '') or result.get('link', '')
                # Extract URN from old site URL: /school/123456
                urn_match = re.search(r'/school/(\d{5,7})', link)
                if urn_match:
                    urn = urn_match.group(1)
                    logger.info(f"✅ Found URN {urn} from OLD site")
                    return {
                        'urn': urn,
                        'official_name': self._extract_school_name(result.get('title', '')),
                        'confidence': 0.80,
                        'source': 'Old financial site'
                    }
            
            # Strategy 3: Search NEW financial site
            logger.info("🔍 Trying NEW financial site")
            new_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
            if location:
                new_query += f' {location}'
            
            results = self.serper_engine.search_web(new_query, num_results=5)
            
            for result in results:
                link = result.get('url', '') or result.get('link', '')
                urn_match = re.search(r'/school/(\d{5,7})', link)
                if urn_match:
                    urn = urn_match.group(1)
                    logger.info(f"✅ Found URN {urn} from NEW site")
                    return {
                        'urn': urn,
                        'official_name': self._extract_school_name(result.get('title', '')),
                        'confidence': 0.80,
                        'source': 'New financial site'
                    }
            
            logger.warning(f"Could not find URN for {school_name}")
            return {'urn': None, 'confidence': 0.0}
            
        except Exception as e:
            logger.error(f"Error finding URN: {e}")
            return {'urn': None, 'error': str(e)}
    
    def _attempt_data_extraction(self, urn: str, school_name: str) -> Dict[str, Any]:
        """
        Attempt to extract financial data - NON-BLOCKING
        Uses AUGUST 2025 patterns that worked
        """
        extracted = {}
        
        # Try search-based extraction (worked in August)
        search_queries = [
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "Teaching and Teaching support staff" "per pupil"',
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "In year balance"',
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "Administrative supplies" "per pupil"',
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "Supply staff"'
        ]
        
        all_content = ""
        
        # Try first 2 searches to save API costs
        for query in search_queries[:2]:
            try:
                results = self.serper_engine.search_web(query, num_results=2)
                for result in results:
                    snippet = result.get('snippet', '')
                    all_content += snippet + " "
            except Exception as e:
                logger.debug(f"Search failed: {e}")
                continue
        
        # Extract specific values using August patterns
        if all_content:
            # Teaching staff per pupil
            teaching_match = re.search(r'Teaching.*?£([\d,]+)\s*per pupil', all_content, re.IGNORECASE)
            if teaching_match:
                try:
                    extracted['teaching_staff_per_pupil'] = int(teaching_match.group(1).replace(',', ''))
                    logger.info(f"✅ Extracted teaching staff cost: £{extracted['teaching_staff_per_pupil']}")
                except:
                    pass
            
            # In year balance (can be negative)
            balance_match = re.search(r'In year balance.*?([−-])?£([\d,]+)', all_content, re.IGNORECASE | re.DOTALL)
            if balance_match:
                try:
                    amount = int(balance_match.group(2).replace(',', ''))
                    # Check if negative
                    if balance_match.group(1) or 'deficit' in balance_match.group(0).lower():
                        amount = -amount
                    extracted['in_year_balance'] = amount
                    logger.info(f"✅ Extracted in-year balance: £{amount:,}")
                except:
                    pass
            
            # Administrative supplies
            admin_match = re.search(r'Administrative supplies.*?£([\d,]+)\s*per pupil', all_content, re.IGNORECASE)
            if admin_match:
                try:
                    extracted['admin_supplies_per_pupil'] = int(admin_match.group(1).replace(',', ''))
                    logger.info(f"✅ Extracted admin supplies: £{extracted['admin_supplies_per_pupil']}")
                except:
                    pass
            
            # Supply staff costs
            supply_match = re.search(r'Supply staff.*?£([\d,]+)', all_content, re.IGNORECASE)
            if supply_match:
                try:
                    extracted['supply_staff_costs'] = int(supply_match.group(1).replace(',', ''))
                    logger.info(f"✅ Extracted supply staff costs: £{extracted['supply_staff_costs']:,}")
                except:
                    pass
        
        if extracted:
            logger.info(f"✅ Successfully extracted {len(extracted)} financial metrics")
        else:
            logger.warning(f"⚠️ No financial data extracted, but users can still view links")
        
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
        
        if 'teaching_staff_per_pupil' in financial_data:
            cost = financial_data['teaching_staff_per_pupil']
            insights.append(f"Teaching staff costs: £{cost:,} per pupil")
            if cost > 7000:
                insights.append("Above-average teaching costs - recruitment optimization could yield significant savings")
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            if supply > 50000:
                insights.append(f"High supply staff expenditure (£{supply:,}) - potential for long-term placement savings")
        
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
                    f"Your school has a healthy surplus of £{balance:,}. "
                    f"Protocol Education can help you maintain this strong financial position "
                    f"through competitive recruitment rates and quality guarantees."
                )
        
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            starters.append(
                f"With teaching costs at £{teaching:,} per pupil, ensuring value in recruitment "
                f"is essential. Protocol's transparent pricing and quality guarantee ensure you "
                f"get excellent teachers at competitive rates. Can we discuss your current "
                f"recruitment challenges?"
            )
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            if supply > 50000:
                starters.append(
                    f"Your supply staff costs of £{supply:,} suggest significant opportunity "
                    f"to optimize. Protocol specializes in providing reliable supply teachers "
                    f"at competitive rates, and many schools save 15-20% by switching to us. "
                    f"Would you like to see a comparison?"
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
