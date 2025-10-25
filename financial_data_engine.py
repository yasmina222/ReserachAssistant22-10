"""
Protocol Education CI System - Financial Data Engine (SIMPLIFIED)
Retrieves school financial data from government sources
SIMPLE VERSION: Find URN, create link, done. No over-engineering.
"""

import re
import logging
import requests
import json
import os
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from models import ConversationStarter
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data - SIMPLE approach"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        
        # Get API key
        try:
            import streamlit as st
            self.scraper_api_key = st.secrets.get('SCRAPER_API_KEY', os.getenv('SCRAPER_API_KEY'))
        except:
            self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
    
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN - SIMPLE version with multiple search strategies
        """
        
        logger.info(f"🔍 Searching for URN: {school_name}")
        
        # Try multiple search strategies
        strategies = [
            self._search_gias,
            self._search_fbit_direct,
            self._search_general
        ]
        
        for strategy in strategies:
            result = strategy(school_name, location)
            if result and result.get('urn'):
                logger.info(f"✅ Found URN {result['urn']} via {result.get('source', 'search')}")
                return result
        
        logger.warning(f"❌ No URN found for {school_name}")
        return {
            'urn': None,
            'official_name': school_name,
            'confidence': 0.0,
            'error': 'Could not find URN'
        }
    
    def _search_gias(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Search Get Information About Schools"""
        
        query = f'"{school_name}" site:get-information-schools.service.gov.uk'
        if location:
            query += f' {location}'
        
        results = self.serper.search_web(query, num_results=10)
        return self._extract_urn_from_results(results, school_name)
    
    def _search_fbit_direct(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Search financial benchmarking site directly"""
        
        query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            query += f' {location}'
        
        results = self.serper.search_web(query, num_results=10)
        
        # Look for URN in URLs
        for result in results:
            url = result.get('url', '')
            
            # Extract URN from URL: /school/{URN}
            match = re.search(r'/school/(\d{5,7})', url)
            if match:
                urn = match.group(1)
                return {
                    'urn': urn,
                    'official_name': self._clean_title(result.get('title', school_name)),
                    'confidence': 0.9,
                    'source': 'FBIT Direct',
                    'url': url
                }
        
        return None
    
    def _search_general(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """General Google search"""
        
        query = f'"{school_name}" URN school UK'
        if location:
            query += f' {location}'
        
        results = self.serper.search_web(query, num_results=10)
        return self._extract_urn_from_results(results, school_name)
    
    def _extract_urn_from_results(self, results: List[Dict], school_name: str) -> Optional[Dict[str, Any]]:
        """Extract URN from search results using pattern matching"""
        
        if not results:
            return None
        
        # URN patterns to look for
        patterns = [
            r'/Establishments/Establishment/Details/(\d{5,7})',  # GIAS URL
            r'/school/(\d{5,7})',  # FBIT URL
            r'URN[:\s]+(\d{5,7})',  # URN: 123456
            r'establishment.*?(\d{6})',  # General
        ]
        
        found_urns = []
        
        for result in results:
            url = result.get('url', '')
            title = result.get('title', '')
            snippet = result.get('snippet', '')
            full_text = f"{url} {title} {snippet}"
            
            for pattern in patterns:
                matches = re.findall(pattern, full_text, re.IGNORECASE)
                for urn in matches:
                    if len(urn) >= 5 and len(urn) <= 7:
                        found_urns.append({
                            'urn': urn,
                            'official_name': self._clean_title(title),
                            'confidence': self._calculate_match(school_name, title),
                            'source': 'Search Results',
                            'url': url
                        })
        
        if not found_urns:
            return None
        
        # Return best match by confidence
        found_urns.sort(key=lambda x: x['confidence'], reverse=True)
        return found_urns[0]
    
    def _clean_title(self, title: str) -> str:
        """Clean school name from search result title"""
        # Remove common suffixes
        title = re.split(r' - URN:| - Get Information| - GOV\.UK| \| ', title)[0]
        return title.strip()
    
    def _calculate_match(self, search_name: str, result_title: str) -> float:
        """Calculate confidence score"""
        search_lower = search_name.lower()
        result_lower = result_title.lower()
        
        if search_lower in result_lower or result_lower in search_lower:
            return 0.9
        
        # Word overlap
        search_words = set(search_lower.split())
        result_words = set(result_lower.split())
        common_words = {'school', 'primary', 'secondary', 'academy', 'the', 'and'}
        search_words -= common_words
        result_words -= common_words
        
        if search_words and result_words:
            overlap = len(search_words & result_words) / len(search_words)
            return 0.5 + (overlap * 0.4)
        
        return 0.5
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Get financial data for a URN
        SIMPLE: Always creates link, optionally tries to extract data
        """
        
        logger.info(f"💰 Getting financial data for URN {urn}")
        
        # ALWAYS create the link - this is the critical part
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'extracted_date': datetime.now().isoformat()
        }
        
        # Try to extract actual data (optional - don't fail if this doesn't work)
        try:
            html_content = self._fetch_page(urn)
            if html_content:
                extracted_data = self._parse_financial_page(html_content)
                financial_data.update(extracted_data)
                financial_data['data_extracted'] = True
            else:
                financial_data['data_extracted'] = False
                financial_data['note'] = 'Click link to view financial data'
        except Exception as e:
            logger.warning(f"Could not extract data: {e}")
            financial_data['data_extracted'] = False
            financial_data['note'] = 'Click link to view financial data'
        
        return financial_data
    
    def _fetch_page(self, urn: str) -> Optional[str]:
        """Try to fetch the page with ScraperAPI"""
        
        if not self.scraper_api_key:
            logger.warning("No ScraperAPI key - skipping data extraction")
            return None
        
        url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        params = {
            'api_key': self.scraper_api_key,
            'url': url,
            'render': 'true',
            'country_code': 'gb'
        }
        
        try:
            response = requests.get('http://api.scraperapi.com', params=params, timeout=30)
            if response.status_code == 200 and 'In year balance' in response.text:
                return response.text
        except Exception as e:
            logger.warning(f"ScraperAPI failed: {e}")
        
        return None
    
    def _parse_financial_page(self, html: str) -> Dict[str, Any]:
        """Parse financial data from HTML"""
        
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract headline figures
        headline_figures = soup.find_all('li', class_='app-headline-figures')
        for figure in headline_figures:
            label = figure.find('p', class_='govuk-body-l govuk-!-font-weight-bold')
            value = figure.find_all('p', class_='govuk-body-l govuk-!-margin-bottom-2')
            
            if label and value:
                label_text = label.get_text(strip=True).lower()
                value_text = value[-1].get_text(strip=True) if value else ''
                
                # Extract amount
                value_match = re.search(r'[-−]?£([\d,]+)', value_text)
                if value_match:
                    amount = int(value_match.group(1).replace(',', ''))
                    if '-' in value_text or '−' in value_text:
                        amount = -amount
                    
                    if 'in year balance' in label_text:
                        data['in_year_balance'] = amount
                    elif 'revenue reserve' in label_text:
                        data['revenue_reserve'] = amount
        
        # Extract spending per pupil
        priority_wrappers = soup.find_all('div', class_='priority-wrapper')
        for wrapper in priority_wrappers:
            category_elem = wrapper.find('h4', class_='govuk-heading-s')
            if not category_elem:
                continue
            
            category = category_elem.get_text(strip=True)
            priority_elem = wrapper.find('p', class_='priority')
            if not priority_elem:
                continue
            
            text = priority_elem.get_text(strip=True)
            
            # Per pupil spending
            amount_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+pupil', text)
            if amount_match:
                amount = int(amount_match.group(1).replace(',', ''))
                
                if 'Teaching' in category and 'staff' in category:
                    data['teaching_staff_per_pupil'] = amount
                elif 'Administrative' in category:
                    data['admin_supplies_per_pupil'] = amount
        
        return data
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete recruitment intelligence - SIMPLE version
        """
        
        logger.info(f"🎯 Getting intelligence for {school_name}")
        
        # Step 1: Find URN
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'school_searched': school_name,
                'message': 'Try using the full official school name'
            }
        
        # Step 2: Get financial data (with link)
        financial_data = self.get_financial_data(
            urn_result['urn'],
            urn_result['official_name'],
            False
        )
        
        # Step 3: Generate insights
        insights = self._generate_insights(financial_data)
        
        # Step 4: Generate conversation starters
        conversations = self._generate_conversations(financial_data)
        
        return {
            'school_searched': school_name,
            'entity_found': {
                'name': urn_result['official_name'],
                'type': 'School',
                'urn': urn_result['urn'],
                'confidence': urn_result['confidence']
            },
            'financial': financial_data,
            'insights': insights,
            'conversation_starters': conversations
        }
    
    def _generate_insights(self, financial_data: Dict) -> List[str]:
        """Generate insights from financial data"""
        
        insights = []
        
        if not financial_data.get('data_extracted'):
            insights.append("Click the link above to view detailed financial data on the government website")
            return insights
        
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                insights.append(f"School has a deficit of £{abs(balance):,}")
            else:
                insights.append(f"School has a surplus of £{balance:,}")
        
        if 'revenue_reserve' in financial_data:
            reserve = financial_data['revenue_reserve']
            insights.append(f"Revenue reserve: £{reserve:,}")
        
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            insights.append(f"Teaching staff costs: £{teaching:,} per pupil")
        
        return insights
    
    def _generate_conversations(self, financial_data: Dict) -> List[str]:
        """Generate conversation starters"""
        
        starters = []
        
        # Always mention the financial data is available
        starters.append(
            "I can see your school's financial data is publicly available. "
            "Protocol Education can help optimize your recruitment and supply staff costs. "
            "Would you like to discuss how we can support your budget planning?"
        )
        
        # Add specific starters if we have data
        if financial_data.get('data_extracted'):
            if 'in_year_balance' in financial_data:
                balance = financial_data['in_year_balance']
                if balance < 0:
                    starters.append(
                        f"I noticed your school is managing a deficit of £{abs(balance):,}. "
                        "Many schools in this position find that optimizing recruitment costs "
                        "through a reliable agency partner makes a significant difference. "
                        "Shall we explore how Protocol Education could help?"
                    )
        
        return starters


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    SIMPLE: Just adds the link and any extractable data
    """
    
    try:
        financial_engine = FinancialDataEngine(serper_engine)
        
        logger.info(f"🏦 Getting financial data for {intel.school_name}")
        
        financial_intel = financial_engine.get_recruitment_intelligence(
            intel.school_name,
            intel.address
        )
        
        if not financial_intel.get('error'):
            # Add conversation starters
            if 'conversation_starters' in financial_intel:
                for starter in financial_intel['conversation_starters']:
                    intel.conversation_starters.append(
                        ConversationStarter(
                            topic="Financial Intelligence",
                            detail=starter,
                            source_url=financial_intel.get('financial', {}).get('source_url', ''),
                            relevance_score=0.85
                        )
                    )
            
            # Store financial data (includes the link)
            intel.financial_data = financial_intel
            
            logger.info(f"✅ Added financial data for {intel.school_name}")
        else:
            logger.warning(f"⚠️ Could not get financial data: {financial_intel.get('error')}")
            intel.financial_data = {
                'error': financial_intel.get('error'),
                'message': financial_intel.get('message')
            }
    
    except Exception as e:
        logger.error(f"❌ Error getting financial data: {e}")
        intel.financial_data = {'error': str(e)}
    
    return intel
