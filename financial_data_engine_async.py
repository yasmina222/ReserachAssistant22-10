"""
Protocol Education CI System - Financial Data Engine (ASYNC VERSION)
PHASE 1: Converted to async for parallel operations
"""

import re
import logging
import aiohttp
import asyncio
import json
import os
from typing import Dict, Optional, List, Any
from datetime import datetime
from models import ConversationStarter
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class FinancialDataEngineAsync:
    """Async version - retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
        
    async def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Find school URN using government database - ASYNC"""
        
        search_query = f'"{school_name}"'
        if location:
            search_query += f' {location}'
        search_query += ' site:get-information-schools.service.gov.uk'
        
        logger.info(f"Searching for school URN: {search_query}")
        
        results = await self.serper.search_web(search_query, num_results=5)
        
        if not results:
            return await self._search_fbit_direct(school_name, location)
        
        urn_matches = []
        for result in results:
            url = result.get('url', '')
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            
            if '/Groups/Group/' in url:
                continue
                
            urn_from_url = None
            
            gias_match = re.search(r'/Establishments/Establishment/Details/(\d{5,7})', url)
            if gias_match:
                urn_from_url = gias_match.group(1)
                logger.info(f"Found URN from GIAS URL: {urn_from_url}")
            
            if not urn_from_url:
                urn_pattern = r'URN:?\s*(\d{5,7})'
                urn_match = re.search(urn_pattern, text)
                if urn_match:
                    urn_from_url = urn_match.group(1)
            
            if urn_from_url:
                official_name = self._extract_school_name(result)
                trust_name = None
                if 'trust' in text.lower() or 'academy trust' in text.lower():
                    trust_pattern = r'Part of\s+([A-Z][A-Za-z\s&]+(?:Trust|Federation))'
                    trust_match = re.search(trust_pattern, text, re.IGNORECASE)
                    if trust_match:
                        trust_name = trust_match.group(1).strip()
                
                urn_matches.append({
                    'urn': urn_from_url,
                    'official_name': official_name,
                    'trust_name': trust_name,
                    'address': self._extract_location(result),
                    'url': url,
                    'confidence': self._calculate_name_match(school_name, result, False)
                })
        
        if not urn_matches:
            logger.warning(f"No URN found for {school_name}")
            return {'urn': None, 'confidence': 0.0, 'error': 'No URN found'}
        
        urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
        best_match = urn_matches[0]
        best_match['alternatives'] = urn_matches[1:3] if len(urn_matches) > 1 else []
        
        return best_match
    
    async def _fetch_fbit_page(self, urn: str) -> Optional[str]:
        """Fetch actual FBIT page content using ScraperAPI - ASYNC"""
        
        if not self.scraper_api_key:
            logger.error("SCRAPER_API_KEY not found")
            return None
        
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        params = {
            'api_key': self.scraper_api_key,
            'url': base_url,
            'render': 'true',
            'country_code': 'gb'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://api.scraperapi.com', params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        text = await response.text()
                        if 'Page not found' not in text and 'Spending priorities for this school' in text:
                            logger.info("Successfully fetched FBIT page")
                            return text
                    logger.error(f"ScraperAPI returned status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching FBIT page: {e}")
            return None
    
    async def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """Retrieve financial data from FBIT website - ASYNC"""
        
        logger.info(f"Fetching financial data for URN {urn}")
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'extracted_date': datetime.now().isoformat()
        }
        
        html_content = await self._fetch_fbit_page(urn)
        
        if not html_content:
            logger.warning("Failed to fetch FBIT page")
            return await self._get_financial_data_from_search(urn, entity_name, is_trust)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract metrics (same parsing logic as original)
        headline_figures = soup.find_all('li', class_='app-headline-figures')
        for figure in headline_figures:
            label = figure.find('p', class_='govuk-body-l govuk-!-font-weight-bold')
            value = figure.find_all('p', class_='govuk-body-l govuk-!-margin-bottom-2')
            
            if label and value:
                label_text = label.get_text(strip=True).lower()
                value_text = value[-1].get_text(strip=True) if value else ''
                
                value_match = re.search(r'[-−]?£([\d,]+)', value_text)
                if value_match:
                    amount = int(value_match.group(1).replace(',', ''))
                    if '-' in value_text or '−' in value_text:
                        amount = -amount
                    
                    if 'in year balance' in label_text:
                        financial_data['in_year_balance'] = amount
                    elif 'revenue reserve' in label_text:
                        financial_data['revenue_reserve'] = amount
        
        # Extract spending priorities
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
            amount_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+pupil', text)
            if amount_match:
                amount = int(amount_match.group(1).replace(',', ''))
                
                if 'Teaching' in category and 'staff' in category:
                    financial_data['teaching_staff_per_pupil'] = amount
                elif 'Administrative supplies' in category:
                    financial_data['admin_supplies_per_pupil'] = amount
                else:
                    key = category.lower().replace(' ', '_') + '_per_pupil'
                    financial_data[key] = amount
        
        # Calculate recruitment estimates
        if 'indirect_employee_expenses' in financial_data:
            financial_data['recruitment_estimates'] = {
                'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25)
            }
        
        financial_data['extraction_confidence'] = self._calculate_extraction_confidence(financial_data)
        
        return financial_data
    
    async def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete recruitment cost intelligence - ASYNC"""
        
        urn_result = await self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        financial_data = await self.get_financial_data(
            urn_result['urn'],
            urn_result['official_name'],
            False
        )
        
        intelligence = {
            'school_searched': school_name,
            'entity_found': {
                'name': urn_result['official_name'],
                'type': 'School',
                'urn': urn_result['urn'],
                'location': urn_result.get('address', ''),
                'trust_name': urn_result.get('trust_name'),
                'confidence': urn_result['confidence']
            },
            'financial': financial_data,
            'insights': self._generate_insights(financial_data, False),
            'comparison': self._get_benchmarks(financial_data),
            'conversation_starters': self._generate_cost_conversations(financial_data, None, None)
        }
        
        return intelligence
    
    async def _get_financial_data_from_search(self, urn: str, entity_name: str, is_trust: bool) -> Dict[str, Any]:
        """Fallback using search - ASYNC"""
        
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': base_url,
            'extracted_date': datetime.now().isoformat(),
            'data_source': 'search_fallback'
        }
        
        search_queries = [
            f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "Spends" "per pupil"',
            f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "In year balance"'
        ]
        
        # Run searches in parallel
        results = await asyncio.gather(*[self.serper.search_web(q, 3) for q in search_queries])
        
        for search_results in results:
            if search_results:
                all_content = ' '.join([r.get('snippet', '') for r in search_results])
                
                patterns = {
                    'teaching_staff_per_pupil': r'Teaching.*?staff.*?Spends\s+£([\d,]+)\s+per\s+pupil',
                    'in_year_balance': r'In\s+year\s+balance[:\s]+[-−]?£([\d,]+)',
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, all_content, re.IGNORECASE | re.DOTALL)
                    if match:
                        value_str = match.group(1).replace(',', '')
                        financial_data[key] = int(value_str)
        
        return financial_data
    
    async def _search_fbit_direct(self, school_name: str, location: Optional[str]) -> Dict:
        """Search FBIT directly - ASYNC"""
        search_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            search_query += f' {location}'
            
        results = await self.serper.search_web(search_query, num_results=3)
        
        for result in results:
            url = result.get('url', '')
            urn_match = re.search(r'/school/(\d{5,7})', url)
            if urn_match:
                return {
                    'urn': urn_match.group(1),
                    'official_name': self._extract_school_name(result),
                    'confidence': 0.7,
                    'is_trust': False,
                    'alternatives': []
                }
        
        return {'urn': None, 'confidence': 0.0, 'error': 'No results found'}
    
    def _calculate_extraction_confidence(self, data: Dict[str, Any]) -> float:
        """Calculate confidence score"""
        essential = ['teaching_staff_per_pupil', 'in_year_balance']
        found = sum(1 for f in essential if f in data and data[f] is not None)
        return round(found / len(essential), 2)
    
    def _generate_insights(self, financial_data: Dict, is_trust: bool) -> List[str]:
        """Generate insights"""
        insights = []
        if 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                insights.append(f"Estimated annual recruitment spend: £{est['midpoint']:,}")
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict, trust_name: str = None, schools_count: int = None) -> List[str]:
        """Generate cost conversations"""
        starters = []
        if 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                cost = est['midpoint']
                starters.append(
                    f"Your school spends approximately £{cost:,} annually on recruitment. "
                    "Protocol Education could help reduce these costs."
                )
        return starters
    
    def _get_benchmarks(self, financial_data: Dict) -> Dict:
        """Get benchmarks"""
        return {'national_average': {}, 'comparison': {}}
    
    def _extract_school_name(self, result: Dict) -> str:
        """Extract school name"""
        title = result.get('title', '')
        name = re.split(r' - URN:| - Get Information', title)[0]
        return name.strip()
    
    def _extract_location(self, result: Dict) -> str:
        """Extract location"""
        snippet = result.get('snippet', '')
        postcode_match = re.search(r'[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}', snippet)
        return postcode_match.group() if postcode_match else ''
    
    def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
        """Calculate name match confidence"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        if search_name == result_name:
            return 1.0
        if search_name in result_name or result_name in search_name:
            return 0.7
        return 0.5


async def enhance_school_with_financial_data_async(intel, serper_engine):
    """Async version of financial enhancement"""
    try:
        financial_engine = FinancialDataEngineAsync(serper_engine)
        financial_intel = await financial_engine.get_recruitment_intelligence(
            intel.school_name,
            intel.address
        )
        
        if not financial_intel.get('error'):
            if 'conversation_starters' in financial_intel:
                for starter in financial_intel['conversation_starters']:
                    intel.conversation_starters.append(
                        ConversationStarter(
                            topic="Recruitment Costs",
                            detail=starter,
                            source_url=financial_intel.get('financial', {}).get('source_url', ''),
                            relevance_score=0.9
                        )
                    )
            intel.financial_data = financial_intel
    except Exception as e:
        logger.error(f"Financial enhancement error: {e}")
    
    return intel
