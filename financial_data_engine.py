import re
import logging
import requests
import json
import os
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from models import ConversationStarter
from bs4 import BeautifulSoup
from openai import OpenAI

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
        
        # Initialize OpenAI client for GPT extraction
        try:
            import streamlit as st
            openai_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
        except:
            openai_key = os.getenv("OPENAI_API_KEY")
        
        self.openai_client = OpenAI(api_key=openai_key)
        
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Find school URN using government database"""
        
        search_query = f'"{school_name}"'
        if location:
            search_query += f' {location}'
        search_query += ' site:get-information-schools.service.gov.uk'
        
        logger.info(f"Searching for school URN: {search_query}")
        
        results = self.serper.search_web(search_query, num_results=5)
        
        if not results:
            return self._search_fbit_direct(school_name, location)
        
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
                    logger.info(f"Found URN from text: {urn_from_url}")
            
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
        
        logger.info(f"Best URN match: {best_match['urn']} for {best_match['official_name']}")
        
        return best_match
    
    def _fetch_fbit_page(self, urn: str) -> Optional[str]:
        """Fetch actual FBIT page content using ScraperAPI"""
        
        if not self.scraper_api_key:
            logger.error("SCRAPER_API_KEY not found in environment")
            return None
        
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        params = {
            'api_key': self.scraper_api_key,
            'url': base_url,
            'render': 'true',
            'country_code': 'gb'
        }
        
        logger.info(f"Fetching FBIT page: {base_url}")
        
        try:
            response = requests.get('http://api.scraperapi.com', params=params, timeout=30)
            
            if response.status_code == 200:
                if 'Page not found' not in response.text:
                    logger.info("✅ Successfully fetched FBIT page")
                    return response.text
                else:
                    logger.error(f"URN {urn} returned a 404 or invalid page")
                    return None
            else:
                logger.error(f"ScraperAPI returned status {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching FBIT page: {e}")
            return None
    
    def _fetch_fbit_comparison_page(self, urn: str) -> Optional[str]:
        """
        Fetch FBIT comparison page using ScraperAPI
        """
        
        if not self.scraper_api_key:
            logger.error("SCRAPER_API_KEY not found in environment")
            return None
        
        comparison_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}/comparison"
        
        params = {
            'api_key': self.scraper_api_key,
            'url': comparison_url,
            'render': 'true',
            'country_code': 'gb'
        }
        
        logger.info(f"🔍 Fetching FBIT comparison page: {comparison_url}")
        
        try:
            response = requests.get('http://api.scraperapi.com', params=params, timeout=30)
            
            if response.status_code == 200:
                logger.info("✅ Successfully fetched FBIT comparison page")
                return response.text
            else:
                logger.error(f"ScraperAPI returned status {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching FBIT comparison page: {e}")
            return None
    
    def _extract_benchmark_with_gpt(self, html_content: str, urn: str) -> Dict[str, Any]:
        """
        NEW: Use GPT-4o-mini to extract benchmark data from HTML
        This is more reliable than parsing HTML structure
        """
        
        logger.info("🤖 Using GPT-4o-mini to extract benchmark data...")
        
        # Truncate HTML if too long (keep first 50K chars which should contain the data)
        html_snippet = html_content[:50000] if len(html_content) > 50000 else html_content
        
        prompt = f"""
Extract the following financial data from this UK school's FBIT comparison page HTML.

CRITICAL: Return ONLY valid JSON with these exact field names. Use null for any missing values.

Required fields (all are annual costs in pounds £):
1. total_expenditure_per_pupil
2. teaching_and_support_costs_per_pupil  
3. teaching_staff_costs
4. supply_teaching_staff_costs
5. educational_consultancy_costs
6. educational_support_staff_costs
7. agency_supply_teaching_staff_costs

Return format:
{{
    "total_expenditure_per_pupil": 7200,
    "teaching_and_support_costs_per_pupil": 5100,
    "teaching_staff_costs": 950000,
    "supply_teaching_staff_costs": 85000,
    "educational_consultancy_costs": 15000,
    "educational_support_staff_costs": 180000,
    "agency_supply_teaching_staff_costs": 42000
}}

HTML content:
{html_snippet}

Return ONLY the JSON object, no explanations.
"""
        
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You extract financial data from HTML. Return only valid JSON with numeric values (no currency symbols, no commas). Use null for missing data."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content
            benchmark_data = json.loads(result_text)
            
            # Log what we found
            found_count = sum(1 for v in benchmark_data.values() if v is not None and v != "null")
            logger.info(f"✅ GPT extracted {found_count}/7 benchmark fields")
            
            for key, value in benchmark_data.items():
                if value and value != "null":
                    logger.info(f"  ✓ {key}: £{value:,}")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ GPT extraction failed: {e}")
            return {}
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data from FBIT website using URN
        UPDATED: Now uses GPT-4o-mini to extract benchmark data
        """
        
        logger.info(f"Fetching financial data for URN {urn}")
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'comparison_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}/comparison",
            'extracted_date': datetime.now().isoformat()
        }
        
        html_content = self._fetch_fbit_page(urn)
        
        if not html_content:
            logger.warning("Failed to fetch FBIT page")
            return financial_data
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract basic financial metrics from main page
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
        
        # NEW: Fetch comparison page and use GPT to extract
        logger.info("🔍 Fetching benchmark comparison data...")
        comparison_html = self._fetch_fbit_comparison_page(urn)
        
        if comparison_html:
            benchmark_data = self._extract_benchmark_with_gpt(comparison_html, urn)
            
            if benchmark_data:
                financial_data['benchmark_data'] = benchmark_data
                logger.info(f"✅ Added benchmark data with {len(benchmark_data)} fields")
            else:
                logger.warning("⚠️ No benchmark data extracted")
        else:
            logger.warning("⚠️ Could not fetch comparison page")
        
        # Calculate recruitment estimates
        if 'benchmark_data' in financial_data:
            benchmark = financial_data['benchmark_data']
            
            # Use supply costs from benchmark data
            supply_costs = benchmark.get('supply_teaching_staff_costs')
            agency_costs = benchmark.get('agency_supply_teaching_staff_costs')
            
            if supply_costs or agency_costs:
                # Estimate recruitment spend as 25% of supply costs
                recruitment_base = (supply_costs or 0) + (agency_costs or 0)
                
                financial_data['recruitment_estimates'] = {
                    'low': int(recruitment_base * 0.20),
                    'high': int(recruitment_base * 0.30),
                    'midpoint': int(recruitment_base * 0.25)
                }
        
        return financial_data
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete recruitment cost intelligence for a school"""
        
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        logger.info(f"Found URN {urn_result['urn']} for {urn_result['official_name']}")
        
        financial_data = self.get_financial_data(
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
            'insights': self._generate_insights(financial_data),
            'conversation_starters': self._generate_cost_conversations(financial_data)
        }
        
        return intelligence
    
    def _generate_insights(self, financial_data: Dict) -> List[str]:
        """Generate insights from financial data"""
        insights = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            if benchmark.get('total_expenditure_per_pupil'):
                insights.append(f"Total expenditure: £{benchmark['total_expenditure_per_pupil']:,} per pupil")
            
            if benchmark.get('supply_teaching_staff_costs'):
                insights.append(f"Supply teaching: £{benchmark['supply_teaching_staff_costs']:,}/year")
            
            if benchmark.get('agency_supply_teaching_staff_costs'):
                insights.append(f"Agency supply: £{benchmark['agency_supply_teaching_staff_costs']:,}/year - HIGH PRIORITY TARGET")
        
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                insights.append(f"Deficit: £{abs(balance):,} - needs cost savings")
            else:
                insights.append(f"Surplus: £{balance:,}")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict) -> List[str]:
        """Generate conversation starters"""
        starters = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            if benchmark.get('agency_supply_teaching_staff_costs', 0) > 0:
                starters.append(
                    f"I see you're spending £{benchmark['agency_supply_teaching_staff_costs']:,} on agency supply staff. "
                    f"Protocol Education typically saves schools 20-30% on these costs with better quality guarantees."
                )
            
            if benchmark.get('supply_teaching_staff_costs', 0) > 50000:
                starters.append(
                    f"Your £{benchmark['supply_teaching_staff_costs']:,} supply costs suggest regular staffing gaps. "
                    f"Our long-term supply solutions typically reduce costs by 15-25%."
                )
        
        return starters
    
    def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
        """Calculate confidence score for name match"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        if search_name == result_name:
            return 1.0
        if search_name in result_name or result_name in search_name:
            return 0.9
        return 0.7
    
    def _extract_school_name(self, search_result: Dict) -> str:
        """Extract official school name from search result"""
        title = search_result.get('title', '')
        name = re.split(r' - URN:| - Get Information| - GOV.UK', title)[0]
        return name.strip()
    
    def _extract_location(self, search_result: Dict) -> str:
        """Extract location from search result"""
        snippet = search_result.get('snippet', '')
        postcode_match = re.search(r'[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}', snippet)
        if postcode_match:
            return postcode_match.group()
        return ''
    
    def _search_fbit_direct(self, school_name: str, location: Optional[str]) -> Dict:
        """Try searching FBIT directly"""
        search_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            search_query += f' {location}'
            
        results = self.serper.search_web(search_query, num_results=3)
        
        for result in results:
            url = result.get('url', '')
            urn_match = re.search(r'/school/(\d{5,7})', url)
            if urn_match:
                return {
                    'urn': urn_match.group(1),
                    'official_name': self._extract_school_name(result),
                    'confidence': 0.7,
                    'alternatives': []
                }
        
        return {'urn': None, 'confidence': 0.0, 'error': 'No results found'}


def enhance_school_with_financial_data(intel, serper_engine):
    """Add financial data to existing school intelligence"""
    
    try:
        financial_engine = FinancialDataEngine(serper_engine)
        
        financial_intel = financial_engine.get_recruitment_intelligence(
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
                            source_url=financial_intel.get('financial', {}).get('comparison_url', ''),
                            relevance_score=0.9
                        )
                    )
            
            intel.financial_data = financial_intel
            logger.info(f"✅ Enhanced {intel.school_name} with financial data")
        
    except Exception as e:
        logger.error(f"Error enhancing school with financial data: {e}")
    
    return intel
