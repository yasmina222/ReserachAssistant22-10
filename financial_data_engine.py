import re
import logging
import requests
import json
import os
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from models import ConversationStarter
from openai import OpenAI

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources using Firecrawl"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        self.firecrawl_api_key = "fc-d1b7c888232f480d8058d9f137460741"
        
        # Initialize OpenAI client for GPT extraction
        try:
            import streamlit as st
            openai_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
        except:
            openai_key = os.getenv("OPENAI_API_KEY")
        
        self.openai_client = OpenAI(api_key=openai_key)
        logger.info("✅ Financial engine initialized with Firecrawl")
        
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
                logger.info(f"✅ Found URN from GIAS URL: {urn_from_url}")
            
            if not urn_from_url:
                urn_pattern = r'URN:?\s*(\d{5,7})'
                urn_match = re.search(urn_pattern, text)
                if urn_match:
                    urn_from_url = urn_match.group(1)
                    logger.info(f"✅ Found URN from text: {urn_from_url}")
            
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
            logger.warning(f"❌ No URN found for {school_name}")
            return {'urn': None, 'confidence': 0.0, 'error': 'No URN found'}
        
        urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
        best_match = urn_matches[0]
        best_match['alternatives'] = urn_matches[1:3] if len(urn_matches) > 1 else []
        
        logger.info(f"✅ Best URN match: {best_match['urn']} for {best_match['official_name']}")
        
        return best_match
    
    def _scrape_with_firecrawl(self, url: str) -> Optional[str]:
        """
        Use Firecrawl API to scrape JavaScript-rendered content
        Firecrawl is designed for complex sites like government portals
        """
        
        firecrawl_url = "https://api.firecrawl.dev/v1/scrape"
        
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "url": url,
            "formats": ["markdown", "html"],
            "waitFor": 3000  # Wait 3 seconds for JavaScript to load
        }
        
        logger.info(f"🔥 Firecrawl scraping: {url}")
        
        try:
            response = requests.post(firecrawl_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Firecrawl returns both markdown and HTML
                if data.get('success'):
                    markdown_content = data.get('data', {}).get('markdown', '')
                    html_content = data.get('data', {}).get('html', '')
                    
                    # Prefer markdown (cleaner) but fall back to HTML
                    content = markdown_content if markdown_content else html_content
                    
                    if content:
                        logger.info(f"✅ Firecrawl success! Got {len(content)} characters")
                        return content
                    else:
                        logger.error("❌ Firecrawl returned empty content")
                        return None
                else:
                    logger.error(f"❌ Firecrawl failed: {data.get('error', 'Unknown error')}")
                    return None
            else:
                logger.error(f"❌ Firecrawl HTTP {response.status_code}: {response.text[:200]}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Firecrawl exception: {e}")
            return None
    
    def _extract_benchmark_with_gpt(self, content: str, urn: str) -> Dict[str, Any]:
        """
        Use GPT-4o-mini to extract benchmark data from scraped content
        Works with both HTML and markdown
        """
        
        logger.info("🤖 Using GPT-4o-mini to extract benchmark data...")
        
        # Truncate if too long
        content_snippet = content[:30000] if len(content) > 30000 else content
        
        prompt = f"""
Extract financial data from this UK school's FBIT comparison page.

CRITICAL: Return ONLY valid JSON. Use null for missing values.

Required fields (annual costs in £):
1. total_expenditure_per_pupil
2. teaching_and_support_costs_per_pupil  
3. teaching_staff_costs
4. supply_teaching_staff_costs
5. educational_consultancy_costs
6. educational_support_staff_costs
7. agency_supply_teaching_staff_costs

Example format:
{{
    "total_expenditure_per_pupil": 7200,
    "teaching_and_support_costs_per_pupil": 5100,
    "teaching_staff_costs": 950000,
    "supply_teaching_staff_costs": 85000,
    "educational_consultancy_costs": 15000,
    "educational_support_staff_costs": 180000,
    "agency_supply_teaching_staff_costs": 42000
}}

Page content:
{content_snippet}

Return ONLY the JSON object with numeric values (no £, no commas).
"""
        
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You extract financial data from web content. Return only valid JSON with numeric values. Use null for missing data."
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
            found_count = sum(1 for v in benchmark_data.values() if v is not None and v != "null" and v != 0)
            logger.info(f"✅ GPT extracted {found_count}/7 benchmark fields")
            
            for key, value in benchmark_data.items():
                if value and value != "null" and value != 0:
                    logger.info(f"  ✓ {key}: £{value:,}")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ GPT extraction failed: {e}")
            return {}
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data from FBIT website using URN
        NOW USING FIRECRAWL for reliable scraping
        """
        
        logger.info(f"💰 Fetching financial data for URN {urn}")
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'comparison_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}/comparison",
            'extracted_date': datetime.now().isoformat()
        }
        
        # Scrape the main FBIT page with Firecrawl
        main_page_content = self._scrape_with_firecrawl(financial_data['source_url'])
        
        if main_page_content:
            # Extract basic financial metrics (in-year balance, revenue reserve)
            balance_match = re.search(r'In year balance[:\s]*[-−]?£([0-9,]+)', main_page_content, re.IGNORECASE)
            if balance_match:
                amount = int(balance_match.group(1).replace(',', ''))
                if '−' in main_page_content[max(0, balance_match.start()-10):balance_match.start()] or '-' in main_page_content[max(0, balance_match.start()-10):balance_match.start()]:
                    amount = -amount
                financial_data['in_year_balance'] = amount
                logger.info(f"  ✓ In-year balance: £{amount:,}")
            
            reserve_match = re.search(r'Revenue reserve[:\s]*£([0-9,]+)', main_page_content, re.IGNORECASE)
            if reserve_match:
                financial_data['revenue_reserve'] = int(reserve_match.group(1).replace(',', ''))
                logger.info(f"  ✓ Revenue reserve: £{financial_data['revenue_reserve']:,}")
        
        # NOW SCRAPE THE COMPARISON PAGE (the one with all the good data!)
        logger.info("🔥 Fetching benchmark comparison page...")
        comparison_content = self._scrape_with_firecrawl(financial_data['comparison_url'])
        
        if comparison_content:
            benchmark_data = self._extract_benchmark_with_gpt(comparison_content, urn)
            
            if benchmark_data:
                financial_data['benchmark_data'] = benchmark_data
                logger.info(f"✅ Added benchmark data with {len(benchmark_data)} fields")
            else:
                logger.warning("⚠️ No benchmark data extracted from comparison page")
        else:
            logger.warning("⚠️ Could not fetch comparison page with Firecrawl")
        
        # Calculate recruitment estimates from benchmark data
        if 'benchmark_data' in financial_data:
            benchmark = financial_data['benchmark_data']
            
            supply_costs = benchmark.get('supply_teaching_staff_costs', 0)
            agency_costs = benchmark.get('agency_supply_teaching_staff_costs', 0)
            
            if supply_costs or agency_costs:
                recruitment_base = (supply_costs or 0) + (agency_costs or 0)
                
                financial_data['recruitment_estimates'] = {
                    'low': int(recruitment_base * 0.20),
                    'high': int(recruitment_base * 0.30),
                    'midpoint': int(recruitment_base * 0.25)
                }
                logger.info(f"  ✓ Recruitment estimate: £{financial_data['recruitment_estimates']['midpoint']:,}")
        
        return financial_data
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete recruitment cost intelligence for a school"""
        
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        logger.info(f"✅ Found URN {urn_result['urn']} for {urn_result['official_name']}")
        
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
                insights.append(f"🎯 Agency supply: £{benchmark['agency_supply_teaching_staff_costs']:,}/year - HIGH PRIORITY TARGET")
        
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                insights.append(f"⚠️ Deficit: £{abs(balance):,} - needs cost savings urgently")
            else:
                insights.append(f"✅ Surplus: £{balance:,}")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict) -> List[str]:
        """Generate conversation starters"""
        starters = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            agency = benchmark.get('agency_supply_teaching_staff_costs', 0)
            if agency > 0:
                savings = int(agency * 0.25)
                starters.append(
                    f"I noticed from the government financial data that you're spending £{agency:,} annually on agency supply staff. "
                    f"Protocol Education typically saves schools 20-30% (approximately £{savings:,}) on these costs "
                    f"with better quality guarantees and dedicated account management."
                )
            
            supply = benchmark.get('supply_teaching_staff_costs', 0)
            if supply > 50000:
                starters.append(
                    f"Your total supply teaching costs of £{supply:,} suggest regular staffing challenges. "
                    f"Many schools find that our long-term supply solutions reduce costs by 15-25% while improving continuity. "
                    f"Would you be open to a brief conversation about how we might help?"
                )
            
            consultancy = benchmark.get('educational_consultancy_costs', 0)
            if consultancy > 15000:
                starters.append(
                    f"I see you're investing £{consultancy:,} in educational consultancy. "
                    f"Often this indicates leadership transitions or Ofsted preparation. "
                    f"We've helped many schools in similar situations with strategic staffing solutions during periods of change."
                )
        
        balance = financial_data.get('in_year_balance', 0)
        if balance < -30000:
            starters.append(
                f"I understand your school is managing a deficit of £{abs(balance):,}. "
                f"Protocol Education has specific programs to help schools reduce recruitment and supply costs "
                f"as part of financial recovery plans. Many of our clients have seen 20-30% savings within the first year."
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
                            relevance_score=0.95
                        )
                    )
            
            intel.financial_data = financial_intel
            logger.info(f"✅ Enhanced {intel.school_name} with financial data")
        
    except Exception as e:
        logger.error(f"❌ Error enhancing school with financial data: {e}")
    
    return intel
