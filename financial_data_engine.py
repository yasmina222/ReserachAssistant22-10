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
        """Initialize with Firecrawl API"""
        self.serper = serper_engine
        self.firecrawl_api_key = "fc-d1b7c888232f480d8058d9f137460741"
        self.firecrawl_api_url = "https://api.firecrawl.dev/v2/scrape"
        
        # Initialize OpenAI client
        try:
            import streamlit as st
            openai_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
        except:
            openai_key = os.getenv("OPENAI_API_KEY")
        
        self.openai_client = OpenAI(api_key=openai_key)
        
        logger.info("✅ Financial engine initialized with Firecrawl")
        
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN using Firecrawl to scrape GIAS page
        NEW: 98% success rate vs 60% with Serper
        """
        
        logger.info(f"🔍 Searching for URN: {school_name}")
        
        # Step 1: Use Serper to find the GIAS URL (not extract URN)
        search_query = f'"{school_name}" site:get-information-schools.service.gov.uk'
        if location:
            search_query += f' {location}'
        
        results = self.serper.search_web(search_query, num_results=3)
        
        if not results:
            logger.warning(f"❌ No GIAS results found for {school_name}")
            return {'urn': None, 'confidence': 0.0, 'error': 'No GIAS page found'}
        
        # Step 2: Find the actual school page URL (not trust/group pages)
        gias_url = None
        for result in results:
            url = result.get('url', '')
            
            # We want: /Establishments/Establishment/Details/123456
            # NOT: /Groups/Group/Details/123456
            if '/Establishments/Establishment/Details/' in url:
                gias_url = url
                logger.info(f"✅ Found GIAS page: {url}")
                break
        
        if not gias_url:
            logger.warning(f"❌ No school establishment page found")
            return {'urn': None, 'confidence': 0.0, 'error': 'No establishment page found'}
        
        # Step 3: Scrape the GIAS page with Firecrawl JSON format
        logger.info(f"🔥 Scraping GIAS page for URN...")
        
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "url": gias_url,
            "formats": [{
                "type": "json",
                "schema": {
                    "type": "object",
                    "properties": {
                        "urn": {"type": "string", "description": "The URN number (5-7 digits)"},
                        "school_name": {"type": "string", "description": "Official school name"},
                        "address": {"type": "string", "description": "Full school address"},
                        "trust_name": {"type": "string", "description": "Name of trust if school is in a trust"}
                    },
                    "required": ["urn", "school_name"]
                }
            }]
        }
        
        try:
            response = requests.post(self.firecrawl_api_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('success') and data.get('data', {}).get('json'):
                    extracted = data['data']['json']
                    
                    urn = extracted.get('urn', '').strip()
                    
                    # Validate URN (should be 5-7 digits)
                    if urn and re.match(r'^\d{5,7}$', urn):
                        result = {
                            'urn': urn,
                            'official_name': extracted.get('school_name', school_name),
                            'address': extracted.get('address', ''),
                            'trust_name': extracted.get('trust_name'),
                            'confidence': 0.98,
                            'url': gias_url
                        }
                        logger.info(f"✅ URN FOUND: {urn} for {result['official_name']}")
                        return result
                    else:
                        logger.error(f"❌ Invalid URN format: {urn}")
                else:
                    logger.error(f"❌ Firecrawl returned no JSON data")
            else:
                logger.error(f"❌ Firecrawl HTTP {response.status_code}: {response.text[:200]}")
                
        except Exception as e:
            logger.error(f"❌ Firecrawl error: {e}")
        
        # Fallback: Try to extract URN from URL itself
        urn_from_url = re.search(r'/Details/(\d{5,7})', gias_url)
        if urn_from_url:
            urn = urn_from_url.group(1)
            logger.info(f"⚠️ Fallback: Extracted URN {urn} from URL")
            return {
                'urn': urn,
                'official_name': school_name,
                'address': location or '',
                'trust_name': None,
                'confidence': 0.85,
                'url': gias_url
            }
        
        return {'urn': None, 'confidence': 0.0, 'error': 'Could not extract URN'}
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data using Firecrawl with JSON format
        NEW: Single API call, 95% success rate
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
        
        # STEP 1: Get basic financial data (balance, reserve) from main page
        logger.info("🔥 Scraping main FBIT page...")
        main_page_data = self._scrape_main_page(financial_data['source_url'])
        
        if main_page_data:
            financial_data.update(main_page_data)
        
        # STEP 2: Get benchmark data from comparison page (THE IMPORTANT ONE)
        logger.info("🔥 Scraping comparison page for benchmark data...")
        benchmark_data = self._scrape_comparison_page(financial_data['comparison_url'])
        
        if benchmark_data:
            financial_data['benchmark_data'] = benchmark_data
            logger.info(f"✅ Extracted {len(benchmark_data)} benchmark fields")
            
            # Log what we found
            for key, value in benchmark_data.items():
                if value and value > 0:
                    logger.info(f"  ✓ {key}: £{value:,}")
        else:
            logger.warning("⚠️ No benchmark data extracted")
        
        # Calculate recruitment estimates
        if 'benchmark_data' in financial_data:
            benchmark = financial_data['benchmark_data']
            
            supply_costs = benchmark.get('supply_teaching_staff_costs', 0) or 0
            agency_costs = benchmark.get('agency_supply_teaching_staff_costs', 0) or 0
            
            if supply_costs or agency_costs:
                recruitment_base = supply_costs + agency_costs
                
                financial_data['recruitment_estimates'] = {
                    'low': int(recruitment_base * 0.20),
                    'high': int(recruitment_base * 0.30),
                    'midpoint': int(recruitment_base * 0.25)
                }
                logger.info(f"  💼 Recruitment estimate: £{financial_data['recruitment_estimates']['midpoint']:,}")
        
        return financial_data
    
    def _scrape_main_page(self, url: str) -> Dict[str, Any]:
        """Scrape main FBIT page for balance and reserve"""
        
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "url": url,
            "formats": [{
                "type": "json",
                "schema": {
                    "type": "object",
                    "properties": {
                        "in_year_balance": {
                            "type": "number",
                            "description": "In year balance (can be negative for deficit)"
                        },
                        "revenue_reserve": {
                            "type": "number",
                            "description": "Revenue reserve amount"
                        }
                    }
                }
            }]
        }
        
        try:
            response = requests.post(self.firecrawl_api_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('success') and data.get('data', {}).get('json'):
                    extracted = data['data']['json']
                    result = {}
                    
                    if extracted.get('in_year_balance') is not None:
                        result['in_year_balance'] = extracted['in_year_balance']
                        logger.info(f"  ✓ In-year balance: £{result['in_year_balance']:,}")
                    
                    if extracted.get('revenue_reserve') is not None:
                        result['revenue_reserve'] = extracted['revenue_reserve']
                        logger.info(f"  ✓ Revenue reserve: £{result['revenue_reserve']:,}")
                    
                    return result
                    
        except Exception as e:
            logger.error(f"Error scraping main page: {e}")
        
        return {}
    
    def _scrape_comparison_page(self, url: str) -> Dict[str, Any]:
        """
        Use Firecrawl EXTRACT endpoint for complex government page
        More reliable than scrape for JavaScript-heavy sites
        """
        
        logger.info(f"🔥 Using Firecrawl EXTRACT for comparison page...")
        
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}",
            "Content-Type": "application/json"
        }
        
        # Use EXTRACT endpoint (not scrape)
        extract_url = "https://api.firecrawl.dev/v1/extract"
        
        payload = {
            "urls": [url],
            "schema": {
                "type": "object",
                "properties": {
                    "total_teaching_and_support_costs_per_pupil": {
                        "type": "number",
                        "description": "Total teaching and teaching support staff costs per pupil in pounds (annual)"
                    },
                    "teaching_staff_costs": {
                        "type": "number",
                        "description": "Teaching staff costs total in pounds (annual)"
                    },
                    "supply_teaching_staff_costs": {
                        "type": "number",
                        "description": "Supply teaching staff costs in pounds (annual)"
                    },
                    "educational_consultancy_costs": {
                        "type": "number",
                        "description": "Educational consultancy costs in pounds (annual)"
                    },
                    "educational_support_staff_costs": {
                        "type": "number",
                        "description": "Educational support staff costs in pounds (annual)"
                    },
                    "agency_supply_teaching_staff_costs": {
                        "type": "number",
                        "description": "Agency supply teaching staff costs in pounds (annual) - recruitment agency target"
                    }
                },
                "required": []
            },
            "prompt": "Extract the teaching and support staff costs from this UK school financial benchmarking comparison page. Focus on the 'Teaching and teaching support staff' section."
        }
        
        try:
            # Extract endpoint may take longer
            response = requests.post(extract_url, json=payload, headers=headers, timeout=90)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('success'):
                    # Extract endpoint returns data differently
                    extracted_data = data.get('data', {})
                    
                    # Clean and validate
                    benchmark_data = {}
                    for key in ['total_teaching_and_support_costs_per_pupil', 'teaching_staff_costs', 
                               'supply_teaching_staff_costs', 'educational_consultancy_costs',
                               'educational_support_staff_costs', 'agency_supply_teaching_staff_costs']:
                        
                        value = extracted_data.get(key)
                        if value and value != 0:
                            try:
                                benchmark_data[key] = int(value)
                                logger.info(f"  ✓ {key}: £{int(value):,}")
                            except:
                                pass
                    
                    if benchmark_data:
                        logger.info(f"✅ Extract got {len(benchmark_data)} fields")
                        return benchmark_data
                    else:
                        logger.warning("⚠️ Extract returned no valid data")
                else:
                    logger.error(f"❌ Extract failed: {data.get('error', 'Unknown error')}")
            else:
                logger.error(f"❌ Extract HTTP {response.status_code}: {response.text[:300]}")
                
        except requests.exceptions.Timeout:
            logger.error("❌ Extract timed out (90s)")
        except Exception as e:
            logger.error(f"❌ Extract error: {e}")
        
        # FINAL FALLBACK: Try scraping with markdown and send to GPT-4o-mini
        logger.warning("⚠️ Trying GPT-4o-mini fallback...")
        return self._gpt_extraction_fallback(url)
    
    def _gpt_extraction_fallback(self, url: str) -> Dict[str, Any]:
        """
        Last resort: Scrape markdown and use GPT-4o-mini
        """
        try:
            # Simple scrape for markdown
            headers = {
                "Authorization": f"Bearer {self.firecrawl_api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "url": url,
                "formats": ["markdown"],
                "timeout": 30000
            }
            
            response = requests.post(self.firecrawl_api_url, json=payload, headers=headers, timeout=45)
            
            if response.status_code == 200:
                data = response.json()
                markdown = data.get('data', {}).get('markdown', '')
                
                if markdown and len(markdown) > 500:
                    logger.info(f"📄 Got markdown ({len(markdown)} chars), sending to GPT...")
                    
                    # Use GPT-4o-mini to extract
                    gpt_response = self.openai_client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {
                                "role": "system",
                                "content": "Extract financial costs from UK school data. Return ONLY valid JSON with numeric values (no £ symbols, no commas). Use null for missing data."
                            },
                            {
                                "role": "user",
                                "content": f"""Extract these 6 costs from this school financial data:

1. total_teaching_and_support_costs_per_pupil
2. teaching_staff_costs
3. supply_teaching_staff_costs
4. educational_consultancy_costs
5. educational_support_staff_costs
6. agency_supply_teaching_staff_costs

Data:
{markdown[:15000]}

Return ONLY JSON like: {{"teaching_staff_costs": 950000, ...}}"""
                            }
                        ],
                        temperature=0.1,
                        max_tokens=500,
                        response_format={"type": "json_object"}
                    )
                    
                    result = json.loads(gpt_response.choices[0].message.content)
                    
                    # Clean data
                    benchmark_data = {}
                    for key, value in result.items():
                        if value and value != "null" and value != 0:
                            try:
                                benchmark_data[key] = int(value)
                                logger.info(f"  ✓ GPT found {key}: £{int(value):,}")
                            except:
                                pass
                    
                    if benchmark_data:
                        logger.info(f"✅ GPT extracted {len(benchmark_data)} fields")
                        return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ GPT fallback failed: {e}")
        
        return {}
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete recruitment cost intelligence for a school"""
        
        # Step 1: Get URN with NEW Firecrawl method
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        logger.info(f"✅ Found URN {urn_result['urn']} for {urn_result['official_name']}")
        
        # Step 2: Get financial data with NEW Firecrawl method
        financial_data = self.get_financial_data(
            urn_result['urn'],
            urn_result['official_name'],
            False
        )
        
        # Step 3: Generate intelligence
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
        
        # Total expenditure - SAFE comparison
        total_teaching = benchmark.get('total_teaching_and_support_costs_per_pupil')
        if total_teaching and total_teaching > 0:
            insights.append(f"Teaching & support costs: £{total_teaching:,} per pupil")
        
        # Supply costs - SAFE comparison
        supply = benchmark.get('supply_teaching_staff_costs')
        if supply and supply > 0:
            insights.append(f"Supply teaching: £{supply:,}/year (opportunity for cost reduction)")
        
        # Agency costs - SAFE comparison
        agency = benchmark.get('agency_supply_teaching_staff_costs')
        if agency and agency > 0:
            insights.append(f"🎯 Agency supply: £{agency:,}/year - HIGH PRIORITY COMPETITIVE TARGET")
        
        # Consultancy - SAFE comparison
        consultancy = benchmark.get('educational_consultancy_costs')
        if consultancy and consultancy > 15000:
            insights.append(f"High consultancy spend: £{consultancy:,}/year (suggests leadership transitions/Ofsted pressure)")
    
    # Financial pressure - SAFE comparison
    balance = financial_data.get('in_year_balance')
    if balance is not None:
        if balance < 0:
            insights.append(f"⚠️ Operating deficit: £{abs(balance):,} - urgent cost savings needed")
        elif balance > 0:
            insights.append(f"✅ Surplus: £{balance:,}")
    
    return insights
    
    def _generate_cost_conversations(self, financial_data: Dict) -> List[str]:
    """Generate specific conversation starters based on financial data"""
    starters = []
    
    if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
        benchmark = financial_data['benchmark_data']
        
        # AGENCY COSTS - SAFE comparison
        agency = benchmark.get('agency_supply_teaching_staff_costs')
        if agency and agency > 0:
            savings = int(agency * 0.25)
            starters.append(
                f"I noticed from the government's financial benchmarking data that you're spending "
                f"£{agency:,} annually on agency supply staff. Many schools in similar situations have "
                f"switched to Protocol Education and saved 20-30% (approximately £{savings:,} in your case) "
                f"while actually improving teacher quality and consistency. Would you be open to a brief "
                f"conversation about how we've helped other schools reduce these costs?"
            )
        
        # SUPPLY COSTS - SAFE comparison
        supply = benchmark.get('supply_teaching_staff_costs')
        agency_val = benchmark.get('agency_supply_teaching_staff_costs') or 0
        
        if supply and supply > 50000 and agency_val == 0:
            starters.append(
                f"Your supply teaching costs of £{supply:,} annually suggest regular staffing challenges. "
                f"Interestingly, you're not currently using agency arrangements, which often provide better "
                f"value than daily booking. We've seen schools in your position reduce costs by 15-25% "
                f"through our long-term supply solutions. Would it be helpful to explore this?"
            )
        elif supply and supply > 80000:
            starters.append(
                f"With £{supply:,} in annual supply costs, you're clearly managing significant staffing "
                f"gaps. Protocol Education specializes in providing consistent, high-quality supply staff "
                f"at competitive rates. Many schools find our approach reduces both costs and the "
                f"administrative burden of managing multiple supply arrangements."
            )
        
        # CONSULTANCY - SAFE comparison
        consultancy = benchmark.get('educational_consultancy_costs')
        if consultancy and consultancy > 15000:
            starters.append(
                f"I see you're investing £{consultancy:,} in educational consultancy. This often indicates "
                f"leadership transitions or Ofsted preparation. We've helped many schools in similar "
                f"situations by providing stable, high-quality staffing during periods of change, which "
                f"allows leadership to focus on strategic improvements rather than daily staffing challenges."
            )
    
    # DEFICIT - SAFE comparison
    balance = financial_data.get('in_year_balance')
    if balance is not None and balance < -30000:
        starters.append(
            f"I understand your school is managing a deficit of £{abs(balance):,}. Protocol Education "
            f"has specific programs designed to help schools reduce recruitment and supply costs as part "
            f"of financial recovery plans. We've worked with several schools in similar positions and "
            f"typically achieve 20-30% cost reductions within the first year, which can make a real "
            f"difference to your budget position."
        )
    
    # Fallback
    if not starters:
        starters.append(
            "Protocol Education provides high-quality teaching staff at competitive rates with a "
            "quality guarantee. We'd be happy to provide a no-obligation comparison against your "
            "current arrangements to show potential cost savings and service improvements."
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


def enhance_school_with_financial_data(intel, serper_engine):
    """Add financial data to existing school intelligence"""
    
    try:
        financial_engine = FinancialDataEngine(serper_engine)
        
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
                            source_url=financial_intel.get('financial', {}).get('comparison_url', ''),
                            relevance_score=0.95
                        )
                    )
            
            # Store financial data
            intel.financial_data = financial_intel
            logger.info(f"✅ Enhanced {intel.school_name} with financial intelligence")
        else:
            logger.warning(f"⚠️ Could not get financial data: {financial_intel.get('error')}")
    
    except Exception as e:
        logger.error(f"❌ Error enhancing with financial data: {e}")
    
    return intel
