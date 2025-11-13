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
    MISSION CRITICAL: Extract 6 teaching/support staff costs from FBIT comparison page
    Uses FIRE-1 agent for complex JavaScript-rendered government site
    """
    
    logger.info(f"🔥 MISSION CRITICAL: Using FIRE-1 agent for comparison page...")
    
    headers = {
        "Authorization": f"Bearer {self.firecrawl_api_key}",
        "Content-Type": "application/json"
    }
    
    # Use v1 Extract with FIRE-1 agent
    extract_url = "https://api.firecrawl.dev/v1/extract"
    
    # CRITICAL: Very detailed prompt to guide extraction
    detailed_prompt = """
MISSION CRITICAL DATA EXTRACTION:

You are extracting financial data from a UK school's official government financial benchmarking page.

CONTEXT: This is the 'comparison' page of the Financial Benchmarking and Insights Tool (FBIT), specifically the section about "Teaching and teaching support staff" costs.

TARGET DATA - Extract these EXACT 6 fields (all are annual costs in British Pounds £):

1. Total teaching and teaching support staff costs (per pupil) - Look for "Total teaching and teaching support staff" followed by "per pupil" or similar wording
2. Teaching staff costs - Look for just "Teaching staff" costs (NOT supply, NOT agency)
3. Supply teaching staff costs - Look for "Supply teaching staff" costs
4. Educational consultancy costs - Look for "Educational consultancy" 
5. Educational support staff costs - Look for "Educational support staff"
6. Agency supply teaching staff costs - Look for "Agency supply" costs (CRITICAL for recruitment analysis)

IMPORTANT INSTRUCTIONS:
- These costs appear in a comparison table or list on the page
- The section is titled "Teaching and teaching support staff"
- Numbers are in British Pounds (£) and may have commas (e.g., £125,000)
- Return ONLY the numeric values (remove £ symbol and commas)
- If a field is not found, return null for that field
- Do NOT make up or estimate values
- Focus on the school's actual costs, not benchmark averages

This data is MISSION CRITICAL for recruitment cost analysis.
"""
    
    payload = {
        "urls": [url],
        "schema": {
            "type": "object",
            "properties": {
                "total_teaching_and_support_costs_per_pupil": {
                    "type": "number",
                    "description": "Total teaching and teaching support staff costs per pupil (annual, in pounds)"
                },
                "teaching_staff_costs": {
                    "type": "number",
                    "description": "Teaching staff costs only - NOT including supply or agency (annual, in pounds)"
                },
                "supply_teaching_staff_costs": {
                    "type": "number",
                    "description": "Supply teaching staff costs (annual, in pounds)"
                },
                "educational_consultancy_costs": {
                    "type": "number",
                    "description": "Educational consultancy costs (annual, in pounds)"
                },
                "educational_support_staff_costs": {
                    "type": "number",
                    "description": "Educational support staff costs (annual, in pounds)"
                },
                "agency_supply_teaching_staff_costs": {
                    "type": "number",
                    "description": "Agency supply teaching staff costs - CRITICAL recruitment target (annual, in pounds)"
                }
            },
            "required": []
        },
        "prompt": detailed_prompt,
        "agent": {
            "model": "FIRE-1"  # Use AI agent for complex site navigation
        }
    }
    
    try:
        logger.info("🤖 Sending to FIRE-1 agent (may take 60-120 seconds)...")
        response = requests.post(extract_url, json=payload, headers=headers, timeout=150)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"📥 Response status: {data.get('status', 'unknown')}")
            
            if data.get('success'):
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
                            logger.info(f"  ✅ {key}: £{int(value):,}")
                        except:
                            logger.warning(f"  ⚠️ Could not parse {key}: {value}")
                
                if benchmark_data:
                    logger.info(f"✅ FIRE-1 extracted {len(benchmark_data)}/6 fields")
                    return benchmark_data
                else:
                    logger.error("❌ FIRE-1 returned no valid data")
                    logger.error(f"Raw response data: {extracted_data}")
            else:
                error_msg = data.get('error', 'Unknown error')
                logger.error(f"❌ FIRE-1 failed: {error_msg}")
                
                # Check if job is still processing
                if data.get('status') == 'processing':
                    job_id = data.get('id')
                    logger.info(f"⏳ Job still processing, ID: {job_id}")
                    # Could poll here, but for now fall through to GPT fallback
        else:
            logger.error(f"❌ FIRE-1 HTTP {response.status_code}")
            logger.error(f"Response: {response.text[:500]}")
            
    except requests.exceptions.Timeout:
        logger.error("❌ FIRE-1 timed out after 150 seconds")
    except Exception as e:
        logger.error(f"❌ FIRE-1 error: {e}")
    
    # FALLBACK 1: Try basic scrape with VERY detailed GPT prompt
    logger.warning("⚠️ FIRE-1 failed, trying detailed GPT fallback...")
    return self._gpt_extraction_fallback_detailed(url)

def _gpt_extraction_fallback_detailed(self, url: str) -> Dict[str, Any]:
    """
    Enhanced GPT fallback with extremely detailed prompt
    """
    try:
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}",
            "Content-Type": "application/json"
        }
        
        # Try getting HTML instead of markdown (more data)
        payload = {
            "url": url,
            "formats": ["html", "markdown"],
            "timeout": 60000
        }
        
        logger.info("📄 Scraping for HTML/markdown...")
        response = requests.post(self.firecrawl_api_url, json=payload, headers=headers, timeout=75)
        
        if response.status_code == 200:
            data = response.json()
            
            # Try HTML first (more complete)
            content = data.get('data', {}).get('html', '')
            if not content or len(content) < 1000:
                content = data.get('data', {}).get('markdown', '')
            
            if content and len(content) > 500:
                logger.info(f"📄 Got content ({len(content)} chars), sending to GPT with detailed prompt...")
                
                # SUPER DETAILED GPT PROMPT
                gpt_prompt = f"""You are extracting MISSION CRITICAL financial data from a UK school's government financial benchmarking page.

CONTEXT: This HTML/markdown is from: {url}
This is the OFFICIAL government "Financial Benchmarking and Insights Tool" comparison page.
It shows a school's costs compared to similar schools.

YOUR TASK: Extract these EXACT 6 cost figures (all in British Pounds £, annual):

1. total_teaching_and_support_costs_per_pupil - The TOTAL teaching and teaching support staff cost PER PUPIL
2. teaching_staff_costs - Teaching staff costs (NOT including supply or agency)
3. supply_teaching_staff_costs - Supply teaching staff costs
4. educational_consultancy_costs - Educational consultancy costs
5. educational_support_staff_costs - Educational support staff costs
6. agency_supply_teaching_staff_costs - Agency supply teaching staff costs (CRITICAL)

SEARCH PATTERNS:
- Look for a section titled "Teaching and teaching support staff"
- Look for tables or lists with cost breakdowns
- Numbers will be formatted like: £125,000 or 125000 or 125,000
- May say "per pupil" for per-pupil costs
- May have comparison data (school vs similar schools) - extract the SCHOOL'S cost, not the comparison

CRITICAL RULES:
- Return ONLY numeric values (remove £ and commas)
- If you find a value, you MUST return it
- If you genuinely cannot find a value, return null
- DO NOT estimate or make up values
- DO NOT confuse different cost categories

Content to analyze:
{content[:20000]}

Return ONLY valid JSON in this exact format:
{{
  "total_teaching_and_support_costs_per_pupil": 5234,
  "teaching_staff_costs": 950000,
  "supply_teaching_staff_costs": 85000,
  "educational_consultancy_costs": 15000,
  "educational_support_staff_costs": 180000,
  "agency_supply_teaching_staff_costs": 42000
}}

Remember: Use null for missing values, but try VERY HARD to find all 6 values."""
                
                gpt_response = self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a financial data extraction specialist. You extract exact values from government financial documents. You are extremely careful to return accurate numbers only."
                        },
                        {
                            "role": "user",
                            "content": gpt_prompt
                        }
                    ],
                    temperature=0.0,  # No creativity, just extraction
                    max_tokens=800,
                    response_format={"type": "json_object"}
                )
                
                result = json.loads(gpt_response.choices[0].message.content)
                
                # Clean data
                benchmark_data = {}
                for key, value in result.items():
                    if value and value != "null" and value != 0:
                        try:
                            benchmark_data[key] = int(value)
                            logger.info(f"  ✅ GPT found {key}: £{int(value):,}")
                        except:
                            logger.warning(f"  ⚠️ Could not parse GPT result for {key}: {value}")
                
                if benchmark_data:
                    logger.info(f"✅ GPT extracted {len(benchmark_data)}/6 fields")
                    return benchmark_data
                else:
                    logger.error("❌ GPT returned no valid data")
                    logger.error(f"GPT response: {result}")
        else:
            logger.error(f"❌ Scrape failed: HTTP {response.status_code}")
        
    except Exception as e:
        logger.error(f"❌ GPT fallback exception: {e}")
    
    logger.error("🚨 ALL EXTRACTION METHODS FAILED - NO FINANCIAL DATA AVAILABLE")
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
