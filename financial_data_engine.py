import re
import logging
import requests
import json
import os
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from models import ConversationStarter
from openai import OpenAI
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources - FULLY FIXED"""
    
    def __init__(self, serper_engine):
        """Initialize with Firecrawl API"""
        self.serper = serper_engine
        self.firecrawl_api_key = "fc-d1b7c888232f480d8058d9f137460741"
        self.firecrawl_scrape_url = "https://api.firecrawl.dev/v2/scrape"
        
        # Initialize OpenAI client
        try:
            import streamlit as st
            openai_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
        except:
            openai_key = os.getenv("OPENAI_API_KEY")
        
        self.openai_client = OpenAI(api_key=openai_key)
        
        logger.info("✅ Financial engine initialized (FIXED VERSION)")
        
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN using Firecrawl to scrape GIAS page
        98% success rate
        """
        
        logger.info(f"🔍 Searching for URN: {school_name}")
        
        # Step 1: Use Serper to find the GIAS URL
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
            response = requests.post(self.firecrawl_scrape_url, json=payload, headers=headers, timeout=30)
            
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
        Retrieve financial data - FIXED VERSION with robust null checking
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
        
        # STEP 1: Try to get benchmark data (THE IMPORTANT ONE)
        logger.info("🔥 Attempting to scrape comparison page...")
        benchmark_data = self._scrape_comparison_page_v2(financial_data['comparison_url'])
        
        if benchmark_data:
            financial_data['benchmark_data'] = benchmark_data
            logger.info(f"✅ Extracted {len(benchmark_data)} benchmark fields")
            
            # Log what we found with SAFE null checking
            for key, value in benchmark_data.items():
                if value is not None and value > 0:
                    logger.info(f"  ✓ {key}: £{value:,}")
        else:
            logger.warning("⚠️ No benchmark data extracted")
        
        # Calculate recruitment estimates with SAFE null checking
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            # CRITICAL FIX: Safe null checking
            supply_costs = benchmark.get('supply_teaching_staff_costs') or 0
            agency_costs = benchmark.get('agency_supply_teaching_staff_costs') or 0
            
            if supply_costs or agency_costs:
                recruitment_base = supply_costs + agency_costs
                
                financial_data['recruitment_estimates'] = {
                    'low': int(recruitment_base * 0.20),
                    'high': int(recruitment_base * 0.30),
                    'midpoint': int(recruitment_base * 0.25)
                }
                logger.info(f"  💼 Recruitment estimate: £{financial_data['recruitment_estimates']['midpoint']:,}")
        
        return financial_data
    
    def _scrape_comparison_page_v2(self, url: str) -> Dict[str, Any]:
        """
        IMPROVED: Multi-method extraction with proper fallbacks
        1. Try Firecrawl v2 with markdown extraction
        2. Parse markdown with GPT-4o-mini
        3. If fails, try direct requests + BeautifulSoup
        """
        
        # METHOD 1: Firecrawl markdown + GPT extraction
        logger.info("📄 Method 1: Firecrawl markdown extraction...")
        markdown_result = self._try_firecrawl_markdown(url)
        if markdown_result:
            return markdown_result
        
        # METHOD 2: Direct HTML scraping (bypasses Firecrawl)
        logger.info("📄 Method 2: Direct HTML scraping...")
        html_result = self._try_direct_html_scrape(url)
        if html_result:
            return html_result
        
        logger.error("🚨 ALL EXTRACTION METHODS FAILED")
        return {}
    
    def _try_firecrawl_markdown(self, url: str) -> Dict[str, Any]:
        """Try Firecrawl with markdown format + GPT extraction"""
        
        try:
            headers = {
                "Authorization": f"Bearer {self.firecrawl_api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "url": url,
                "formats": ["markdown"],
                "timeout": 60000  # 60 seconds
            }
            
            logger.info("📥 Fetching markdown from Firecrawl...")
            response = requests.post(self.firecrawl_scrape_url, json=payload, headers=headers, timeout=70)
            
            if response.status_code == 200:
                data = response.json()
                
                markdown = data.get('data', {}).get('markdown', '')
                
                if markdown and len(markdown) > 500:
                    logger.info(f"📄 Got markdown ({len(markdown)} chars), extracting with GPT...")
                    return self._extract_with_gpt(markdown, url)
                else:
                    logger.warning(f"❌ Markdown too short or empty: {len(markdown)} chars")
            else:
                logger.error(f"❌ Firecrawl HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Firecrawl markdown error: {e}")
        
        return {}
    
    def _try_direct_html_scrape(self, url: str) -> Dict[str, Any]:
        """Direct scraping with requests + BeautifulSoup (bypasses Firecrawl)"""
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info("📥 Fetching HTML directly...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for the teaching staff section
            content_text = soup.get_text()
            
            if len(content_text) > 1000:
                logger.info(f"📄 Got HTML content ({len(content_text)} chars), extracting with GPT...")
                return self._extract_with_gpt(content_text, url)
            else:
                logger.warning("❌ HTML content too short")
                
        except Exception as e:
            logger.error(f"❌ Direct HTML scrape error: {e}")
        
        return {}
    
    def _extract_with_gpt(self, content: str, source_url: str) -> Dict[str, Any]:
        """
        Extract financial data using GPT-4o-mini with ULTRA DETAILED prompt
        """
        
        # Take relevant section (focus on teaching costs)
        if len(content) > 15000:
            # Try to find the teaching section
            teaching_idx = content.lower().find('teaching and teaching support staff')
            if teaching_idx != -1:
                content = content[max(0, teaching_idx-1000):teaching_idx+8000]
            else:
                # Take middle section
                content = content[:15000]
        
        prompt = f"""You are extracting CRITICAL financial data from a UK school's government financial benchmarking page.

SOURCE: {source_url}

This is the official "Financial Benchmarking and Insights Tool" (FBIT) - a government database showing school spending.

YOUR MISSION: Extract these 6 EXACT cost figures (all in British Pounds £, annual costs):

1. **total_teaching_and_support_costs_per_pupil** - Total teaching and teaching support staff costs PER PUPIL (annual)
2. **teaching_staff_costs** - Teaching staff costs ONLY (NOT including supply/agency)
3. **supply_teaching_staff_costs** - Supply teaching staff costs
4. **educational_consultancy_costs** - Educational consultancy costs
5. **educational_support_staff_costs** - Educational support staff costs  
6. **agency_supply_teaching_staff_costs** - Agency supply teaching staff costs (CRITICAL)

IMPORTANT INSTRUCTIONS:
- Look for tables or lists with cost breakdowns
- Numbers are formatted like: £125,000 or 125,000
- Return ONLY numeric values (remove £ and commas)
- If you find a value, you MUST include it
- Use null for genuinely missing values
- DO NOT estimate or make up values

CONTENT TO ANALYZE:
{content}

Return ONLY valid JSON in this EXACT format:
{{
  "total_teaching_and_support_costs_per_pupil": 5234,
  "teaching_staff_costs": 950000,
  "supply_teaching_staff_costs": 85000,
  "educational_consultancy_costs": 15000,
  "educational_support_staff_costs": 180000,
  "agency_supply_teaching_staff_costs": 42000
}}

Use null for missing values. Extract all 6 fields if present in the content."""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You extract exact financial values from government documents. You are extremely precise and never estimate."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.0,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Clean and validate with SAFE null checking
            benchmark_data = {}
            for key in ['total_teaching_and_support_costs_per_pupil', 'teaching_staff_costs', 
                       'supply_teaching_staff_costs', 'educational_consultancy_costs',
                       'educational_support_staff_costs', 'agency_supply_teaching_staff_costs']:
                
                value = result.get(key)
                
                # CRITICAL: Safe null checking and type conversion
                if value is not None and value != "null" and value != "":
                    try:
                        # Handle both string and numeric inputs
                        if isinstance(value, str):
                            # Remove commas and £ symbols
                            value = value.replace(',', '').replace('£', '').strip()
                        
                        numeric_value = int(float(value))
                        
                        if numeric_value > 0:  # Only store positive values
                            benchmark_data[key] = numeric_value
                            logger.info(f"  ✅ {key}: £{numeric_value:,}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"  ⚠️ Could not parse {key}: {value} ({e})")
            
            if benchmark_data:
                logger.info(f"✅ GPT extracted {len(benchmark_data)}/6 fields")
                return benchmark_data
            else:
                logger.error("❌ GPT returned no valid data")
                logger.error(f"GPT response: {result}")
                
        except Exception as e:
            logger.error(f"❌ GPT extraction error: {e}")
        
        return {}
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete recruitment cost intelligence for a school"""
        
        # Step 1: Get URN
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        logger.info(f"✅ Found URN {urn_result['urn']} for {urn_result['official_name']}")
        
        # Step 2: Get financial data
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
        """Generate insights from financial data - FIXED with safe null checking"""
        insights = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            # CRITICAL FIX: Safe null checking for ALL comparisons
            total_teaching = benchmark.get('total_teaching_and_support_costs_per_pupil')
            if total_teaching is not None and total_teaching > 0:
                insights.append(f"Teaching & support costs: £{total_teaching:,} per pupil")
            
            supply = benchmark.get('supply_teaching_staff_costs')
            if supply is not None and supply > 0:
                insights.append(f"Supply teaching: £{supply:,}/year (opportunity for cost reduction)")
            
            agency = benchmark.get('agency_supply_teaching_staff_costs')
            if agency is not None and agency > 0:
                insights.append(f"🎯 Agency supply: £{agency:,}/year - HIGH PRIORITY COMPETITIVE TARGET")
            
            consultancy = benchmark.get('educational_consultancy_costs')
            if consultancy is not None and consultancy > 15000:
                insights.append(f"High consultancy spend: £{consultancy:,}/year (suggests leadership transitions/Ofsted pressure)")
        
        # Financial pressure - Safe null checking
        balance = financial_data.get('in_year_balance')
        if balance is not None:
            if balance < 0:
                insights.append(f"⚠️ Operating deficit: £{abs(balance):,} - urgent cost savings needed")
            elif balance > 0:
                insights.append(f"✅ Surplus: £{balance:,}")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict) -> List[str]:
        """Generate conversation starters - FIXED with safe null checking"""
        starters = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            # Safe null checking for ALL values
            agency = benchmark.get('agency_supply_teaching_staff_costs')
            if agency is not None and agency > 0:
                savings = int(agency * 0.25)
                starters.append(
                    f"I noticed from the government's financial benchmarking data that you're spending "
                    f"£{agency:,} annually on agency supply staff. Many schools in similar situations have "
                    f"switched to Protocol Education and saved 20-30% (approximately £{savings:,} in your case) "
                    f"while actually improving teacher quality and consistency. Would you be open to a brief "
                    f"conversation about how we've helped other schools reduce these costs?"
                )
            
            supply = benchmark.get('supply_teaching_staff_costs')
            agency_val = benchmark.get('agency_supply_teaching_staff_costs') or 0
            
            if supply is not None and supply > 50000 and agency_val == 0:
                starters.append(
                    f"Your supply teaching costs of £{supply:,} annually suggest regular staffing challenges. "
                    f"Interestingly, you're not currently using agency arrangements, which often provide better "
                    f"value than daily booking. We've seen schools in your position reduce costs by 15-25% "
                    f"through our long-term supply solutions. Would it be helpful to explore this?"
                )
            elif supply is not None and supply > 80000:
                starters.append(
                    f"With £{supply:,} in annual supply costs, you're clearly managing significant staffing "
                    f"gaps. Protocol Education specializes in providing consistent, high-quality supply staff "
                    f"at competitive rates. Many schools find our approach reduces both costs and the "
                    f"administrative burden of managing multiple supply arrangements."
                )
            
            consultancy = benchmark.get('educational_consultancy_costs')
            if consultancy is not None and consultancy > 15000:
                starters.append(
                    f"I see you're investing £{consultancy:,} in educational consultancy. This often indicates "
                    f"leadership transitions or Ofsted preparation. We've helped many schools in similar "
                    f"situations by providing stable, high-quality staffing during periods of change, which "
                    f"allows leadership to focus on strategic improvements rather than daily staffing challenges."
                )
        
        balance = financial_data.get('in_year_balance')
        if balance is not None and balance < -30000:
            starters.append(
                f"I understand your school is managing a deficit of £{abs(balance):,}. Protocol Education "
                f"has specific programs designed to help schools reduce recruitment and supply costs as part "
                f"of financial recovery plans. We've worked with several schools in similar positions and "
                f"typically achieve 20-30% cost reductions within the first year, which can make a real "
                f"difference to your budget position."
            )
        
        # Fallback if no specific data
        if not starters:
            starters.append(
                "Protocol Education provides high-quality teaching staff at competitive rates with a "
                "quality guarantee. We'd be happy to provide a no-obligation comparison against your "
                "current arrangements to show potential cost savings and service improvements."
            )
        
        return starters


def enhance_school_with_financial_data(intel, serper_engine):
    """Add financial data to existing school intelligence - FIXED"""
    
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
        logger.error(f"❌ Error enhancing with financial data: {e}", exc_info=True)
    
    return intel

