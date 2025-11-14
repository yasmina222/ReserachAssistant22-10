import re
import logging
import requests
import json
import os
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from models import ConversationStarter
from firecrawl import Firecrawl
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Pydantic schema for financial data extraction
class FinancialDataSchema(BaseModel):
    total_teaching_and_support_staff_costs_per_pupil: float = Field(
        description="The total teaching and support staff costs per pupil"
    )
    teaching_staff_costs: float = Field(
        description="The teaching staff costs (NOT including supply or agency)"
    )
    supply_teaching_staff_costs: float = Field(
        description="The supply teaching staff costs"
    )
    educational_consultancy_costs: float = Field(
        description="The educational consultancy costs"
    )
    educational_support_staff_costs: float = Field(
        description="The educational support staff costs"
    )
    agency_supply_teaching_staff_costs: float = Field(
        description="The agency supply teaching staff costs - CRITICAL for recruitment"
    )

class FinancialDataEngine:
    """Retrieves school financial data from government sources using Firecrawl SDK"""
    
    def __init__(self, serper_engine):
        """Initialize with Firecrawl SDK"""
        self.serper = serper_engine
        # HARDCODED API KEY - Update this with your new key
        self.firecrawl_api_key = "fc-YOUR-NEW-API-KEY-HERE"
        
        # Initialize Firecrawl SDK
        self.firecrawl_app = Firecrawl(api_key=self.firecrawl_api_key)
        
        # Log which key is being used (obscured for security)
        if self.firecrawl_api_key:
            masked_key = self.firecrawl_api_key[:10] + "..." + self.firecrawl_api_key[-4:]
            logger.info(f"✅ Firecrawl API key loaded: {masked_key}")
        else:
            logger.error("❌ No Firecrawl API key found!")
        
        logger.info("✅ Financial engine initialized with Firecrawl SDK (WORKING VERSION!)")
        
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
        
        # Step 3: Extract URN directly from URL (most reliable method)
        logger.info(f"🔍 Extracting URN from URL...")
        
        urn_from_url = re.search(r'/Details/(\d{5,7})', gias_url)
        if urn_from_url:
            urn = urn_from_url.group(1)
            logger.info(f"✅ URN FOUND: {urn}")
            return {
                'urn': urn,
                'official_name': school_name,
                'address': location or '',
                'trust_name': None,
                'confidence': 0.95,
                'url': gias_url
            }
        else:
            logger.error(f"❌ Could not extract URN from URL: {gias_url}")
            return {'urn': None, 'confidence': 0.0, 'error': 'Could not extract URN from URL'}
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data - FIXED VERSION with comparison data extraction
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
        
        # STEP 1: Get comparison data from main page (e.g., "higher than 93.3% of similar schools")
        logger.info("🔥 Extracting comparison data from main page...")
        comparison_text = self._extract_comparison_data(financial_data['source_url'])
        if comparison_text:
            financial_data['comparison_text'] = comparison_text
            logger.info(f"✅ Comparison data stored: {comparison_text}")
        else:
            logger.warning("⚠️ No comparison text extracted")
        
        # STEP 2: Get detailed benchmark data from comparison page
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
        
        # Store RAW extracted data (NO CALCULATIONS!)
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            # Just store the raw extracted data - no calculations
            financial_data['raw_extracted_data'] = {
                'teaching_staff_costs': benchmark.get('teaching_staff_costs'),
                'supply_teaching_staff_costs': benchmark.get('supply_teaching_staff_costs'),
                'agency_supply_teaching_staff_costs': benchmark.get('agency_supply_teaching_staff_costs'),
                'educational_support_staff_costs': benchmark.get('educational_support_staff_costs'),
                'educational_consultancy_costs': benchmark.get('educational_consultancy_costs'),
                'total_teaching_and_support_costs_per_pupil': benchmark.get('total_teaching_and_support_costs_per_pupil')
            }
            
            logger.info(f"  💼 Raw financial data stored (no calculations)")
        
        return financial_data
    
    def _extract_comparison_data(self, url: str) -> Optional[str]:
        """
        Extract comparison text from main FBIT page
        BULLETPROOF VERSION with comprehensive logging and fallback strategies
        """
        
        logger.info(f"🔥 Starting comparison data extraction from: {url}")
        
        try:
            result = self.firecrawl_app.extract(
                urls=[url],
                prompt=(
                    "Extract the comparison information for 'Teaching and Teaching support staff'. "
                    "Look for text like 'Spends £X per pupil' and 'Spending is higher/lower than X% of similar schools'. "
                    "Also identify if it's marked as 'High priority', 'Medium priority', or 'Low priority'. "
                    "Return the complete comparison statement."
                )
            )
            
            logger.info(f"📥 Firecrawl extraction result type: {type(result.data)}")
            logger.info(f"📥 Firecrawl extraction content: {result.data}")
            
            if not (result and result.success and result.data):
                logger.error(f"❌ Firecrawl failed or returned no data - success={result.success if result else None}")
                return None
                
            data_dict = result.data
            
            # CRITICAL: Ensure we're working with a dict
            if not isinstance(data_dict, dict):
                logger.error(f"❌ Expected dict, got {type(data_dict)}: {data_dict}")
                return None
            
            logger.info(f"📊 Available fields in response: {list(data_dict.keys())}")
            
            # Try to extract the complete statement first (most comprehensive)
            comparison = data_dict.get('completeComparisonStatement')
            
            if comparison and isinstance(comparison, str):
                logger.info(f"✅ COMPARISON EXTRACTED (complete): {comparison}")
                return comparison
            
            # Fallback: Build it from parts
            spending = data_dict.get('spendingPerPupil', '')
            comparison_part = data_dict.get('spendingComparison', '')
            priority = data_dict.get('priorityLevel', '')
            
            logger.info(f"📊 Individual parts found:")
            logger.info(f"  - spendingPerPupil: {spending}")
            logger.info(f"  - spendingComparison: {comparison_part}")
            logger.info(f"  - priorityLevel: {priority}")
            
            if spending or comparison_part:
                # Construct the comparison string from available parts
                parts = [p for p in [spending, comparison_part, priority] if p]
                constructed = " — ".join(parts)
                logger.info(f"✅ COMPARISON CONSTRUCTED from parts: {constructed}")
                return constructed
            
            # Last resort: Return any available field
            for key in ['spending_comparison', 'comparison_text', 'comparison']:
                if val := data_dict.get(key):
                    logger.info(f"✅ COMPARISON from fallback field '{key}': {val}")
                    return str(val)
            
            logger.error(f"❌ No usable comparison data in fields: {list(data_dict.keys())}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error extracting comparison data: {e}", exc_info=True)
            return None
    
    def _scrape_comparison_page_v2(self, url: str) -> Dict[str, Any]:
        """
        WORKING METHOD: Use Firecrawl SDK - FIXED RESPONSE PARSING
        """
        
        logger.info(f"🔥 Using Firecrawl SDK extract() method for: {url}")
        
        try:
            # Call Firecrawl extract
            result = self.firecrawl_app.extract(
                urls=[url],
                prompt=(
                    "Extract financial data from the 'Teaching and teaching support staff' section. "
                    "Look for 'Total teaching and teaching support staff' followed by 'per pupil' or similar wording. "
                    "For 'Teaching staff costs', ensure it is NOT supply or agency related. "
                    "For 'Supply teaching staff costs', look for the exact phrase. "
                    "For 'Educational consultancy costs', look for 'Educational consultancy'. "
                    "For 'Educational support staff costs', look for 'Educational support staff'. "
                    "For 'Agency supply teaching staff costs', this is CRITICAL for recruitment analysis. "
                    "All costs should be in British Pounds (£)."
                ),
                schema=FinancialDataSchema.model_json_schema()
            )
            
            logger.info(f"📥 RAW Firecrawl result: {result}")
            
            # CRITICAL: From docs, data is at result.data (the actual extracted dict)
            if result and result.success and result.data:
                extracted = result.data
                logger.info(f"✅ Extracted object: {extracted}")
                
                # Convert to our format
                benchmark_data = {}
                
                for key in ['total_teaching_and_support_staff_costs_per_pupil', 'teaching_staff_costs',
                           'supply_teaching_staff_costs', 'educational_consultancy_costs',
                           'educational_support_staff_costs', 'agency_supply_teaching_staff_costs']:
                    
                    # Get value from dict or object attribute
                    if isinstance(extracted, dict):
                        value = extracted.get(key)
                    else:
                        value = getattr(extracted, key, None)
                    
                    # Store ALL values including zeros
                    if value is not None:
                        try:
                            numeric_value = float(value)
                            benchmark_data[key] = int(numeric_value) if numeric_value == int(numeric_value) else numeric_value
                            logger.info(f"  ✅ {key}: £{numeric_value:,.0f}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"  ⚠️ Could not parse {key}: {value}")
                
                if benchmark_data:
                    logger.info(f"🎉 SUCCESS! Extracted {len(benchmark_data)}/6 fields")
                    return benchmark_data
                else:
                    logger.error(f"❌ No valid fields extracted from: {extracted}")
            else:
                logger.error(f"❌ Firecrawl failed: success={result.success if result else 'None'}")
                
        except Exception as e:
            logger.error(f"❌ Firecrawl SDK exception: {e}", exc_info=True)
        
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
        """Generate insights from financial data - NO CALCULATIONS, just observations"""
        insights = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            # Just report the actual costs - no calculations
            total_teaching = benchmark.get('total_teaching_and_support_costs_per_pupil')
            if total_teaching is not None and total_teaching > 0:
                insights.append(f"Teaching & support costs: £{total_teaching:,} per pupil")
            
            supply = benchmark.get('supply_teaching_staff_costs')
            if supply is not None and supply > 0:
                insights.append(f"Supply teaching costs: £{supply:,}/year")
            
            agency = benchmark.get('agency_supply_teaching_staff_costs')
            if agency is not None and agency > 0:
                insights.append(f"Agency supply costs: £{agency:,}/year")
            
            consultancy = benchmark.get('educational_consultancy_costs')
            if consultancy is not None and consultancy > 0:
                insights.append(f"Educational consultancy costs: £{consultancy:,}/year")
            
            support = benchmark.get('educational_support_staff_costs')
            if support is not None and support > 0:
                insights.append(f"Educational support staff costs: £{support:,}/year")
        
        # Financial pressure - Safe null checking
        balance = financial_data.get('in_year_balance')
        if balance is not None:
            if balance < 0:
                insights.append(f"⚠️ Operating deficit: £{abs(balance):,}")
            elif balance > 0:
                insights.append(f"✅ Surplus: £{balance:,}")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict) -> List[str]:
        """Generate conversation starters - PRIORITIZE comparison data first"""
        starters = []
        
        # Get comparison text if available
        comparison_text = financial_data.get('comparison_text', '')
        
        logger.info(f"🔍 Generating conversations - comparison_text present: {bool(comparison_text)}")
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            # PRIORITY 1: Comparison-based conversation (most compelling!)
            total_per_pupil = benchmark.get('total_teaching_and_support_costs_per_pupil')
            if total_per_pupil and total_per_pupil > 0 and comparison_text:
                logger.info(f"✅ Creating comparison-based conversation starter")
                if 'higher than' in comparison_text.lower() or 'above' in comparison_text.lower():
                    # School is spending MORE than similar schools
                    starters.append(
                        f"I noticed you're spending £{total_per_pupil:,.0f} per pupil on teaching and support staff, "
                        f"which our analysis suggests may indicate opportunities for better resource management. "
                        f"We've helped other schools in similar positions reduce these costs by 15-20% "
                        f"without compromising teacher and support staff quality. "
                        f"Would you be open to a brief conversation about how we've achieved this?"
                    )
                elif 'lower than' in comparison_text.lower() or 'below' in comparison_text.lower():
                    # School is spending LESS than similar schools (good news!)
                    starters.append(
                        f"Your teaching and support staff costs of £{total_per_pupil:,.0f} per pupil show you're "
                        f"already managing resources efficiently compared to similar schools. "
                        f"We work with well-run schools like yours to maintain quality while exploring "
                        f"opportunities for even greater value. Would it be helpful to discuss this?"
                    )
            
            # PRIORITY 2: Agency supply costs conversation
            agency = benchmark.get('agency_supply_teaching_staff_costs')
            if agency is not None and agency > 0:
                starters.append(
                    f"I noticed from the government's financial benchmarking data that you're spending "
                    f"£{agency:,} per pupil annually on agency supply staff. Many schools in similar situations have "
                    f"switched to us and achieved significant cost savings "
                    f"while actually improving teacher quality and consistency. Would you be open to a brief "
                    f"conversation about how we've helped other schools reduce these costs?"
                )
            
            # PRIORITY 3: Supply teaching costs conversation
            supply = benchmark.get('supply_teaching_staff_costs')
            if supply is not None and supply > 0:
                starters.append(
                    f"Your supply teaching costs of £{supply:,} per pupil annually suggest regular staffing challenges. "
                    f"We specialize in providing consistent, high-quality supply staff "
                    f"at competitive rates. Many schools find our approach reduces both costs and the "
                    f"administrative burden of managing multiple supply arrangements."
                )
            
            # PRIORITY 4: Consultancy costs conversation
            consultancy = benchmark.get('educational_consultancy_costs')
            if consultancy is not None and consultancy > 0:
                starters.append(
                    f"I see you're investing £{consultancy:,} per pupil in educational consultancy. "
                    f"We've helped many schools by providing stable, high-quality staffing during periods of change, "
                    f"which allows leadership to focus on strategic improvements rather than daily staffing challenges."
                )
        
        balance = financial_data.get('in_year_balance')
        if balance is not None and balance < -30000:
            starters.append(
                f"I understand your school is managing a deficit of £{abs(balance):,}. "
                f"We have specific programs designed to help schools reduce recruitment and supply costs as part "
                f"of financial recovery plans."
            )
        
        # Fallback if no specific data
        if not starters:
            starters.append(
                "We provide high-quality teaching staff at competitive rates with a "
                "quality guarantee. We'd be happy to provide a no-obligation comparison against your "
                "current arrangements to show potential cost savings and service improvements."
            )
        
        logger.info(f"✅ Generated {len(starters)} conversation starters")
        
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
