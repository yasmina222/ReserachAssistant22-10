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
    """Retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
        
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN using government database
        """
        
        # Simple search query - just find the school
        search_query = f'"{school_name}"'
        if location:
            search_query += f' {location}'
        search_query += ' site:get-information-schools.service.gov.uk'
        
        logger.info(f"Searching for school URN: {search_query}")
        
        # Search using Serper
        results = self.serper.search_web(search_query, num_results=5)
        
        if not results:
            # Try FBIT site as fallback
            return self._search_fbit_direct(school_name, location)
        
        # Parse results for URN - Extract from URLs and text
        urn_matches = []
        for result in results:
            url = result.get('url', '')
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            
            # Skip trust/group pages - we want individual school pages
            if '/Groups/Group/' in url:
                continue
                
            urn_from_url = None
            
            # GIAS pattern for individual schools: /Establishments/Establishment/Details/134225
            gias_match = re.search(r'/Establishments/Establishment/Details/(\d{5,7})', url)
            if gias_match:
                urn_from_url = gias_match.group(1)
                logger.info(f"Found URN from GIAS URL: {urn_from_url}")
            
            # Also check text for URN
            if not urn_from_url:
                urn_pattern = r'URN:?\s*(\d{5,7})'
                urn_match = re.search(urn_pattern, text)
                if urn_match:
                    urn_from_url = urn_match.group(1)
                    logger.info(f"Found URN from text: {urn_from_url}")
            
            if urn_from_url:
                # Extract school name
                official_name = self._extract_school_name(result)
                
                # Check if school is part of a trust (but don't change URN)
                trust_name = None
                if 'trust' in text.lower() or 'academy trust' in text.lower():
                    # Try to extract trust name
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
        
        # Sort by confidence and return best match
        urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
        best_match = urn_matches[0]
        best_match['alternatives'] = urn_matches[1:3] if len(urn_matches) > 1 else []
        
        logger.info(f"Best URN match: {best_match['urn']} for {best_match['official_name']} (confidence: {best_match['confidence']:.2f})")
        
        return best_match
    
    def _fetch_fbit_page(self, urn: str) -> Optional[str]:
        """Fetch actual FBIT page content using DIRECT HTTP (no ScraperAPI)"""
        
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        # CRITICAL FIX: Direct fetch, NO ScraperAPI
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        logger.info(f"🔍 Fetching FBIT page directly: {base_url}")
        
        try:
            # DIRECT REQUEST - NO SCRAPERAPI
            response = requests.get(base_url, headers=headers, timeout=15)
            response.raise_for_status()
            
            logger.info(f"✅ Got response: {response.status_code}, Length: {len(response.text)} chars")
            
            # DEBUG: Log first 500 chars to see what we got
            logger.debug(f"Response preview: {response.text[:500]}")
            
            # Check if it's a valid page
            if 'Spending priorities for this school' in response.text or 'In year balance' in response.text:
                logger.info(f"✅ Successfully fetched valid FBIT page ({len(response.text)} chars)")
                return response.text
            else:
                logger.error(f"❌ URN {urn} returned invalid page - no financial markers found")
                logger.debug(f"Page content preview: {response.text[:1000]}")
                return None
                
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ HTTP Error fetching FBIT page: {e.response.status_code}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout fetching FBIT page")
            return None
        except Exception as e:
            logger.error(f"❌ Error fetching FBIT page: {e}")
            return None
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data from FBIT website using URN
        ENHANCED with extensive debugging and fallback mechanisms
        """
        
        logger.info(f"📊 Fetching financial data for URN {urn} ({'Trust' if is_trust else 'School'})")
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'extracted_date': datetime.now().isoformat(),
            'extraction_method': 'direct_fetch'
        }
        
        # Fetch the actual FBIT page
        html_content = self._fetch_fbit_page(urn)
        
        if not html_content:
            logger.warning("⚠️ Failed to fetch FBIT page, falling back to search approach")
            return self._get_financial_data_from_search(urn, entity_name, is_trust)
        
        # Parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        logger.info(f"🔍 Parsing HTML with BeautifulSoup...")
        
        # STRATEGY 1: Try the structured selectors
        extracted_count = 0
        
        # Extract header financial metrics (In year balance, Revenue reserve)
        headline_figures = soup.find_all('li', class_='app-headline-figures')
        logger.info(f"Found {len(headline_figures)} headline figures")
        
        for figure in headline_figures:
            label = figure.find('p', class_='govuk-body-l govuk-!-font-weight-bold')
            value = figure.find_all('p', class_='govuk-body-l govuk-!-margin-bottom-2')
            
            if label and value:
                label_text = label.get_text(strip=True).lower()
                value_text = value[-1].get_text(strip=True) if value else ''
                
                logger.debug(f"Processing figure: {label_text} = {value_text}")
                
                # Parse the value
                value_match = re.search(r'[-−]?£([\d,]+)', value_text)
                if value_match:
                    amount = int(value_match.group(1).replace(',', ''))
                    # Check for negative value
                    if '-' in value_text or '−' in value_text:
                        amount = -amount
                    
                    if 'in year balance' in label_text:
                        financial_data['in_year_balance'] = amount
                        extracted_count += 1
                        logger.info(f"✅ Found in year balance: £{amount:,}")
                    elif 'revenue reserve' in label_text:
                        financial_data['revenue_reserve'] = amount
                        extracted_count += 1
                        logger.info(f"✅ Found revenue reserve: £{amount:,}")
        
        # Extract spending priorities
        priority_wrappers = soup.find_all('div', class_='priority-wrapper')
        logger.info(f"Found {len(priority_wrappers)} spending priority items")
        
        for wrapper in priority_wrappers:
            # Get the category name
            category_elem = wrapper.find('h4', class_='govuk-heading-s')
            if not category_elem:
                continue
            
            category = category_elem.get_text(strip=True)
            
            # Get the spending details
            priority_elem = wrapper.find('p', class_='priority')
            if not priority_elem:
                continue
            
            text = priority_elem.get_text(strip=True)
            
            logger.debug(f"Processing category: {category} - {text[:50]}...")
            
            # Extract amount using correct pattern: "Spends £X,XXX per pupil"
            amount_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+pupil', text)
            if amount_match:
                amount = int(amount_match.group(1).replace(',', ''))
                
                # Map categories to our expected fields
                if 'Teaching' in category and 'staff' in category:
                    financial_data['teaching_staff_per_pupil'] = amount
                    extracted_count += 1
                    logger.info(f"✅ Found teaching staff costs: £{amount:,} per pupil")
                elif 'Administrative supplies' in category:
                    financial_data['admin_supplies_per_pupil'] = amount
                    extracted_count += 1
                    logger.info(f"✅ Found admin supplies: £{amount:,} per pupil")
                else:
                    # Store other costs
                    key = category.lower().replace(' ', '_') + '_per_pupil'
                    financial_data[key] = amount
                    extracted_count += 1
                    logger.info(f"✅ Found {category}: £{amount:,} per pupil")
            
            # Check for per square metre costs (utilities)
            sqm_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+square\s+metre', text)
            if sqm_match:
                amount = int(sqm_match.group(1).replace(',', ''))
                if 'Utilities' in category:
                    financial_data['utilities_per_sqm'] = amount
                    extracted_count += 1
                    logger.info(f"✅ Found utilities: £{amount:,} per square metre")
        
        # STRATEGY 2: If structured parsing failed, try text extraction
        if extracted_count == 0:
            logger.warning("⚠️ Structured parsing extracted nothing, trying text extraction fallback...")
            all_text = soup.get_text()
            
            # Search for supply staff costs in the full page
            supply_match = re.search(r'Supply\s+staff\s+costs?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
            if supply_match:
                financial_data['supply_staff_costs'] = int(supply_match.group(1).replace(',', ''))
                extracted_count += 1
                logger.info(f"✅ Found supply staff costs: £{financial_data['supply_staff_costs']:,}")
            
            # Search for indirect employee expenses
            indirect_match = re.search(r'Indirect\s+employee\s+expenses?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
            if indirect_match:
                financial_data['indirect_employee_expenses'] = int(indirect_match.group(1).replace(',', ''))
                extracted_count += 1
                logger.info(f"✅ Found indirect employee expenses: £{financial_data['indirect_employee_expenses']:,}")
        
        # Calculate recruitment estimates if we have data
        if 'indirect_employee_expenses' in financial_data:
            financial_data['recruitment_estimates'] = {
                'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25)
            }
        elif 'teaching_staff_per_pupil' in financial_data:
            # Estimate based on teaching costs if no indirect expenses found
            teaching_total_estimate = financial_data['teaching_staff_per_pupil'] * 200  # Assume ~200 pupils
            financial_data['recruitment_estimates'] = {
                'low': int(teaching_total_estimate * 0.03),
                'high': int(teaching_total_estimate * 0.05),
                'midpoint': int(teaching_total_estimate * 0.04),
                'note': 'Estimated from teaching costs'
            }
        
        # Add extraction confidence score
        financial_data['extraction_confidence'] = self._calculate_extraction_confidence(financial_data)
        financial_data['fields_extracted'] = extracted_count
        
        logger.info(f"📊 Extraction complete: {extracted_count} fields extracted, confidence: {financial_data['extraction_confidence']:.2f}")
        
        return financial_data
    
    def _calculate_extraction_confidence(self, data: Dict[str, Any]) -> float:
        """Calculate confidence score for extracted data"""
        
        essential_fields = ['teaching_staff_per_pupil', 'in_year_balance', 'revenue_reserve']
        found_essential = sum(1 for field in essential_fields if field in data and data[field] is not None)
        
        optional_fields = ['admin_supplies_per_pupil', 'utilities_per_sqm', 'supply_staff_costs', 'indirect_employee_expenses']
        found_optional = sum(1 for field in optional_fields if field in data and data[field] is not None)
        
        # Calculate confidence (essential fields worth more)
        confidence = (found_essential / len(essential_fields)) * 0.7 + (found_optional / len(optional_fields)) * 0.3
        
        return round(confidence, 2)
    
    def _get_financial_data_from_search(self, urn: str, entity_name: str, is_trust: bool) -> Dict[str, Any]:
        """Fallback method using search (original approach)"""
        
        logger.info(f"🔍 Using SEARCH FALLBACK for URN {urn}")
        
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': base_url,
            'extracted_date': datetime.now().isoformat(),
            'extraction_method': 'search_fallback'
        }
        
        # Search for specific pages
        search_queries = [
            f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "Spends" "per pupil"',
            f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "In year balance" "Revenue reserve"'
        ]
        
        extracted_count = 0
        
        for query in search_queries:
            results = self.serper.search_web(query, num_results=3)
            
            if results:
                # Combine all snippets
                all_content = ' '.join([r.get('snippet', '') for r in results])
                
                logger.debug(f"Search content preview: {all_content[:200]}")
                
                # Extract values with updated patterns
                patterns = {
                    'teaching_staff_per_pupil': r'Teaching.*?staff.*?Spends\s+£([\d,]+)\s+per\s+pupil',
                    'admin_supplies_per_pupil': r'Administrative\s+supplies.*?Spends\s+£([\d,]+)\s+per\s+pupil',
                    'utilities_per_sqm': r'Utilities.*?Spends\s+£([\d,]+)\s+per\s+square\s+metre',
                    'in_year_balance': r'In\s+year\s+balance[:\s]+[-−]?£([\d,]+)',
                    'revenue_reserve': r'Revenue\s+reserve[:\s]+£([\d,]+)'
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, all_content, re.IGNORECASE | re.DOTALL)
                    if match:
                        value_str = match.group(1).replace(',', '')
                        financial_data[key] = int(value_str)
                        extracted_count += 1
                        logger.info(f"✅ Search fallback found {key}: {financial_data[key]}")
                        
                        # Handle negative balance
                        if key == 'in_year_balance' and ('−' in all_content[max(0, match.start()-10):match.start()] or '-' in all_content[max(0, match.start()-10):match.start()]):
                            financial_data[key] = -financial_data[key]
        
        financial_data['fields_extracted'] = extracted_count
        financial_data['extraction_confidence'] = self._calculate_extraction_confidence(financial_data)
        
        logger.info(f"📊 Search fallback complete: {extracted_count} fields extracted")
        
        return financial_data
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete recruitment cost intelligence for a school
        """
        
        logger.info(f"🎯 Starting recruitment intelligence for: {school_name}")
        
        # Step 1: Get school URN (not trust URN)
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            logger.error(f"❌ Could not find URN for {school_name}")
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        logger.info(f"✅ Found URN {urn_result['urn']} for {urn_result['official_name']}")
        
        # Step 2: Get financial data for this specific school
        financial_data = self.get_financial_data(
            urn_result['urn'],
            urn_result['official_name'],
            False  # Always False - we want school data, not trust data
        )
        
        # Step 3: Combine and enhance
        intelligence = {
            'school_searched': school_name,
            'entity_found': {
                'name': urn_result['official_name'],
                'type': 'School',
                'urn': urn_result['urn'],
                'location': urn_result.get('address', ''),
                'trust_name': urn_result.get('trust_name'),  # Just for info
                'confidence': urn_result['confidence']
            },
            'financial': financial_data,
            'insights': self._generate_insights(financial_data, False),  # Always school-level
            'comparison': self._get_benchmarks(financial_data),
            'conversation_starters': self._generate_cost_conversations(
                financial_data, 
                None,  # Don't use trust name in conversations
                None   # Don't use schools count
            )
        }
        
        logger.info(f"✅ Recruitment intelligence complete for {school_name}")
        
        return intelligence
    
    def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
        """Calculate confidence score for name match"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        # Boost confidence for trust results
        base_confidence = 0.7 if is_trust else 0.5
        
        # Exact match
        if search_name == result_name:
            return 1.0
        
        # Contains match
        if search_name in result_name or result_name in search_name:
            return base_confidence + 0.2
        
        # Partial word match
        search_words = set(search_name.split())
        result_words = set(result_name.split())
        common_words = search_words.intersection(result_words)
        
        if common_words:
            return base_confidence + (0.2 * len(common_words) / len(search_words))
        
        return base_confidence - 0.2
    
    def _generate_insights(self, financial_data: Dict, is_trust: bool) -> List[str]:
        """Generate insights from financial data"""
        insights = []
        
        if 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                midpoint = est['midpoint']
                insights.append(f"Estimated annual recruitment spend: £{midpoint:,}")
                
                if 'note' in est:
                    insights.append(f"Note: {est['note']}")
            
            if 'supply_staff_costs' in financial_data:
                supply = financial_data['supply_staff_costs']
                total_temp_costs = est.get('midpoint', 0) + supply
                insights.append(f"Total temporary staffing costs: £{total_temp_costs:,}")
        
        # Add balance insights
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                insights.append(f"School has a deficit of £{abs(balance):,} - may need cost-saving measures")
            else:
                insights.append(f"School has a surplus of £{balance:,}")
        
        # Add per-pupil spending insights
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            insights.append(f"Teaching staff costs: £{teaching:,} per pupil")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict, trust_name: str = None, schools_count: int = None) -> List[str]:
        """Generate conversation starters about costs"""
        starters = []
        
        if 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                cost = est['midpoint']
                starters.append(
                    f"Your school spends approximately £{cost:,} annually on recruitment. "
                    "Protocol Education could help reduce these costs through our competitive rates and quality guarantee."
                )
        
        # Supply staff conversation starters
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            starters.append(
                f"Your £{supply:,} annual supply costs could be reduced through our long-term "
                "staffing solutions and competitive daily rates."
            )
        
        # Balance-related starters
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                starters.append(
                    f"I noticed your school is managing a deficit of £{abs(balance):,}. "
                    "Protocol Education can help reduce recruitment costs as part of your financial recovery plan."
                )
        
        # Teaching cost insights
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            if teaching > 5000:  # High spending threshold
                starters.append(
                    f"With teaching costs at £{teaching:,} per pupil, ensuring value in recruitment "
                    "is crucial. Protocol's quality guarantee ensures you get the best teachers at competitive rates."
                )
        
        return starters
    
    def _extract_school_name(self, search_result: Dict) -> str:
        """Extract official school name from search result"""
        title = search_result.get('title', '')
        # Remove common suffixes
        name = re.split(r' - URN:| - Get Information| - GOV.UK', title)[0]
        return name.strip()
    
    def _extract_location(self, search_result: Dict) -> str:
        """Extract location from search result"""
        snippet = search_result.get('snippet', '')
        # Look for postcode pattern
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
            # Extract URN from FBIT URL
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
    
    def _get_benchmarks(self, financial_data: Dict) -> Dict:
        """Get benchmark comparisons"""
        benchmarks = {
            'national_average': {
                'indirect_employee_expenses': 35000,
                'supply_costs': 100000,
                'teaching_per_pupil': 5500
            },
            'comparison': {}
        }
        
        # Compare with benchmarks
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            avg = benchmarks['national_average']['teaching_per_pupil']
            diff_pct = ((teaching - avg) / avg) * 100
            benchmarks['comparison']['teaching_vs_average'] = f"{'+' if diff_pct > 0 else ''}{diff_pct:.1f}%"
        
        return benchmarks


# Integration function for the premium processor - OUTSIDE THE CLASS
def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    
    Args:
        intel: SchoolIntelligence object
        serper_engine: Existing PremiumAIEngine instance
    """
    
    try:
        logger.info(f"💰 Enhancing {intel.school_name} with financial data...")
        
        financial_engine = FinancialDataEngine(serper_engine)
        
        # Get recruitment cost intelligence
        financial_intel = financial_engine.get_recruitment_intelligence(
            intel.school_name,
            intel.address
        )
        
        # Add to existing intelligence
        if not financial_intel.get('error'):
            logger.info(f"✅ Financial data retrieved for {intel.school_name}")
            
            # CRITICAL FIX: Convert conversation starters to PLAIN STRINGS for serialization
            conversation_starters_text = financial_intel.get('conversation_starters', [])
            
            # Add financial insights to conversation starters AS STRINGS
            if conversation_starters_text:
                for starter_text in conversation_starters_text:
                    # Create ConversationStarter object properly
                    intel.conversation_starters.append(
                        ConversationStarter(
                            topic="Recruitment Costs",
                            detail=starter_text,  # This is already a string
                            source_url=financial_intel.get('financial', {}).get('source_url', ''),
                            relevance_score=0.9
                        )
                    )
            
            # Store financial data in intel object
            intel.financial_data = financial_intel
            
            logger.info(f"✅ Successfully enhanced {intel.school_name} with financial data")
        else:
            logger.warning(f"⚠️ Could not get financial data for {intel.school_name}: {financial_intel.get('error')}")
            logger.info(f"⚠️ No financial data extracted, but users can still view links")
    
    except Exception as e:
        logger.error(f"❌ Error enhancing school with financial data: {e}", exc_info=True)
    
    return intel
