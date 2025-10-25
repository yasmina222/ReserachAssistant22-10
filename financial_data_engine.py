"""
Protocol Education CI System - Financial Data Engine (FIXED)
Retrieves school financial data from government sources
FIXED: Reliable URN finding and validation, working links guaranteed
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
    """Retrieves school financial data from government sources with reliable URN finding"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        
        # FIXED: Get API key from Streamlit secrets first, then environment
        try:
            import streamlit as st
            self.scraper_api_key = st.secrets.get('SCRAPER_API_KEY', os.getenv('SCRAPER_API_KEY'))
        except:
            self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
        
        # Known working GIAS URL patterns
        self.gias_patterns = [
            r'get-information-schools\.service\.gov\.uk/Establishments/Establishment/Details/(\d{5,7})',
            r'schools\.edubase\.gov\.uk/ViewEstablishment\.aspx\?EstablishmentID=(\d{5,7})',
            r'/Establishment/Details/(\d{5,7})',
            r'URN[:\s]+(\d{5,7})',
            r'establishment.*?(\d{6})',
        ]
    
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN using improved multi-strategy approach
        FIXED: Much more reliable URN finding with validation
        """
        
        logger.info(f"🔍 Starting URN search for: {school_name}")
        
        # Strategy 1: Direct GIAS search (most reliable)
        urn_result = self._search_gias_direct(school_name, location)
        if urn_result and urn_result.get('urn'):
            logger.info(f"✅ Found URN via GIAS: {urn_result['urn']}")
            # VALIDATE the URN before returning
            if self._validate_urn(urn_result['urn']):
                return urn_result
            else:
                logger.warning(f"❌ URN {urn_result['urn']} failed validation")
        
        # Strategy 2: Google search for GIAS pages
        urn_result = self._search_via_google(school_name, location)
        if urn_result and urn_result.get('urn'):
            logger.info(f"✅ Found URN via Google: {urn_result['urn']}")
            if self._validate_urn(urn_result['urn']):
                return urn_result
            else:
                logger.warning(f"❌ URN {urn_result['urn']} failed validation")
        
        # Strategy 3: Search financial benchmarking site directly
        urn_result = self._search_fbit_direct(school_name, location)
        if urn_result and urn_result.get('urn'):
            logger.info(f"✅ Found URN via FBIT: {urn_result['urn']}")
            if self._validate_urn(urn_result['urn']):
                return urn_result
        
        # Strategy 4: Fuzzy matching on school name variations
        urn_result = self._fuzzy_search(school_name, location)
        if urn_result and urn_result.get('urn'):
            logger.info(f"✅ Found URN via fuzzy search: {urn_result['urn']}")
            if self._validate_urn(urn_result['urn']):
                return urn_result
        
        logger.error(f"❌ Could not find valid URN for {school_name}")
        return {
            'urn': None,
            'official_name': school_name,
            'confidence': 0.0,
            'error': 'URN not found after trying all search strategies',
            'alternatives': []
        }
    
    def _search_gias_direct(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Search Get Information About Schools directly"""
        
        # Build precise search query
        search_query = f'"{school_name}" site:get-information-schools.service.gov.uk'
        if location:
            search_query += f' "{location}"'
        
        results = self.serper.search_web(search_query, num_results=10)
        
        return self._extract_urn_from_results(results, school_name, 'GIAS Direct')
    
    def _search_via_google(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Search for school via general Google search"""
        
        search_queries = [
            f'"{school_name}" URN UK school',
            f'"{school_name}" get information about schools',
            f'"{school_name}" school details UK'
        ]
        
        if location:
            search_queries.insert(0, f'"{school_name}" {location} URN')
        
        for query in search_queries[:3]:  # Try first 3 queries
            results = self.serper.search_web(query, num_results=10)
            urn_result = self._extract_urn_from_results(results, school_name, 'Google Search')
            
            if urn_result and urn_result.get('urn'):
                return urn_result
        
        return None
    
    def _search_fbit_direct(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Search financial benchmarking site directly"""
        
        search_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            search_query += f' {location}'
        
        results = self.serper.search_web(search_query, num_results=10)
        
        # Look for URN in URLs
        for result in results:
            url = result.get('url', '')
            
            # Extract URN from FBIT URL format: /school/{URN}
            fbit_match = re.search(r'/school/(\d{5,7})', url)
            if fbit_match:
                urn = fbit_match.group(1)
                return {
                    'urn': urn,
                    'official_name': self._extract_school_name(result),
                    'confidence': 0.9,
                    'source': 'FBIT Direct',
                    'url': url,
                    'alternatives': []
                }
        
        return None
    
    def _fuzzy_search(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Try variations of the school name"""
        
        # Generate name variations
        variations = [
            school_name,
            school_name.replace("'", ""),  # Remove apostrophes
            school_name.replace("St ", "Saint "),  # St -> Saint
            school_name.replace("Saint ", "St "),  # Saint -> St
            re.sub(r'\s+School$', '', school_name),  # Remove trailing "School"
            re.sub(r'\s+(Primary|Secondary|Academy).*$', '', school_name),  # Remove type
        ]
        
        # Remove duplicates while preserving order
        variations = list(dict.fromkeys(variations))
        
        for variation in variations[:5]:  # Try first 5 variations
            if variation != school_name:  # Skip original as already tried
                logger.info(f"🔄 Trying variation: {variation}")
                result = self._search_gias_direct(variation, location)
                if result and result.get('urn'):
                    result['note'] = f'Found using name variation: {variation}'
                    return result
        
        return None
    
    def _extract_urn_from_results(self, results: List[Dict], school_name: str, source: str) -> Optional[Dict[str, Any]]:
        """Extract URN from search results using multiple patterns"""
        
        urn_matches = []
        
        for result in results:
            url = result.get('url', '')
            title = result.get('title', '')
            snippet = result.get('snippet', '')
            full_text = f"{url} {title} {snippet}"
            
            # Try all known patterns
            for pattern in self.gias_patterns:
                matches = re.finditer(pattern, full_text, re.IGNORECASE)
                for match in matches:
                    urn = match.group(1)
                    
                    # Validate URN format (5-7 digits)
                    if len(urn) >= 5 and len(urn) <= 7:
                        official_name = self._extract_school_name(result)
                        confidence = self._calculate_name_match(school_name, official_name)
                        
                        urn_matches.append({
                            'urn': urn,
                            'official_name': official_name,
                            'confidence': confidence,
                            'source': source,
                            'url': url,
                        })
        
        # Remove duplicates based on URN
        unique_urns = {}
        for match in urn_matches:
            urn = match['urn']
            if urn not in unique_urns or match['confidence'] > unique_urns[urn]['confidence']:
                unique_urns[urn] = match
        
        urn_matches = list(unique_urns.values())
        
        if not urn_matches:
            return None
        
        # Sort by confidence and return best match
        urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
        best_match = urn_matches[0]
        best_match['alternatives'] = urn_matches[1:3] if len(urn_matches) > 1 else []
        
        return best_match
    
    def _validate_urn(self, urn: str) -> bool:
        """
        Validate URN by checking if the financial page exists
        CRITICAL: This ensures we only return working links
        """
        
        if not urn or len(urn) < 5:
            return False
        
        # Build the URL
        test_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        try:
            # Quick HEAD request to check if page exists
            response = requests.head(test_url, timeout=5, allow_redirects=True)
            
            if response.status_code == 200:
                logger.info(f"✅ URN {urn} validated - page exists")
                return True
            else:
                logger.warning(f"❌ URN {urn} failed validation - status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ URN {urn} validation failed: {e}")
            # If we can't validate, try with ScraperAPI as backup
            return self._validate_with_scraper_api(urn)
    
    def _validate_with_scraper_api(self, urn: str) -> bool:
        """Validate URN using ScraperAPI as backup"""
        
        if not self.scraper_api_key:
            return False
        
        test_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        params = {
            'api_key': self.scraper_api_key,
            'url': test_url,
            'render': 'false',  # Don't need full render for validation
            'country_code': 'gb'
        }
        
        try:
            response = requests.get('http://api.scraperapi.com', params=params, timeout=10)
            
            if response.status_code == 200 and 'Page not found' not in response.text:
                logger.info(f"✅ URN {urn} validated via ScraperAPI")
                return True
            else:
                logger.warning(f"❌ URN {urn} invalid via ScraperAPI")
                return False
                
        except Exception as e:
            logger.error(f"ScraperAPI validation error: {e}")
            return False
    
    def _fetch_fbit_page(self, urn: str) -> Optional[str]:
        """Fetch actual FBIT page content using ScraperAPI"""
        
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
        
        logger.info(f"📥 Fetching FBIT page for URN {urn}")
        
        try:
            response = requests.get('http://api.scraperapi.com', params=params, timeout=30)
            
            if response.status_code == 200:
                # Check if it's a valid page
                if 'Page not found' in response.text:
                    logger.error(f"❌ URN {urn} returned 404 page")
                    return None
                
                if 'Spending priorities for this school' in response.text or 'In year balance' in response.text:
                    logger.info(f"✅ Successfully fetched FBIT page for URN {urn}")
                    return response.text
                else:
                    logger.error(f"❌ Page content doesn't look like a valid FBIT page")
                    return None
            else:
                logger.error(f"❌ ScraperAPI returned status {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching FBIT page: {e}")
            return None
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data from FBIT website using URN
        FIXED: Only creates link if URN is validated
        """
        
        logger.info(f"💰 Fetching financial data for URN {urn}")
        
        # CRITICAL: Validate URN first
        if not self._validate_urn(urn):
            logger.error(f"❌ URN {urn} is invalid - not generating link")
            return {
                'error': 'Invalid URN - financial data link not available',
                'urn': urn,
                'entity_name': entity_name
            }
        
        # URN is valid - create the link
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'extracted_date': datetime.now().isoformat(),
            'link_validated': True  # We know it works!
        }
        
        # Try to extract actual data from the page
        html_content = self._fetch_fbit_page(urn)
        
        if not html_content:
            logger.warning(f"⚠️ Could not extract data, but link is valid")
            financial_data['data_extracted'] = False
            financial_data['note'] = 'Link is valid but data extraction failed. Click link to view manually.'
            return financial_data
        
        # Parse the page
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract headline figures (In year balance, Revenue reserve)
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
                        logger.info(f"📊 In year balance: £{amount:,}")
                    elif 'revenue reserve' in label_text:
                        financial_data['revenue_reserve'] = amount
                        logger.info(f"📊 Revenue reserve: £{amount:,}")
        
        # Extract spending priorities
        priority_wrappers = soup.find_all('div', class_='priority-wrapper')
        
        logger.info(f"📊 Found {len(priority_wrappers)} spending priority items")
        
        for wrapper in priority_wrappers:
            category_elem = wrapper.find('h4', class_='govuk-heading-s')
            if not category_elem:
                continue
            
            category = category_elem.get_text(strip=True)
            
            priority_elem = wrapper.find('p', class_='priority')
            if not priority_elem:
                continue
            
            text = priority_elem.get_text(strip=True)
            
            # Extract per-pupil spending
            amount_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+pupil', text)
            if amount_match:
                amount = int(amount_match.group(1).replace(',', ''))
                
                if 'Teaching' in category and 'staff' in category:
                    financial_data['teaching_staff_per_pupil'] = amount
                    logger.info(f"📊 Teaching staff: £{amount:,}/pupil")
                elif 'Administrative supplies' in category:
                    financial_data['admin_supplies_per_pupil'] = amount
                    logger.info(f"📊 Admin supplies: £{amount:,}/pupil")
                else:
                    key = category.lower().replace(' ', '_') + '_per_pupil'
                    financial_data[key] = amount
                    logger.info(f"📊 {category}: £{amount:,}/pupil")
            
            # Extract per-square-meter spending
            sqm_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+square\s+metre', text)
            if sqm_match:
                amount = int(sqm_match.group(1).replace(',', ''))
                if 'Utilities' in category:
                    financial_data['utilities_per_sqm'] = amount
                    logger.info(f"📊 Utilities: £{amount:,}/sqm")
        
        # Extract additional financial details from text
        all_text = soup.get_text()
        
        supply_match = re.search(r'Supply\s+staff\s+costs?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
        if supply_match:
            financial_data['supply_staff_costs'] = int(supply_match.group(1).replace(',', ''))
            logger.info(f"📊 Supply staff costs: £{financial_data['supply_staff_costs']:,}")
        
        indirect_match = re.search(r'Indirect\s+employee\s+expenses?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
        if indirect_match:
            financial_data['indirect_employee_expenses'] = int(indirect_match.group(1).replace(',', ''))
            logger.info(f"📊 Indirect employee expenses: £{financial_data['indirect_employee_expenses']:,}")
        
        # Estimate recruitment costs
        if 'indirect_employee_expenses' in financial_data:
            financial_data['recruitment_estimates'] = {
                'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25),
                'basis': 'Estimated as 20-30% of indirect employee expenses'
            }
        elif 'teaching_staff_per_pupil' in financial_data:
            teaching_total_estimate = financial_data['teaching_staff_per_pupil'] * 200  # Assume ~200 pupils
            financial_data['recruitment_estimates'] = {
                'low': int(teaching_total_estimate * 0.03),
                'high': int(teaching_total_estimate * 0.05),
                'midpoint': int(teaching_total_estimate * 0.04),
                'basis': 'Estimated as 3-5% of teaching costs'
            }
        
        # Calculate extraction confidence
        financial_data['extraction_confidence'] = self._calculate_extraction_confidence(financial_data)
        financial_data['data_extracted'] = True
        
        logger.info(f"✅ Successfully extracted financial data (confidence: {financial_data['extraction_confidence']:.0%})")
        
        return financial_data
    
    def _calculate_extraction_confidence(self, data: Dict[str, Any]) -> float:
        """Calculate confidence score for extracted data"""
        
        essential_fields = ['teaching_staff_per_pupil', 'in_year_balance', 'revenue_reserve']
        found_essential = sum(1 for field in essential_fields if field in data and data[field] is not None)
        
        optional_fields = ['admin_supplies_per_pupil', 'utilities_per_sqm', 'supply_staff_costs', 'indirect_employee_expenses']
        found_optional = sum(1 for field in optional_fields if field in data and data[field] is not None)
        
        confidence = (found_essential / len(essential_fields)) * 0.7 + (found_optional / len(optional_fields)) * 0.3
        
        return round(confidence, 2)
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete recruitment cost intelligence for a school"""
        
        logger.info(f"🎯 Getting recruitment intelligence for {school_name}")
        
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            logger.error(f"❌ Could not find URN for {school_name}")
            return {
                'error': 'Could not find school URN',
                'school_searched': school_name,
                'suggestions': urn_result.get('alternatives', []),
                'message': 'Try searching with the full official school name or check spelling'
            }
        
        logger.info(f"✅ Found URN {urn_result['urn']} for {urn_result['official_name']}")
        
        financial_data = self.get_financial_data(
            urn_result['urn'],
            urn_result['official_name'],
            False
        )
        
        if financial_data.get('error'):
            return {
                'error': financial_data['error'],
                'school_searched': school_name,
                'urn_found': urn_result['urn']
            }
        
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
            'conversation_starters': self._generate_cost_conversations(financial_data, None, None)
        }
        
        return intelligence
    
    def _calculate_name_match(self, search_name: str, result_name: str) -> float:
        """Calculate confidence score for name match"""
        
        search_lower = search_name.lower()
        result_lower = result_name.lower()
        
        # Exact match
        if search_lower == result_lower:
            return 1.0
        
        # Contains match
        if search_lower in result_lower or result_lower in search_lower:
            return 0.85
        
        # Word overlap
        search_words = set(search_lower.split())
        result_words = set(result_lower.split())
        
        # Remove common words
        common_words = {'school', 'primary', 'secondary', 'academy', 'the', 'and', 'of'}
        search_words -= common_words
        result_words -= common_words
        
        if not search_words or not result_words:
            return 0.5
        
        overlap = len(search_words.intersection(result_words))
        overlap_ratio = overlap / len(search_words)
        
        return 0.5 + (overlap_ratio * 0.4)
    
    def _generate_insights(self, financial_data: Dict, is_trust: bool) -> List[str]:
        """Generate insights from financial data"""
        
        insights = []
        
        # Check if data was extracted
        if not financial_data.get('data_extracted', False):
            insights.append("Click the link above to view detailed financial information")
            return insights
        
        if 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                midpoint = est['midpoint']
                insights.append(f"Estimated annual recruitment spend: £{midpoint:,}")
                insights.append(f"Basis: {est.get('basis', 'Estimated from available data')}")
            
            if 'supply_staff_costs' in financial_data:
                supply = financial_data['supply_staff_costs']
                total_temp = est.get('midpoint', 0) + supply
                insights.append(f"Total temporary staffing costs: £{total_temp:,}")
        
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                insights.append(f"School has a deficit of £{abs(balance):,} - may need cost-saving measures")
            else:
                insights.append(f"School has a surplus of £{balance:,}")
        
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            insights.append(f"Teaching staff costs: £{teaching:,} per pupil")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict, trust_name: str = None, schools_count: int = None) -> List[str]:
        """Generate conversation starters about costs"""
        
        starters = []
        
        # Check if data was extracted
        if not financial_data.get('data_extracted', False):
            starters.append(
                "I can see your school's financial data is available on the government's benchmarking tool. "
                "This gives us great insights into your spending patterns. Protocol Education can help "
                "optimize your recruitment and supply costs - shall we discuss your current challenges?"
            )
            return starters
        
        if 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                cost = est['midpoint']
                starters.append(
                    f"Your school spends approximately £{cost:,} annually on recruitment. "
                    "Protocol Education could help reduce these costs through our competitive rates "
                    "and quality guarantee - saving you thousands each year."
                )
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            starters.append(
                f"With supply costs of £{supply:,}, our long-term staffing solutions and "
                "competitive daily rates could deliver significant savings while maintaining quality."
            )
        
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                starters.append(
                    f"I noticed your school is managing a deficit of £{abs(balance):,}. "
                    "Protocol Education can help reduce recruitment costs as part of your "
                    "financial recovery plan - let's discuss how."
                )
        
        return starters
    
    def _extract_school_name(self, search_result: Dict) -> str:
        """Extract official school name from search result"""
        
        title = search_result.get('title', '')
        
        # Remove common suffixes
        name = re.split(r' - URN:| - Get Information| - GOV\.UK| \| ', title)[0]
        return name.strip()


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    FIXED: Only shows link if URN is valid and verified
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
                for starter in financial_intel['conversation_starters']:
                    intel.conversation_starters.append(
                        ConversationStarter(
                            topic="Recruitment Costs",
                            detail=starter,
                            source_url=financial_intel.get('financial', {}).get('source_url', ''),
                            relevance_score=0.9
                        )
                    )
            
            # Store financial data
            intel.financial_data = financial_intel
            
            logger.info(f"✅ Successfully enhanced {intel.school_name} with financial data")
        else:
            logger.warning(f"⚠️ Could not get financial data for {intel.school_name}: {financial_intel.get('error')}")
            # Store partial data so user knows we tried
            intel.financial_data = {
                'error': financial_intel.get('error'),
                'message': financial_intel.get('message', 'Financial data not available')
            }
    
    except Exception as e:
        logger.error(f"❌ Error enhancing school with financial data: {e}")
    
    return intel
