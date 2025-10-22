"""
Protocol Education CI System - Enhanced Financial Data Engine
Integrates CSV-based RAG with existing web scraping functionality
CSV-first approach with intelligent fallback to web scraping
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
from csv_financial_loader import CSVFinancialLoader

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """   
    Enhanced financial data engine with CSV-first lookup and web scraping fallback
    Maintains exact compatibility with existing code while adding CSV speed
    """   
    
def __init__(self, serper_engine):
    """Initialize with CSV loader and existing Serper engine"""
    self.serper = serper_engine
    self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
    
    # FIXED: Initialize CSV loader only if available
    if CSVFinancialLoader is not None:
        try:
            self.csv_loader = CSVFinancialLoader()
            logger.info(f"CSV loader initialized with {self.csv_loader.get_stats()['total_schools']} schools")
        except Exception as e:
            logger.error(f"Failed to initialize CSV loader: {e}")
            self.csv_loader = None
    else:
        self.csv_loader = None
        logger.info("CSV loader disabled - module not available")
    
    # Track data sources for logging
    self.source_stats = {
        'csv_hits': 0,
        'web_scraping_hits': 0,
        'total_requests': 0
    }

def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
    """
    ENHANCED: Check CSV first, then fall back to web scraping
    Maintains exact same return format as original method
    
    Args:
        urn: School URN
        entity_name: School name for CSV lookup
        is_trust: Whether this is trust-level data (affects web scraping only)
        
    Returns:
        Same dictionary format as original web scraper
    """
    self.source_stats['total_requests'] += 1
    
    # STEP 1: Try CSV lookup first (fastest)
    csv_result = None
    if self.csv_loader and entity_name:
        logger.info(f"🔍 Checking CSV for: {entity_name} (URN: {urn})")
        csv_result = self.csv_loader.get_financial_data(entity_name, urn)
        
        if csv_result:
            self.source_stats['csv_hits'] += 1
            logger.info(f"✅ Found financial data in CSV for {entity_name}")
            
            # Enhance CSV data with additional calculated fields to match web scraper format
            csv_result = self._enhance_csv_data(csv_result, is_trust)
            
            # Add web scraper compatible fields
            csv_result['entity_type'] = 'Trust' if is_trust else 'School'
            
            return csv_result
    
    # STEP 2: Fall back to web scraping (original functionality)
    logger.info(f"⬇️ CSV miss - falling back to web scraping for URN {urn}")
    self.source_stats['web_scraping_hits'] += 1
    
    return self._get_financial_data_web_scraping(urn, entity_name, is_trust)

def _enhance_csv_data(self, csv_data: Dict[str, Any], is_trust: bool) -> Dict[str, Any]:
    """
    Enhance CSV data to match web scraper output format exactly
    """
    enhanced = csv_data.copy()
    
    # Ensure all expected fields exist
    expected_fields = [
        'teaching_staff_per_pupil',
        'supply_staff_costs', 
        'indirect_employee_expenses',
        'recruitment_estimates'
    ]
    
    for field in expected_fields:
        if field not in enhanced:
            enhanced[field] = 0 if field != 'recruitment_estimates' else {}
    
    # Add trust-specific enhancements if needed
    if is_trust and enhanced.get('recruitment_estimates') and isinstance(enhanced['recruitment_estimates'], dict):
        # For trust data, add trust-specific fields
        midpoint = enhanced['recruitment_estimates'].get('midpoint', 0)
        if midpoint > 0:
            # Estimate number of schools in trust (you might want to add this to your CSV)
            estimated_schools = max(1, int(midpoint / 15000))  # Rough estimate
            
            enhanced['recruitment_estimates'].update({
                'total_trust': midpoint,
                'per_school_avg': int(midpoint / estimated_schools),
                'economies_of_scale_saving': '35-45%',
                'explanation': f"Trust-wide recruitment savings across ~{estimated_schools} schools"
            })
    
    # Add benchmarking data if available
    enhanced['comparison'] = self._get_csv_benchmarks(enhanced)
    
    return enhanced

def _get_csv_benchmarks(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
    """Generate benchmark comparisons using CSV data"""
    if not self.csv_loader or self.csv_loader.df.empty:
        return {}
    
    benchmarks = {}
    
    # Calculate percentiles from CSV data
    supply_col = 'Supply Staff: E02 + E10 + E26'
    if supply_col in self.csv_loader.df.columns:
        supply_data = self.csv_loader.df[supply_col].dropna()
        if len(supply_data) > 10:  # Need reasonable sample size
            current_supply = financial_data.get('supply_staff_costs', 0)
            if current_supply > 0:
                percentile = (supply_data < current_supply).mean() * 100
                benchmarks['supply_cost_percentile'] = f"{percentile:.0f}th percentile"
                
                median_supply = supply_data.median()
                if median_supply > 0:
                    vs_median = ((current_supply - median_supply) / median_supply) * 100
                    benchmarks['vs_median_supply'] = f"{'+' if vs_median > 0 else ''}{vs_median:.1f}%"
    
    return benchmarks

def _get_financial_data_web_scraping(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
    """
    Original web scraping method (unchanged for compatibility)
    """
    logger.info(f"Fetching financial data via web scraping for URN {urn}")
    
    financial_data = {
        'urn': urn,
        'entity_name': entity_name,
        'entity_type': 'Trust' if is_trust else 'School',
        'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
        'extracted_date': datetime.now().isoformat(),
        'data_source': 'web_scraping'
    }
    
    # Fetch the actual FBIT page
    html_content = self._fetch_fbit_page(urn)
    
    if not html_content:
        # Fallback to search approach if scraping fails
        logger.warning("Failed to fetch FBIT page, falling back to search approach")
        return self._get_financial_data_from_search(urn, entity_name, is_trust)
    
    # Parse the HTML (original logic)
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract headline figures
    headline_figures = soup.find_all('li', class_='app-headline-figures')
    for figure in headline_figures:
        label = figure.find('p', class_='govuk-body-l govuk-!-font-weight-bold')
        value = figure.find_all('p', class_='govuk-body-l govuk-!-margin-bottom-2')
        
        if label and value:
            label_text = label.get_text(strip=True).lower()
            value_text = value[-1].get_text(strip=True) if value else ''
            
            # Parse the value
            value_match = re.search(r'[-−]?£([\d,]+)', value_text)
            if value_match:
                amount = int(value_match.group(1).replace(',', ''))
                # Check for negative value
                if '-' in value_text or '−' in value_text:
                    amount = -amount
                
                if 'in year balance' in label_text:
                    financial_data['in_year_balance'] = amount
                elif 'revenue reserve' in label_text:
                    financial_data['revenue_reserve'] = amount
    
    # Extract spending priorities (original parsing logic)
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
        
        # Extract per-pupil costs
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
        
        # Extract per-square-meter costs
        sqm_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+square\s+metre', text)
        if sqm_match:
            amount = int(sqm_match.group(1).replace(',', ''))
            if 'Utilities' in category:
                financial_data['utilities_per_sqm'] = amount
    
    # Search for additional financial data in page text
    all_text = soup.get_text()
    
    # Supply staff costs
    supply_match = re.search(r'Supply\s+staff\s+costs?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
    if supply_match:
        financial_data['supply_staff_costs'] = int(supply_match.group(1).replace(',', ''))
    
    # Indirect employee expenses
    indirect_match = re.search(r'Indirect\s+employee\s+expenses?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
    if indirect_match:
        financial_data['indirect_employee_expenses'] = int(indirect_match.group(1).replace(',', ''))
    
    # Calculate recruitment estimates (original logic)
    if 'indirect_employee_expenses' in financial_data:
        if is_trust and hasattr(self, '_last_schools_count') and self._last_schools_count:
            schools = self._last_schools_count
            total_recruitment = int(financial_data['indirect_employee_expenses'] * 0.25)
            
            financial_data['recruitment_estimates'] = {
                'total_trust': total_recruitment,
                'per_school_avg': int(total_recruitment / schools),
                'economies_of_scale_saving': '35-45%',
                'explanation': f"Trust-wide recruitment for {schools} schools provides significant cost savings"
            }
        else:
            financial_data['recruitment_estimates'] = {
                'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25)
            }
    
    # Add extraction confidence
    financial_data['extraction_confidence'] = self._calculate_extraction_confidence(financial_data)
    
    return financial_data

def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
    """
    ENHANCED: Complete recruitment cost intelligence with CSV-first approach
    """
    logger.info(f"🎯 Getting recruitment intelligence for: {school_name}")
    
    # Step 1: Try CSV lookup first
    if self.csv_loader:
        csv_result = self.csv_loader.get_financial_data(school_name)
        if csv_result:
            logger.info(f"✅ Found {school_name} in CSV - using fast lookup")
            
            # Create intelligence response using CSV data
            return {
                'school_searched': school_name,
                'entity_found': {
                    'name': csv_result['entity_name'],
                    'type': csv_result['entity_type'],
                    'urn': csv_result['urn'],
                    'location': csv_result.get('local_authority', location or ''),
                    'confidence': 0.95,  # High confidence for CSV data
                    'data_source': 'csv'
                },
                'financial': csv_result,
                'insights': self._generate_insights(csv_result, False),
                'comparison': csv_result.get('comparison', {}),
                'conversation_starters': self._generate_cost_conversations(csv_result, None, None),
                'processing_method': 'csv_lookup',
                'processing_time': 0.1  # CSV lookup is very fast
            }
    
    # Step 2: Fall back to original web scraping method
    logger.info(f"⬇️ School not in CSV - using web scraping for {school_name}")
    
    # Get school URN via web search (original method)
    urn_result = self.get_school_urn(school_name, location)
    
    if not urn_result.get('urn'):
        return {
            'error': 'Could not find school URN',
            'suggestions': urn_result.get('alternatives', []),
            'processing_method': 'web_search_failed'
        }
    
    logger.info(f"Found URN {urn_result['urn']} for {urn_result['official_name']}")
    
    # Get financial data for this specific school via web scraping
    financial_data = self._get_financial_data_web_scraping(
        urn_result['urn'],
        urn_result['official_name'],
        False  # Always False for school-level data
    )
    
    # Combine and enhance (original logic)
    intelligence = {
        'school_searched': school_name,
        'entity_found': {
            'name': urn_result['official_name'],
            'type': 'School',
            'urn': urn_result['urn'],
            'location': urn_result.get('address', ''),
            'trust_name': urn_result.get('trust_name'),
            'confidence': urn_result['confidence'],
            'data_source': 'web_scraping'
        },
        'financial': financial_data,
        'insights': self._generate_insights(financial_data, False),
        'comparison': self._get_benchmarks(financial_data),
        'conversation_starters': self._generate_cost_conversations(financial_data, None, None),
        'processing_method': 'web_scraping'
    }
    
    return intelligence

def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
    """
    Find school URN using government database
    """
    search_query = f'"{school_name}"'
    if location:
        search_query += f' {location}'
    search_query += ' site:get-information-schools.service.gov.uk URN'
    
    logger.info(f"Searching for school URN: {search_query}")
    
    # Search using Serper
    results = self.serper.search_web(search_query, num_results=5)
    
    if not results:
        return self._search_fbit_direct(school_name, location)
    
    # Parse results for URN
    urn_matches = []
    for result in results:
        url = result.get('url', '')
        text = f"{result.get('title', '')} {result.get('snippet', '')}"
        
        # Skip trust/group pages
        if '/Groups/Group/' in url:
            continue
            
        # Extract URN from URL pattern
        gias_match = re.search(r'/Establishments/Establishment/Details/(\d{5,7})', url)
        if gias_match:
            urn_from_url = gias_match.group(1)
            official_name = self._extract_school_name(result)
            
            urn_matches.append({
                'urn': urn_from_url,
                'official_name': official_name,
                'trust_name': None,
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
            if 'Page not found' not in response.text and 'Spending priorities for this school' in response.text:
                logger.info("Successfully fetched FBIT page")
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

def _get_financial_data_from_search(self, urn: str, entity_name: str, is_trust: bool) -> Dict[str, Any]:
    """Fallback method using search (original approach)"""
    base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
    
    financial_data = {
        'urn': urn,
        'entity_name': entity_name,
        'entity_type': 'Trust' if is_trust else 'School',
        'source_url': base_url,
        'extracted_date': datetime.now().isoformat(),
        'data_source': 'search_fallback'
    }
    
    # Search for specific pages
    search_queries = [
        f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "Spends" "per pupil"',
        f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "In year balance" "Revenue reserve"'
    ]
    
    for query in search_queries:
        results = self.serper.search_web(query, num_results=3)
        
        if results:
            all_content = ' '.join([r.get('snippet', '') for r in results])
            
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
                    
                    if key == 'in_year_balance' and ('−' in all_content[max(0, match.start()-10):match.start()] or '-' in all_content[max(0, match.start()-10):match.start()]):
                        financial_data[key] = -financial_data[key]
    
    return financial_data

def _calculate_extraction_confidence(self, data: Dict[str, Any]) -> float:
    """Calculate confidence score for extracted data"""
    essential_fields = ['teaching_staff_per_pupil', 'in_year_balance', 'revenue_reserve']
    found_essential = sum(1 for field in essential_fields if field in data and data[field] is not None)
    
    optional_fields = ['admin_supplies_per_pupil', 'utilities_per_sqm', 'supply_staff_costs', 'indirect_employee_expenses']
    found_optional = sum(1 for field in optional_fields if field in data and data[field] is not None)
    
    confidence = (found_essential / len(essential_fields)) * 0.7 + (found_optional / len(optional_fields)) * 0.3
    return round(confidence, 2)

def _generate_insights(self, financial_data: Dict, is_trust: bool) -> List[str]:
    """Enhanced insights generation with CSV awareness"""
    insights = []
    
    # Add data source insight
    if financial_data.get('data_source') == 'csv':
        insights.append("📊 Data retrieved from CSV database (fast lookup)")
    elif financial_data.get('data_source') == 'web_scraping':
        insights.append("🌐 Data retrieved via government website scraping")
    
    # Recruitment estimates
    if 'recruitment_estimates' in financial_data:
        est = financial_data['recruitment_estimates']
        if 'midpoint' in est:
            midpoint = est['midpoint']
            insights.append(f"💰 Estimated annual recruitment spend: £{midpoint:,}")
            
            if 'note' in est:
                insights.append(f"ℹ️ {est['note']}")
    
    # Supply cost insights
    if 'supply_staff_costs' in financial_data:
        supply = financial_data['supply_staff_costs']
        if supply > 0:
            insights.append(f"👥 Annual supply staff costs: £{supply:,}")
            
            # Add benchmark if available from CSV
            if financial_data.get('comparison', {}).get('vs_median_supply'):
                vs_median = financial_data['comparison']['vs_median_supply']
                insights.append(f"📈 Supply costs vs CSV median: {vs_median}")
    
    # Balance insights
    if 'in_year_balance' in financial_data:
        balance = financial_data['in_year_balance']
        if balance < 0:
            insights.append(f"⚠️ School has a deficit of £{abs(balance):,} - cost-saving opportunities")
        else:
            insights.append(f"✅ School has a surplus of £{balance:,}")
    
    # Per-pupil spending insights
    if 'teaching_staff_per_pupil' in financial_data:
        teaching = financial_data['teaching_staff_per_pupil']
        insights.append(f"👨‍🏫 Teaching staff costs: £{teaching:,} per pupil")
    
    return insights

def _generate_cost_conversations(self, financial_data: Dict, trust_name: str = None, schools_count: int = None) -> List[str]:
    """Enhanced conversation starters with CSV data awareness"""
    starters = []
    
    # Recruitment cost conversations
    if 'recruitment_estimates' in financial_data:
        est = financial_data['recruitment_estimates']
        if 'midpoint' in est:
            cost = est['midpoint']
            data_source = "our database shows" if financial_data.get('data_source') == 'csv' else "government data indicates"
            
            starters.append(
                f"Based on financial analysis, {data_source} your school spends approximately "
                f"£{cost:,} annually on recruitment. Protocol Education's transparent pricing "
                "could help you achieve better value and reduce these costs."
            )
    
    # Supply staff conversations with CSV benchmarking
    if 'supply_staff_costs' in financial_data:
        supply = financial_data['supply_staff_costs']
        if supply > 0:
            benchmark_text = ""
            if financial_data.get('comparison', {}).get('vs_median_supply'):
                vs_median = financial_data['comparison']['vs_median_supply']
                if '+' in vs_median:
                    benchmark_text = f" - that's {vs_median} above the median for similar schools"
                else:
                    benchmark_text = f" - you're already {vs_median.replace('-', '')} below average"
            
            starters.append(
                f"Your annual supply costs are £{supply:,}{benchmark_text}. "
                f"Protocol Education's quality guarantee ensures you get maximum value "
                "from every placement while maintaining competitive rates."
            )
    
    # Financial health conversations
    if 'in_year_balance' in financial_data:
        balance = financial_data['in_year_balance']
        if balance < 0:
            starters.append(
                f"I understand your school is managing budget pressures with a £{abs(balance):,} deficit. "
                "Protocol Education can support your financial recovery with cost-effective recruitment "
                "solutions that don't compromise on quality."
            )
        elif balance > 50000:  # Healthy surplus
            starters.append(
                f"With a healthy £{balance:,} surplus, now might be the perfect time to invest "
                "in premium recruitment services that will attract and retain the best teachers "
                "for your school's continued success."
            )
    
    return starters

def get_data_source_stats(self) -> Dict[str, Any]:
    """Get statistics on data source usage for monitoring"""
    total = self.source_stats['total_requests']
    
    if total == 0:
        return {
            'total_requests': 0,
            'csv_usage': '0%',
            'web_scraping_usage': '0%',
            'csv_hit_rate': 0.0
        }
    
    csv_rate = self.source_stats['csv_hits'] / total
    web_rate = self.source_stats['web_scraping_hits'] / total
    
    return {
        'total_requests': total,
        'csv_hits': self.source_stats['csv_hits'],
        'web_scraping_hits': self.source_stats['web_scraping_hits'],
        'csv_usage': f"{csv_rate:.1%}",
        'web_scraping_usage': f"{web_rate:.1%}",
        'csv_hit_rate': csv_rate,
        'avg_response_time': {
            'csv': '~0.1s',
            'web_scraping': '~15-30s'
        }
    }

def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
    """Calculate confidence score for name match"""
    result_name = self._extract_school_name(result).lower()
    search_name = search_name.lower()
    
    base_confidence = 0.7 if is_trust else 0.5
    
    if search_name == result_name:
        return 1.0
    
    if search_name in result_name or result_name in search_name:
        return base_confidence + 0.2
    
    search_words = set(search_name.split())
    result_words = set(result_name.split())
    common_words = search_words.intersection(result_words)
    
    if common_words:
        return base_confidence + (0.2 * len(common_words) / len(search_words))
    
    return base_confidence - 0.2

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
                'is_trust': False,
                'alternatives': []
            }
    
    return {'urn': None, 'confidence': 0.0, 'error': 'No results found'}

def _get_benchmarks(self, financial_data: Dict) -> Dict:
    """Enhanced benchmarks with CSV data if available"""
    benchmarks = {
        'national_average': {
            'indirect_employee_expenses': 35000,
            'supply_costs': 100000,
            'teaching_per_pupil': 5500
        },
        'comparison': {}
    }
    
    # Add CSV benchmarks if available
    if self.csv_loader and financial_data.get('comparison'):
        benchmarks['csv_benchmarks'] = financial_data['comparison']
    
    # Original comparison logic
    if 'teaching_staff_per_pupil' in financial_data:
        teaching = financial_data['teaching_staff_per_pupil']
        avg = benchmarks['national_average']['teaching_per_pupil']
        diff_pct = ((teaching - avg) / avg) * 100
        benchmarks['comparison']['teaching_vs_average'] = f"{'+' if diff_pct > 0 else ''}{diff_pct:.1f}%"
    
    return benchmarks
Integration function for the premium processor
def enhance_school_with_financial_data(intel, serper_engine):
"""
Enhanced: Add financial data with CSV-first approach
Args:
    intel: SchoolIntelligence object
    serper_engine: Existing PremiumAIEngine instance
"""
try:
    # Use the enhanced financial engine
    financial_engine = FinancialDataEngine(serper_engine)
    
    # Get recruitment cost intelligence (now CSV-first)
    financial_intel = financial_engine.get_recruitment_intelligence(
        intel.school_name,
        intel.address
    )
    
    # Add to existing intelligence
    if not financial_intel.get('error'):
        # Add processing method info
        method = financial_intel.get('processing_method', 'unknown')
        logger.info(f"Financial data for {intel.school_name} obtained via: {method}")
        
        # Add financial insights to conversation starters
        if 'conversation_starters' in financial_intel:
            for starter in financial_intel['conversation_starters']:
                intel.conversation_starters.append(
                    ConversationStarter(
                        topic="Financial Intelligence",
                        detail=starter,
                        source_url=financial_intel.get('financial', {}).get('source_url', ''),
                        relevance_score=0.9
                    )
                )
        
        # Store enhanced financial data
        intel.financial_data = financial_intel
        
        logger.info(f"✅ Enhanced {intel.school_name} with financial data via {method}")
    else:
        logger.warning(f"⚠️ Could not get financial data for {intel.school_name}: {financial_intel.get('error')}")

except Exception as e:
    logger.error(f"❌ Error enhancing school with financial data: {e}")

return intel
