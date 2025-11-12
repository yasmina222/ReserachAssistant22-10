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
        SIMPLIFIED VERSION - Just find the school's URN, not trust URN
        
        Returns:
            {
                'urn': '141133',
                'official_name': 'Brookfield School',
                'trust_name': 'Excellence Academy Trust',
                'address': 'Birmingham, B13 0RG',
                'type': 'Academy',
                'confidence': 0.95,
                'alternatives': []
            }
        """
        
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
        
        logger.info(f"Best URN match: {best_match['urn']} for {best_match['official_name']} (confidence: {best_match['confidence']:.2f})")
        
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
                    logger.debug(f"Response preview: {response.text[:500]}")
                    return None
            else:
                logger.error(f"ScraperAPI returned status {response.status_code}")
                if response.text:
                    logger.debug(f"Response: {response.text[:500]}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching FBIT page: {e}")
            return None
    
    def _fetch_fbit_comparison_page(self, urn: str) -> Optional[str]:
        """
        NEW: Fetch FBIT comparison/benchmark page using ScraperAPI
        URL: https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}/comparison
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
                if 'Page not found' not in response.text:
                    logger.info("✅ Successfully fetched FBIT comparison page")
                    return response.text
                else:
                    logger.error(f"URN {urn} comparison page not found")
                    return None
            else:
                logger.error(f"ScraperAPI returned status {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching FBIT comparison page: {e}")
            return None
    
    def _parse_benchmark_data(self, html_content: str) -> Dict[str, Any]:
        """
        NEW: Parse benchmark comparison data from FBIT comparison page
        Extracts the 7 critical cost fields
        """
        
        soup = BeautifulSoup(html_content, 'html.parser')
        benchmark_data = {}
        
        logger.info("📊 Parsing benchmark comparison data...")
        
        cost_patterns = {
            'total_expenditure_per_pupil': r'Total\s+expenditure\s+per\s+pupil[:\s]*£([0-9,]+)',
            'teaching_and_support_costs_per_pupil': r'Total\s+teaching\s+and\s+teaching\s+support\s+staff\s+costs\s+per\s+pupil[:\s]*£([0-9,]+)',
            'teaching_staff_costs': r'Teaching\s+staff\s+costs[:\s]*£([0-9,]+)',
            'supply_teaching_staff_costs': r'Supply\s+teaching\s+staff\s+costs[:\s]*£([0-9,]+)',
            'educational_consultancy_costs': r'Educational\s+consultancy\s+costs[:\s]*£([0-9,]+)',
            'educational_support_staff_costs': r'Educational\s+support\s+staff\s+costs[:\s]*£([0-9,]+)',
            'agency_supply_teaching_staff_costs': r'Agency\s+supply\s+teaching\s+staff\s+costs[:\s]*£([0-9,]+)'
        }
        
        page_text = soup.get_text()
        
        for field_name, pattern in cost_patterns.items():
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    amount = int(amount_str)
                    benchmark_data[field_name] = amount
                    logger.info(f"✅ Found {field_name}: £{amount:,}")
                except ValueError:
                    continue
        
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    for cell in cells[1:]:
                        cell_text = cell.get_text(strip=True)
                        money_match = re.search(r'£([0-9,]+)', cell_text)
                        if money_match:
                            amount_str = money_match.group(1).replace(',', '')
                            try:
                                amount = int(amount_str)
                                
                                if 'total expenditure' in label and 'per pupil' in label:
                                    benchmark_data['total_expenditure_per_pupil'] = amount
                                elif 'teaching and teaching support' in label and 'per pupil' in label:
                                    benchmark_data['teaching_and_support_costs_per_pupil'] = amount
                                elif 'supply teaching staff' in label and 'agency' not in label:
                                    benchmark_data['supply_teaching_staff_costs'] = amount
                                elif 'agency supply' in label:
                                    benchmark_data['agency_supply_teaching_staff_costs'] = amount
                                elif 'educational consultancy' in label:
                                    benchmark_data['educational_consultancy_costs'] = amount
                                elif 'educational support staff' in label:
                                    benchmark_data['educational_support_staff_costs'] = amount
                                elif 'teaching staff' in label and 'support' not in label and 'supply' not in label:
                                    benchmark_data['teaching_staff_costs'] = amount
                                
                            except ValueError:
                                continue
        
        logger.info(f"📊 Extracted {len(benchmark_data)} benchmark data points")
        return benchmark_data
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data from FBIT website using URN
        UPDATED: Now also fetches benchmark comparison data
        """
        
        logger.info(f"Fetching financial data for URN {urn} ({'Trust' if is_trust else 'School'})")
        
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
            logger.warning("Failed to fetch FBIT page, falling back to search approach")
            return self._get_financial_data_from_search(urn, entity_name, is_trust)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
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
                        logger.info(f"Found in year balance: £{amount:,}")
                    elif 'revenue reserve' in label_text:
                        financial_data['revenue_reserve'] = amount
                        logger.info(f"Found revenue reserve: £{amount:,}")
        
        priority_wrappers = soup.find_all('div', class_='priority-wrapper')
        
        logger.info(f"Found {len(priority_wrappers)} spending priority items")
        
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
                    logger.info(f"Found teaching staff costs: £{amount:,} per pupil")
                elif 'Administrative supplies' in category:
                    financial_data['admin_supplies_per_pupil'] = amount
                    logger.info(f"Found admin supplies: £{amount:,} per pupil")
                else:
                    key = category.lower().replace(' ', '_') + '_per_pupil'
                    financial_data[key] = amount
                    logger.info(f"Found {category}: £{amount:,} per pupil")
            
            sqm_match = re.search(r'Spends\s+£([\d,]+)\s+per\s+square\s+metre', text)
            if sqm_match:
                amount = int(sqm_match.group(1).replace(',', ''))
                if 'Utilities' in category:
                    financial_data['utilities_per_sqm'] = amount
                    logger.info(f"Found utilities: £{amount:,} per square metre")
        
        logger.info("🔍 Fetching benchmark comparison data...")
        comparison_html = self._fetch_fbit_comparison_page(urn)
        
        if comparison_html:
            benchmark_data = self._parse_benchmark_data(comparison_html)
            
            if benchmark_data:
                financial_data['benchmark_data'] = benchmark_data
                logger.info(f"✅ Added {len(benchmark_data)} benchmark metrics")
            else:
                logger.warning("⚠️ No benchmark data extracted")
        else:
            logger.warning("⚠️ Could not fetch comparison page")
        
        all_text = soup.get_text()
        
        supply_match = re.search(r'Supply\s+staff\s+costs?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
        if supply_match:
            financial_data['supply_staff_costs'] = int(supply_match.group(1).replace(',', ''))
            logger.info(f"Found supply staff costs: £{financial_data['supply_staff_costs']:,}")
        
        indirect_match = re.search(r'Indirect\s+employee\s+expenses?[:\s]+£?([\d,]+)', all_text, re.IGNORECASE)
        if indirect_match:
            financial_data['indirect_employee_expenses'] = int(indirect_match.group(1).replace(',', ''))
            logger.info(f"Found indirect employee expenses: £{financial_data['indirect_employee_expenses']:,}")
        
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
                
                financial_data['per_school_estimates'] = {
                    'avg_indirect_employee': int(financial_data['indirect_employee_expenses'] / schools),
                    'avg_supply': int(financial_data.get('supply_staff_costs', 0) / schools) if financial_data.get('supply_staff_costs') else None
                }
            else:
                financial_data['recruitment_estimates'] = {
                    'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                    'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                    'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25)
                }
        elif 'teaching_staff_per_pupil' in financial_data:
            teaching_total_estimate = financial_data['teaching_staff_per_pupil'] * 200
            financial_data['recruitment_estimates'] = {
                'low': int(teaching_total_estimate * 0.03),
                'high': int(teaching_total_estimate * 0.05),
                'midpoint': int(teaching_total_estimate * 0.04),
                'note': 'Estimated from teaching costs'
            }
        
        financial_data['extraction_confidence'] = self._calculate_extraction_confidence(financial_data)
        
        return financial_data
    
    def _calculate_extraction_confidence(self, data: Dict[str, Any]) -> float:
        """Calculate confidence score for extracted data"""
        
        essential_fields = ['teaching_staff_per_pupil', 'in_year_balance', 'revenue_reserve']
        found_essential = sum(1 for field in essential_fields if field in data and data[field] is not None)
        
        optional_fields = ['admin_supplies_per_pupil', 'utilities_per_sqm', 'supply_staff_costs', 'indirect_employee_expenses']
        found_optional = sum(1 for field in optional_fields if field in data and data[field] is not None)
        
        benchmark_bonus = 0
        if 'benchmark_data' in data and data['benchmark_data']:
            benchmark_count = len(data['benchmark_data'])
            benchmark_bonus = min(benchmark_count / 7, 1.0) * 0.2
        
        confidence = (found_essential / len(essential_fields)) * 0.5 + (found_optional / len(optional_fields)) * 0.3 + benchmark_bonus
        
        return round(confidence, 2)
    
    def _get_financial_data_from_search(self, urn: str, entity_name: str, is_trust: bool) -> Dict[str, Any]:
        """Fallback method using search"""
        
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
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete recruitment cost intelligence for a school
        """
        
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
            'insights': self._generate_insights(financial_data, False),
            'comparison': self._get_benchmarks(financial_data),
            'conversation_starters': self._generate_cost_conversations(
                financial_data, 
                None,
                None
            )
        }
        
        return intelligence
    
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
    
    def _generate_insights(self, financial_data: Dict, is_trust: bool) -> List[str]:
        """Generate insights from financial data"""
        insights = []
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            if 'total_expenditure_per_pupil' in benchmark:
                insights.append(f"Total expenditure: £{benchmark['total_expenditure_per_pupil']:,} per pupil")
            
            if 'supply_teaching_staff_costs' in benchmark:
                insights.append(f"Supply teaching staff: £{benchmark['supply_teaching_staff_costs']:,} (opportunity for cost reduction)")
            
            if 'agency_supply_teaching_staff_costs' in benchmark:
                insights.append(f"Agency supply costs: £{benchmark['agency_supply_teaching_staff_costs']:,} (high-priority target)")
        
        if is_trust and 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'total_trust' in est:
                insights.append(f"Trust-wide annual recruitment spend: £{est['total_trust']:,}")
                insights.append(f"Average per school: £{est['per_school_avg']:,} (saving {est['economies_of_scale_saving']} vs independent)")
            
            if 'supply_staff_costs' in financial_data and 'per_school_estimates' in financial_data:
                supply_per = financial_data['per_school_estimates'].get('avg_supply', 0)
                if supply_per:
                    insights.append(f"Average supply costs per school: £{supply_per:,}")
        
        elif 'recruitment_estimates' in financial_data:
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
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmark = financial_data['benchmark_data']
            
            if benchmark.get('agency_supply_teaching_staff_costs', 0) > 0:
                starters.append(
                    f"I noticed you're spending £{benchmark['agency_supply_teaching_staff_costs']:,} on agency supply staff. "
                    f"Protocol Education offers competitive rates and exclusive arrangements that could reduce this by 20-30%. "
                    f"We guarantee quality and continuity that agencies often struggle to provide."
                )
            
            if benchmark.get('supply_teaching_staff_costs', 0) > 0:
                starters.append(
                    f"Your supply teaching costs of £{benchmark['supply_teaching_staff_costs']:,} suggest regular staffing gaps. "
                    f"Have you considered our long-term supply solutions? Many schools find they save 15-25% compared to daily booking."
                )
        
        if trust_name and 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'total_trust' in est:
                starters.append(
                    f"I see {trust_name} manages {schools_count or 'multiple'} schools with approximately "
                    f"£{est['total_trust']:,} in annual recruitment costs. Protocol Education's trust-wide "
                    "partnership could reduce this by 30-40% through economies of scale."
                )
                
                if schools_count and est.get('per_school_avg'):
                    starters.append(
                        f"With an average recruitment spend of £{est['per_school_avg']:,} per school, "
                        "a trust-wide agreement with Protocol could standardize quality while reducing costs."
                    )
        
        elif 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            if 'midpoint' in est:
                cost = est['midpoint']
                starters.append(
                    f"Your school spends approximately £{cost:,} annually on recruitment. "
                    "Protocol Education could help reduce these costs through our competitive rates and quality guarantee."
                )
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            if trust_name:
                starters.append(
                    f"With trust-wide supply costs totaling £{supply:,}, our dedicated trust team "
                    "can ensure consistent quality cover across all your schools."
                )
            else:
                starters.append(
                    f"Your £{supply:,} annual supply costs could be reduced through our long-term "
                    "staffing solutions and competitive daily rates."
                )
        
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                starters.append(
                    f"I noticed your school is managing a deficit of £{abs(balance):,}. "
                    "Protocol Education can help reduce recruitment costs as part of your financial recovery plan."
                )
        
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            if teaching > 5000:
                starters.append(
                    f"With teaching costs at £{teaching:,} per pupil, ensuring value in recruitment "
                    "is crucial. Protocol's quality guarantee ensures you get the best teachers at competitive rates."
                )
        
        return starters
    
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
        """Get benchmark comparisons"""
        benchmarks = {
            'national_average': {
                'indirect_employee_expenses': 35000,
                'supply_costs': 100000,
                'teaching_per_pupil': 5500
            },
            'comparison': {}
        }
        
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            avg = benchmarks['national_average']['teaching_per_pupil']
            diff_pct = ((teaching - avg) / avg) * 100
            benchmarks['comparison']['teaching_vs_average'] = f"{'+' if diff_pct > 0 else ''}{diff_pct:.1f}%"
        
        if 'benchmark_data' in financial_data and financial_data['benchmark_data']:
            benchmarks['school_benchmark_data'] = financial_data['benchmark_data']
        
        return benchmarks


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    """
    
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
                            source_url=financial_intel.get('financial', {}).get('source_url', ''),
                            relevance_score=0.9
                        )
                    )
            
            intel.financial_data = financial_intel
            
            logger.info(f"✅ Successfully enhanced {intel.school_name} with financial data")
        else:
            logger.warning(f"Could not get financial data for {intel.school_name}: {financial_intel.get('error')}")
    
    except Exception as e:
        logger.error(f"Error enhancing school with financial data: {e}")
    
    return intel
