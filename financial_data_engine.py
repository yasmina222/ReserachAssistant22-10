import re
import logging
import requests
from typing import Dict, Optional, List, Any
from datetime import datetime
from models import ConversationStarter

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data URLs from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        
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
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Get financial data URL for a school - SIMPLIFIED VERSION
        Just returns the link, no scraping needed
        """
        
        logger.info(f"Getting financial URL for: {school_name}")
        
        # Step 1: Get school URN
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            logger.error(f"Could not find URN for {school_name}")
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        urn = urn_result['urn']
        logger.info(f"Found URN {urn} for {urn_result['official_name']}")
        
        # Step 2: Build the financial data URL
        financial_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        # Step 3: Verify the URL exists (quick check)
        url_valid = self._verify_url_exists(financial_url)
        
        # Step 4: Return the intelligence with the URL
        intelligence = {
            'school_searched': school_name,
            'entity_found': {
                'name': urn_result['official_name'],
                'type': 'School',
                'urn': urn,
                'location': urn_result.get('address', ''),
                'trust_name': urn_result.get('trust_name'),
                'confidence': urn_result['confidence']
            },
            'financial_url': financial_url,
            'url_valid': url_valid,
            'conversation_starters': [
                f"I've reviewed your school's financial data at {financial_url}. "
                f"Protocol Education can help optimize your recruitment spending and reduce agency costs."
            ]
        }
        
        logger.info(f"✅ Financial URL generated: {financial_url}")
        
        return intelligence
    
    def _verify_url_exists(self, url: str) -> bool:
        """Quick check if URL returns 200"""
        try:
            response = requests.head(url, timeout=5, allow_redirects=True)
            return response.status_code == 200
        except:
            return False
    
    def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
        """Calculate confidence score for name match"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        if search_name == result_name:
            return 1.0
        
        if search_name in result_name or result_name in search_name:
            return 0.7
        
        search_words = set(search_name.split())
        result_words = set(result_name.split())
        common_words = search_words.intersection(result_words)
        
        if common_words:
            return 0.5 + (0.2 * len(common_words) / len(search_words))
        
        return 0.3
    
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


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data URL to existing school intelligence
    SIMPLIFIED - Just adds the link, no scraping
    """
    
    try:
        logger.info(f"Adding financial URL for {intel.school_name}")
        
        financial_engine = FinancialDataEngine(serper_engine)
        
        financial_intel = financial_engine.get_recruitment_intelligence(
            intel.school_name,
            intel.address
        )
        
        if not financial_intel.get('error'):
            # Store the financial URL
            intel.financial_data = {
                'url': financial_intel['financial_url'],
                'urn': financial_intel['entity_found']['urn'],
                'school_name': financial_intel['entity_found']['name'],
                'url_valid': financial_intel['url_valid']
            }
            
            # Add conversation starter with the link
            for starter_text in financial_intel.get('conversation_starters', []):
                intel.conversation_starters.append(
                    ConversationStarter(
                        topic="Financial Data",
                        detail=starter_text,
                        source_url=financial_intel['financial_url'],
                        relevance_score=0.9
                    )
                )
            
            logger.info(f"✅ Financial URL added: {financial_intel['financial_url']}")
        else:
            logger.warning(f"⚠️ Could not find financial data for {intel.school_name}")
    
    except Exception as e:
        logger.error(f"❌ Error adding financial URL: {e}")
    
    return intel
