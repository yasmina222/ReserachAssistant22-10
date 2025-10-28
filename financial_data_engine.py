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
        """
        Find school URN using government database
        FIXED: Better error handling and logging
        """
        
        search_query = f'"{school_name}"'
        if location:
            search_query += f' {location}'
        search_query += ' site:get-information-schools.service.gov.uk'
        
        logger.info(f"🔍 Searching for school URN: {search_query}")
        
        try:
            results = self.serper.search_web(search_query, num_results=5)
            logger.info(f"📡 Serper returned {len(results) if results else 0} results")
        except Exception as e:
            logger.error(f"❌ Serper search failed: {e}")
            return {'urn': None, 'confidence': 0.0, 'error': f'Search failed: {e}'}
        
        if not results:
            logger.warning(f"⚠️ No results from Serper for {school_name}")
            return self._search_fbit_direct(school_name, location)
        
        urn_matches = []
        for result in results:
            url = result.get('url', '')
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            
            logger.debug(f"Checking result: {url[:100]}")
            
            # Skip trust/group pages
            if '/Groups/Group/' in url:
                logger.debug("Skipping group page")
                continue
                
            urn_from_url = None
            
            # GIAS pattern: /Establishments/Establishment/Details/123456
            gias_match = re.search(r'/Establishments/Establishment/Details/(\d{5,7})', url)
            if gias_match:
                urn_from_url = gias_match.group(1)
                logger.info(f"✅ Found URN from GIAS URL: {urn_from_url}")
            
            # Also check text for URN
            if not urn_from_url:
                urn_pattern = r'URN:?\s*(\d{5,7})'
                urn_match = re.search(urn_pattern, text)
                if urn_match:
                    urn_from_url = urn_match.group(1)
                    logger.info(f"✅ Found URN from text: {urn_from_url}")
            
            if urn_from_url:
                official_name = self._extract_school_name(result)
                trust_name = None
                
                # Check if school is part of a trust (just for info)
                if 'trust' in text.lower() or 'academy trust' in text.lower():
                    trust_pattern = r'Part of\s+([A-Z][A-Za-z\s&]+(?:Trust|Federation))'
                    trust_match = re.search(trust_pattern, text, re.IGNORECASE)
                    if trust_match:
                        trust_name = trust_match.group(1).strip()
                        logger.info(f"ℹ️ School is part of trust: {trust_name}")
                
                confidence = self._calculate_name_match(school_name, result, False)
                
                urn_matches.append({
                    'urn': urn_from_url,
                    'official_name': official_name,
                    'trust_name': trust_name,
                    'address': self._extract_location(result),
                    'url': url,
                    'confidence': confidence
                })
                
                logger.info(f"Added URN match: {urn_from_url} with confidence {confidence:.2f}")
        
        if not urn_matches:
            logger.warning(f"❌ No URN found for {school_name}")
            return {'urn': None, 'confidence': 0.0, 'error': 'No URN found in search results'}
        
        # Sort by confidence and return best match
        urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
        best_match = urn_matches[0]
        best_match['alternatives'] = urn_matches[1:3] if len(urn_matches) > 1 else []
        
        logger.info(f"🎯 Best URN match: {best_match['urn']} for {best_match['official_name']} (confidence: {best_match['confidence']:.2f})")
        
        return best_match
    
    def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
        """Calculate confidence score for name match"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        # Exact match
        if search_name == result_name:
            return 1.0
        
        # Contains match
        if search_name in result_name or result_name in search_name:
            return 0.7
        
        # Partial word match
        search_words = set(search_name.split())
        result_words = set(result_name.split())
        common_words = search_words.intersection(result_words)
        
        if common_words:
            return 0.5 + (0.2 * len(common_words) / len(search_words))
        
        return 0.3
    
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
        """
        Try searching FBIT directly as fallback
        FIXED: Better error handling
        """
        search_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            search_query += f' {location}'
        
        logger.info(f"🔄 Trying FBIT direct search: {search_query}")
        
        try:
            results = self.serper.search_web(search_query, num_results=3)
        except Exception as e:
            logger.error(f"❌ FBIT direct search failed: {e}")
            return {'urn': None, 'confidence': 0.0, 'error': f'FBIT search failed: {e}'}
        
        if not results:
            return {'urn': None, 'confidence': 0.0, 'error': 'No FBIT results found'}
        
        for result in results:
            url = result.get('url', '')
            # Extract URN from FBIT URL pattern: /school/123456
            urn_match = re.search(r'/school/(\d{5,7})', url)
            if urn_match:
                urn = urn_match.group(1)
                logger.info(f"✅ Found URN from FBIT URL: {urn}")
                return {
                    'urn': urn,
                    'official_name': self._extract_school_name(result),
                    'confidence': 0.7,
                    'trust_name': None,
                    'address': '',
                    'alternatives': []
                }
        
        return {'urn': None, 'confidence': 0.0, 'error': 'No URN found in FBIT results'}


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial URL to school intelligence - AUGUST WORKING VERSION RESTORED
    CRITICAL FIX: Robust error handling + always creates financial_data structure
    """
    
    logger.info(f"🎯 Starting financial data enhancement for {intel.school_name}")
    
    try:
        financial_engine = FinancialDataEngine(serper_engine)
        
        # Try to get the URN
        logger.info(f"📞 Calling get_school_urn for {intel.school_name}")
        urn_result = financial_engine.get_school_urn(intel.school_name, intel.address)
        
        # Log the result
        logger.info(f"📊 URN result: {urn_result}")
        
        if urn_result.get('urn'):
            urn = urn_result['urn']
            school_name = urn_result.get('official_name', intel.school_name)
            
            # Build the government website URL
            financial_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
            
            logger.info(f"✅ Financial URL created: {financial_url}")
            
            # CRITICAL: Store in the EXACT structure that Streamlit expects
            intel.financial_data = {
                'url': financial_url,
                'urn': urn,
                'school_name': school_name,
                'entity_type': 'School',
                'url_valid': True
            }
            
            # Add conversation starter with proper ConversationStarter object
            try:
                intel.conversation_starters.append(
                    ConversationStarter(
                        topic="Financial Data Available",
                        detail=f"View {school_name}'s financial data including spending priorities, staff costs per pupil, revenue reserves, and benchmarking comparisons on the UK Government's Financial Benchmarking and Insights Tool.",
                        source_url=financial_url,
                        relevance_score=0.9
                    )
                )
                logger.info("✅ Added financial conversation starter")
            except Exception as e:
                logger.error(f"⚠️ Could not add conversation starter: {e}")
            
            logger.info(f"✅ SUCCESS: Financial data added for {intel.school_name}")
            
        else:
            # URN not found - log the error but don't crash
            error_msg = urn_result.get('error', 'Unknown error')
            logger.warning(f"⚠️ Could not find URN for {intel.school_name}: {error_msg}")
            
            # Still create financial_data but mark as unavailable
            intel.financial_data = {
                'url': None,
                'urn': None,
                'school_name': intel.school_name,
                'entity_type': 'School',
                'url_valid': False,
                'error': error_msg
            }
    
    except Exception as e:
        # Catch ANY error and log it with full details
        logger.error(f"❌ EXCEPTION in enhance_school_with_financial_data: {str(e)}", exc_info=True)
        
        # Create a minimal financial_data structure so we don't break downstream
        intel.financial_data = {
            'url': None,
            'urn': None,
            'school_name': intel.school_name,
            'entity_type': 'School',
            'url_valid': False,
            'error': f'Failed to retrieve financial data: {str(e)}'
        }
    
    return intel
