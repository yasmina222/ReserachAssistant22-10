"""
Protocol Education CI System - Premium Processor (ASYNC PARALLELIZED)
PHASE 1 OPTIMIZATION: Parallel execution of all intelligence modules
Reduces processing time from 60-90s to 15-25s per school
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import time
import asyncio

from ai_engine_premium_async import PremiumAIEngineAsync
from email_pattern_validator import enhance_contacts_with_emails
from ofsted_analyzer_v2_async import OfstedAnalyzerAsync, integrate_ofsted_analyzer_async
from vacancy_detector_async import integrate_vacancy_detector_async
from financial_data_engine_async import enhance_school_with_financial_data_async
from models import (SchoolIntelligence, Contact, ConversationStarter, ContactType)
from cache_async import IntelligenceCacheAsync

logger = logging.getLogger(__name__)

# Feature flags
ENABLE_OFSTED_ENHANCEMENT = True
ENABLE_VACANCY_DETECTION = True

class PremiumSchoolProcessorAsync:
    """Async processor that runs all modules in parallel for maximum speed"""
    
    def __init__(self):
        self.ai_engine = PremiumAIEngineAsync()
        self.cache = IntelligenceCacheAsync()
        
    async def process_single_school(self, school_name: str, 
                            website_url: Optional[str] = None,
                            force_refresh: bool = False) -> SchoolIntelligence:
        """Process a single school using parallel AI research"""
        
        start_time = time.time()
        logger.info(f"Processing school: {school_name}")
        
        # Check cache first
        if not force_refresh:
            cached_data = await self.cache.get(school_name, 'full_intelligence')
            if cached_data:
                logger.info(f"Returning cached data for {school_name}")
                return self._deserialize_intelligence(cached_data['data'])
        
        # Extract location from school name if possible
        location = None
        for borough in ['Camden', 'Islington', 'Westminster', 'Hackney', 'Tower Hamlets', 
                       'Birmingham', 'Manchester', 'Leeds', 'Bristol', 'Liverpool']:
            if borough.lower() in school_name.lower():
                location = borough
                break
        
        # PARALLEL EXECUTION - Run all modules concurrently
        logger.info(f"Starting parallel research for {school_name}")
        
        try:
            # Run all independent modules in parallel
            results = await asyncio.gather(
                # Core research (provides basic data)
                self.ai_engine.research_school(school_name, location),
                
                # These can all run independently
                self._run_financial_analysis(school_name, location),
                self._run_ofsted_analysis(school_name) if ENABLE_OFSTED_ENHANCEMENT else self._empty_result('ofsted'),
                self._run_vacancy_detection(school_name, website_url) if ENABLE_VACANCY_DETECTION else self._empty_result('vacancy'),
                
                return_exceptions=True  # Don't fail entire process if one module fails
            )
            
            # Unpack results
            basic_research, financial_data, ofsted_data, vacancy_data = results
            
            # Handle any exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    module_name = ['basic_research', 'financial', 'ofsted', 'vacancy'][i]
                    logger.error(f"{module_name} failed: {result}")
            
        except Exception as e:
            logger.error(f"Parallel execution error: {e}")
            # Fallback to basic research only
            basic_research = await self.ai_engine.research_school(school_name, location)
            financial_data = None
            ofsted_data = None
            vacancy_data = None
        
        # Convert basic research to SchoolIntelligence object
        intel = self._convert_to_intelligence(basic_research, website_url)
        
        # Enhance with email generation (requires contacts from basic research)
        if intel.website:
            known_emails = self._extract_known_emails(basic_research)
            intel.contacts = enhance_contacts_with_emails(
                intel.contacts, 
                intel.website,
                known_emails
            )
        
        # Add financial data if successful
        if financial_data and not isinstance(financial_data, Exception):
            intel.financial_data = financial_data
            self._add_financial_conversations(intel, financial_data)
        
        # Add Ofsted data if successful
        if ofsted_data and not isinstance(ofsted_data, Exception):
            intel.ofsted_enhanced = ofsted_data
            self._add_ofsted_conversations(intel, ofsted_data)
        
        # Add vacancy data if successful
        if vacancy_data and not isinstance(vacancy_data, Exception):
            intel.vacancy_data = vacancy_data
            self._add_vacancy_conversations(intel, vacancy_data)
        
        # Sort conversation starters by priority
        intel.conversation_starters.sort(
            key=lambda x: x.relevance_score, 
            reverse=True
        )
        
        # Set processing time
        intel.processing_time = time.time() - start_time
        
        # Cache results WITH PROPER SERIALIZATION
        try:
            serialized = self._serialize_intelligence(intel)
            await self.cache.set(
                school_name, 
                'full_intelligence',
                serialized,
                basic_research.get('sources', [])
            )
            logger.info(f"✅ Successfully cached data for {school_name}")
        except Exception as e:
            logger.error(f"❌ Error caching data for {school_name}: {e}")
        
        logger.info(f"Completed {school_name} in {intel.processing_time:.2f}s")
        return intel
    
    async def _run_financial_analysis(self, school_name: str, location: Optional[str]) -> Optional[Dict[str, Any]]:
        """Run financial analysis module"""
        try:
            from financial_data_engine_async import FinancialDataEngineAsync
            financial_engine = FinancialDataEngineAsync(self.ai_engine)
            
            financial_intel = await financial_engine.get_recruitment_intelligence(
                school_name,
                location
            )
            
            if not financial_intel.get('error'):
                return financial_intel
            else:
                logger.warning(f"Financial analysis returned error: {financial_intel['error']}")
                return None
                
        except Exception as e:
            logger.error(f"Financial analysis error: {e}")
            return None
    
    async def _run_ofsted_analysis(self, school_name: str) -> Optional[Dict[str, Any]]:
        """Run Ofsted analysis module"""
        try:
            analyzer = OfstedAnalyzerAsync(self.ai_engine, self.ai_engine.openai_client)
            
            # Need basic Ofsted info first - get from basic research if cached
            basic_ofsted = {
                'rating': 'Unknown',
                'inspection_date': None
            }
            
            enhanced_ofsted = await analyzer.get_enhanced_ofsted_analysis(
                school_name,
                basic_ofsted
            )
            
            return enhanced_ofsted
            
        except Exception as e:
            logger.error(f"Ofsted analysis error: {e}")
            return None
    
    async def _run_vacancy_detection(self, school_name: str, website: Optional[str]) -> Optional[Dict[str, Any]]:
        """Run vacancy detection module"""
        try:
            from vacancy_detector_async import VacancyDetectorAsync
            detector = VacancyDetectorAsync(
                self.ai_engine,
                self.ai_engine.openai_client
            )
            
            vacancy_data = await detector.detect_vacancies(
                school_name,
                website
            )
            
            return vacancy_data
            
        except Exception as e:
            logger.error(f"Vacancy detection error: {e}")
            return None
    
    async def _empty_result(self, module_type: str) -> None:
        """Return empty result for disabled modules"""
        return None
    
    def _extract_known_emails(self, research_result: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract known emails from research results for pattern detection"""
        known_emails = []
        data = research_result.get('data', {})
        
        contact_details = data.get('CONTACT DETAILS', {})
        if verified_email := contact_details.get('Best verified email addresses'):
            if verified_email != 'Not found' and '@' in verified_email:
                known_emails.append({
                    'email': verified_email,
                    'first_name': 'Unknown',
                    'last_name': 'Unknown'
                })
        
        return known_emails
    
    def _add_financial_conversations(self, intel: SchoolIntelligence, financial_data: Dict[str, Any]):
        """Add financial conversation starters to intelligence"""
        if 'conversation_starters' in financial_data:
            for starter in financial_data['conversation_starters']:
                intel.conversation_starters.append(
                    ConversationStarter(
                        topic="Recruitment Costs",
                        detail=starter,
                        source_url=financial_data.get('financial', {}).get('source_url', ''),
                        relevance_score=0.9
                    )
                )
    
    def _add_ofsted_conversations(self, intel: SchoolIntelligence, ofsted_data: Dict[str, Any]):
        """Add Ofsted conversation starters to intelligence"""
        if ofsted_data.get('conversation_starters'):
            for starter in ofsted_data['conversation_starters']:
                if isinstance(starter, ConversationStarter):
                    intel.conversation_starters.append(starter)
    
    def _add_vacancy_conversations(self, intel: SchoolIntelligence, vacancy_data: Dict[str, Any]):
        """Add vacancy conversation starters to intelligence"""
        if vacancy_data.get('conversation_starters'):
            for starter in vacancy_data['conversation_starters']:
                if isinstance(starter, ConversationStarter):
                    intel.conversation_starters.append(starter)
        
        # Add competitors from vacancies
        if vacancy_data.get('analysis', {}).get('competitors_active'):
            from models import CompetitorPresence
            for competitor in vacancy_data['analysis']['competitors_active']:
                existing = next((c for c in intel.competitors 
                               if c.agency_name == competitor), None)
                
                if not existing:
                    intel.competitors.append(
                        CompetitorPresence(
                            agency_name=competitor,
                            presence_type='job_posting',
                            evidence_urls=[v.url for v in vacancy_data.get('vacancies', []) 
                                         if hasattr(v, 'competitor_mentioned') and 
                                         v.competitor_mentioned == competitor][:2],
                            confidence_score=0.9,
                            weaknesses=['May not have exclusive arrangement']
                        )
                    )
    
    def _convert_to_intelligence(self, research_result: Dict[str, Any], 
                               provided_website: Optional[str] = None) -> SchoolIntelligence:
        """Convert premium AI results to SchoolIntelligence model"""
        
        data = research_result.get('data', {})
        
        # Initialize intelligence object
        intel = SchoolIntelligence(
            school_name=research_result.get('school_name', ''),
            website=provided_website or data.get('BASIC INFORMATION', {}).get('Website URL', '')
        )
        
        # Extract basic info
        basic_info = data.get('BASIC INFORMATION', {})
        intel.address = basic_info.get('Full address')
        intel.phone_main = basic_info.get('Main phone number')
        
        # Extract contacts
        intel.contacts = self._extract_contacts(data)
        
        # Extract Ofsted info
        ofsted_info = data.get('OFSTED INFORMATION', {})
        intel.ofsted_rating = ofsted_info.get('Current Ofsted rating')
        if ofsted_date := ofsted_info.get('Date of last inspection'):
            try:
                intel.ofsted_date = datetime.strptime(ofsted_date, '%d %B %Y')
            except:
                pass
        
        # Extract conversation starters
        starters = data.get('CONVERSATION STARTERS for recruitment consultants', [])
        for i, starter_text in enumerate(starters):
            if isinstance(starter_text, str):
                intel.conversation_starters.append(
                    ConversationStarter(
                        topic=f"Talking Point {i+1}",
                        detail=starter_text,
                        source_url=research_result.get('sources', [''])[0] if research_result.get('sources') else '',
                        relevance_score=0.9
                    )
                )
        
        # Extract recent updates
        recent = data.get('RECENT SCHOOL NEWS (2023-2024)', {})
        if achievements := recent.get('Recent achievements or awards'):
            if achievements != 'Not found':
                intel.recent_achievements = [achievements] if isinstance(achievements, str) else achievements
        
        if events := recent.get('Major events or initiatives'):
            if events != 'Not found':
                intel.upcoming_events = [events] if isinstance(events, str) else events
                
        if changes := recent.get('Leadership changes'):
            if changes != 'Not found':
                intel.leadership_changes = [changes] if isinstance(changes, str) else changes
        
        # Set data quality score
        intel.data_quality_score = self._calculate_quality_score(intel)
        
        # Set metadata
        intel.sources_checked = len(research_result.get('sources', []))
        
        return intel
    
    def _extract_contacts(self, data: Dict[str, Any]) -> List[Contact]:
        """Extract contacts from premium AI data"""
        
        contacts = []
        leadership = data.get('KEY LEADERSHIP CONTACTS', {})
        contact_details = data.get('CONTACT DETAILS', {})
        
        # Map AI roles to our ContactType enum
        role_mapping = {
            'Headteacher/Principal': ContactType.DEPUTY_HEAD,
            'Deputy Headteacher': ContactType.DEPUTY_HEAD,
            'Assistant Headteacher': ContactType.ASSISTANT_HEAD,
            'Business Manager': ContactType.BUSINESS_MANAGER,
            'SENCO': ContactType.SENCO
        }
        
        for ai_role, contact_type in role_mapping.items():
            names = leadership.get(ai_role, 'Not found')
            
            if names and names != 'Not found':
                # Handle both single names and lists
                if isinstance(names, list):
                    name_list = names
                else:
                    name_list = [names]
                
                for name in name_list:
                    # Skip if it's just the first person for deputy/assistant roles
                    if contact_type in [ContactType.DEPUTY_HEAD, ContactType.ASSISTANT_HEAD]:
                        if len(contacts) > 0 and any(c.role == contact_type for c in contacts):
                            continue
                    
                    contact = Contact(
                        role=contact_type,
                        full_name=name,
                        email=None,  # Will be enhanced later
                        phone=data.get('BASIC INFORMATION', {}).get('Main phone number'),
                        confidence_score=0.8 if name != 'Not found' else 0.0,
                        evidence_urls=data.get('sources', [])[:3],
                        verification_method="Premium AI Research"
                    )
                    contacts.append(contact)
        
        return contacts
    
    def _calculate_quality_score(self, intel: SchoolIntelligence) -> float:
        """Calculate overall data quality score"""
        
        scores = []
        
        # Basic info (20%)
        if intel.website:
            scores.append(0.1)
        if intel.phone_main and intel.phone_main != 'Not found':
            scores.append(0.1)
            
        # Contacts (40%) - most important
        if intel.contacts:
            contact_score = sum(c.confidence_score for c in intel.contacts) / len(intel.contacts)
            scores.append(contact_score * 0.4)
        else:
            scores.append(0)
            
        # Ofsted (20%)
        if intel.ofsted_rating and intel.ofsted_rating != 'Not found':
            scores.append(0.2)
        else:
            scores.append(0)
            
        # Conversation intelligence (20%)
        if intel.conversation_starters:
            scores.append(0.2)
        else:
            scores.append(0)
            
        return sum(scores)
    
    async def process_borough(self, borough_name: str, 
                       school_type: str = 'all') -> List[SchoolIntelligence]:
        """Process all schools in a borough - with parallel execution"""
        
        logger.info(f"Processing borough: {borough_name}, type: {school_type}")
        
        # For now, use a predefined list - in production, this would search for schools
        test_schools = [
            f"Primary School 1 {borough_name}",
            f"Secondary School 1 {borough_name}",
            f"Academy 1 {borough_name}"
        ]
        
        # Process all schools in parallel
        tasks = [self.process_single_school(school_name) for school_name in test_schools]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out any exceptions
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to process {test_schools[i]}: {result}")
            else:
                valid_results.append(result)
                
        return valid_results
    
    def _serialize_intelligence(self, intel: SchoolIntelligence) -> Dict[str, Any]:
        """Convert SchoolIntelligence to dict for caching"""
        
        # Properly serialize conversation starters
        conversation_starters_serialized = []
        for starter in intel.conversation_starters:
            if isinstance(starter, ConversationStarter):
                conversation_starters_serialized.append({
                    'topic': starter.topic,
                    'detail': starter.detail,
                    'source_url': starter.source_url if hasattr(starter, 'source_url') else '',
                    'relevance_score': starter.relevance_score
                })
            elif isinstance(starter, dict):
                conversation_starters_serialized.append(starter)
            else:
                conversation_starters_serialized.append({
                    'topic': 'General',
                    'detail': str(starter),
                    'source_url': '',
                    'relevance_score': 0.7
                })
        
        serialized = {
            'school_name': intel.school_name,
            'website': intel.website,
            'address': intel.address,
            'phone_main': intel.phone_main,
            'contacts': [
                {
                    'role': c.role.value,
                    'full_name': c.full_name,
                    'email': c.email,
                    'phone': c.phone,
                    'confidence_score': c.confidence_score
                }
                for c in intel.contacts
            ],
            'ofsted_rating': intel.ofsted_rating,
            'ofsted_date': intel.ofsted_date.isoformat() if intel.ofsted_date else None,
            'conversation_starters': conversation_starters_serialized,
            'data_quality_score': intel.data_quality_score
        }
        
        # Include financial data if present
        if hasattr(intel, 'financial_data') and intel.financial_data:
            serialized['financial_data'] = intel.financial_data
        
        # Include Ofsted enhanced data if present
        if hasattr(intel, 'ofsted_enhanced') and intel.ofsted_enhanced:
            serialized['ofsted_enhanced'] = intel.ofsted_enhanced
            
        # Include vacancy data if present
        if hasattr(intel, 'vacancy_data') and intel.vacancy_data:
            serialized['vacancy_data'] = intel.vacancy_data
            
        return serialized
    
    def _deserialize_intelligence(self, data: Dict[str, Any]) -> SchoolIntelligence:
        """Convert dict back to SchoolIntelligence"""
        
        intel = SchoolIntelligence(
            school_name=data['school_name'],
            website=data.get('website', ''),
            address=data.get('address'),
            phone_main=data.get('phone_main')
        )
        
        # Recreate contacts
        for c_data in data.get('contacts', []):
            contact = Contact(
                role=ContactType(c_data['role']),
                full_name=c_data['full_name'],
                email=c_data.get('email'),
                phone=c_data.get('phone'),
                confidence_score=c_data.get('confidence_score', 0.5)
            )
            intel.contacts.append(contact)
            
        # Recreate conversation starters FROM DICTS
        for starter_data in data.get('conversation_starters', []):
            date_obj = None
            if starter_data.get('date'):
                try:
                    date_obj = datetime.fromisoformat(starter_data['date'])
                except:
                    pass
            
            starter = ConversationStarter(
                topic=starter_data.get('topic', 'General'),
                detail=starter_data.get('detail', ''),
                source_url=starter_data.get('source_url', ''),
                relevance_score=starter_data.get('relevance_score', 0.7),
                date=date_obj
            )
            intel.conversation_starters.append(starter)
        
        intel.ofsted_rating = data.get('ofsted_rating')
        if data.get('ofsted_date'):
            try:
                intel.ofsted_date = datetime.fromisoformat(data['ofsted_date'])
            except:
                pass
                
        intel.data_quality_score = data.get('data_quality_score', 0.0)
        
        # Restore financial data if present
        if 'financial_data' in data:
            intel.financial_data = data['financial_data']
        
        # Restore enhanced Ofsted data if present
        if 'ofsted_enhanced' in data:
            intel.ofsted_enhanced = data['ofsted_enhanced']
            
        # Restore vacancy data if present
        if 'vacancy_data' in data:
            intel.vacancy_data = data['vacancy_data']
        
        return intel


# Synchronous wrapper for backward compatibility
class PremiumSchoolProcessor:
    """Synchronous wrapper that maintains backward compatibility"""
    
    def __init__(self):
        self.async_processor = PremiumSchoolProcessorAsync()
    
    def process_single_school(self, school_name: str, 
                            website_url: Optional[str] = None,
                            force_refresh: bool = False) -> SchoolIntelligence:
        """Synchronous wrapper for process_single_school"""
        return asyncio.run(
            self.async_processor.process_single_school(school_name, website_url, force_refresh)
        )
    
    def process_borough(self, borough_name: str, 
                       school_type: str = 'all') -> List[SchoolIntelligence]:
        """Synchronous wrapper for process_borough"""
        return asyncio.run(
            self.async_processor.process_borough(borough_name, school_type)
        )
    
    @property
    def ai_engine(self):
        """Provide access to AI engine for usage stats"""
        return self.async_processor.ai_engine
