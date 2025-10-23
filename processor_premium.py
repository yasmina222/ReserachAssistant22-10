"""
Protocol Education CI System - Premium Processor (ENHANCED)
Integrates the premium AI engine with existing Streamlit app
Enhanced: Added Ofsted analysis and vacancy detection
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

from ai_engine_premium import PremiumAIEngine
from email_pattern_validator import enhance_contacts_with_emails
from ofsted_analyzer_v2 import OfstedAnalyzer, integrate_ofsted_analyzer
from vacancy_detector import integrate_vacancy_detector
from financial_data_engine import enhance_school_with_financial_data
from models import (
    SchoolIntelligence, Contact, CompetitorPresence, 
    ConversationStarter, ContactType
)
from cache import IntelligenceCache

logger = logging.getLogger(__name__)

# Feature flags
ENABLE_OFSTED_ENHANCEMENT = True
ENABLE_VACANCY_DETECTION = True

class PremiumSchoolProcessor:
    """Processor that uses premium AI engine instead of web scraping"""
    
    def __init__(self):
        self.ai_engine = PremiumAIEngine()
        self.cache = IntelligenceCache()
        
    def process_single_school(self, school_name: str, 
                            website_url: Optional[str] = None,
                            force_refresh: bool = False) -> SchoolIntelligence:
        """Process a single school using premium AI research"""
        
        start_time = time.time()
        logger.info(f"Processing school: {school_name}")
        
        # Check cache first
        if not force_refresh:
            cached_data = self.cache.get(school_name, 'full_intelligence')
            if cached_data:
                logger.info(f"Returning cached data for {school_name}")
                return self._deserialize_intelligence(cached_data['data'])
        
        # Extract location from school name if possible
        location = None
        for borough in ['Camden', 'Islington', 'Westminster', 'Hackney', 'Tower Hamlets']:
            if borough.lower() in school_name.lower():
                location = borough
                break
        
        # Research using premium AI
        research_result = self.ai_engine.research_school(school_name, location)
        
        # Convert to SchoolIntelligence object
        intel = self._convert_to_intelligence(research_result, website_url)
        
        # Set processing time
        intel.processing_time = time.time() - start_time
        
        # ENHANCEMENT 1: Financial Data
        try:
            intel = enhance_school_with_financial_data(intel, self.ai_engine)
        except Exception as e:
            logger.error(f"Error enhancing with financial data: {e}")
            # Continue without financial data rather than failing completely
        
        # ENHANCEMENT 2: Deep Ofsted Analysis
        if ENABLE_OFSTED_ENHANCEMENT:
            try:
                enhance_with_ofsted = integrate_ofsted_analyzer(self)
                intel = enhance_with_ofsted(intel, self.ai_engine)
            except Exception as e:
                logger.error(f"Ofsted enhancement error: {e}")
        
        # ENHANCEMENT 3: Vacancy Detection
        if ENABLE_VACANCY_DETECTION:
            try:
                detect_vacancies = integrate_vacancy_detector(self)
                intel = detect_vacancies(intel, self.ai_engine)
            except Exception as e:
                logger.error(f"Vacancy detection error: {e}")
        
        # Sort conversation starters by priority
        intel.conversation_starters.sort(
            key=lambda x: x.relevance_score, 
            reverse=True
        )
        
        # Cache results
        self.cache.set(
            school_name, 
            'full_intelligence',
            self._serialize_intelligence(intel),
            research_result.get('sources', [])
        )
        
        logger.info(f"Completed {school_name} in {intel.processing_time:.2f}s")
        return intel
    
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
        
        # Enhance contacts with email generation
        if intel.website:
            # Look for any known emails in the data
            known_emails = []
            contact_details = data.get('CONTACT DETAILS', {})
            if verified_email := contact_details.get('Best verified email addresses'):
                if verified_email != 'Not found' and '@' in verified_email:
                    # Try to match to a contact
                    known_emails.append({
                        'email': verified_email,
                        'first_name': 'Unknown',
                        'last_name': 'Unknown'
                    })
            
            # Enhance contacts with generated emails
            intel.contacts = enhance_contacts_with_emails(
                intel.contacts, 
                intel.website,
                known_emails
            )
        
        # Extract competitors (if found)
        intel.competitors = self._extract_competitors(data)
        
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
            'Headteacher/Principal': ContactType.DEPUTY_HEAD,  # Often the key decision maker
            'Deputy Headteacher': ContactType.DEPUTY_HEAD,
            'Assistant Headteacher': ContactType.ASSISTANT_HEAD,
            'Business Manager': ContactType.BUSINESS_MANAGER,
            'SENCO': ContactType.SENCO
        }
        
        # Extract main email for pattern
        main_email = contact_details.get('Best verified email addresses')
        if not main_email or main_email == 'Not found':
            main_email = data.get('BASIC INFORMATION', {}).get('Main email address')
        
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
    
    def _extract_competitors(self, data: Dict[str, Any]) -> List[CompetitorPresence]:
        """Extract competitor information from premium AI data"""
        
        competitors = []
        recruit_intel = data.get('RECRUITMENT INTELLIGENCE', {})
        
        # Check if any agencies were mentioned
        agencies = recruit_intel.get('Any recruitment agencies mentioned in connection with the school')
        if agencies and agencies != 'Not found':
            # Parse agency names if found
            if isinstance(agencies, str):
                agency_names = [a.strip() for a in agencies.split(',')]
            else:
                agency_names = agencies
                
            for agency in agency_names:
                competitor = CompetitorPresence(
                    agency_name=agency,
                    presence_type='mentioned',
                    evidence_urls=data.get('sources', [])[:2],
                    confidence_score=0.7
                )
                competitors.append(competitor)
        
        return competitors
    
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
    
    def process_borough(self, borough_name: str, 
                       school_type: str = 'all') -> List[SchoolIntelligence]:
        """Process all schools in a borough"""
        
        logger.info(f"Processing borough: {borough_name}, type: {school_type}")
        
        # For now, use a predefined list - in production, this would search for schools
        # You could enhance this to use the AI to first get a list of schools
        test_schools = [
            f"Primary School 1 {borough_name}",
            f"Secondary School 1 {borough_name}",
            f"Academy 1 {borough_name}"
        ]
        
        results = []
        for school_name in test_schools:
            try:
                intel = self.process_single_school(school_name)
                results.append(intel)
            except Exception as e:
                logger.error(f"Failed to process {school_name}: {e}")
                
        return results
    
    def _serialize_intelligence(self, intel: SchoolIntelligence) -> Dict[str, Any]:
        """Convert SchoolIntelligence to dict for caching"""
        
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
            'competitors': [
                {
                    'agency_name': c.agency_name,
                    'presence_type': c.presence_type,
                    'confidence_score': c.confidence_score
                }
                for c in intel.competitors
            ],
            'ofsted_rating': intel.ofsted_rating,
            'ofsted_date': intel.ofsted_date.isoformat() if intel.ofsted_date else None,
            'conversation_starters': [
                {
                    'topic': s.topic,
                    'detail': s.detail,
                    'relevance_score': s.relevance_score
                }
                for s in intel.conversation_starters
            ],
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
            # Serialize vacancy data (excluding objects)
            serialized['vacancy_data'] = {
                'total_found': intel.vacancy_data['total_found'],
                'senior_roles': intel.vacancy_data['senior_roles'],
                'analysis': intel.vacancy_data['analysis']
            }
            
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
        
        # Recreate competitors
        for comp_data in data.get('competitors', []):
            competitor = CompetitorPresence(
                agency_name=comp_data['agency_name'],
                presence_type=comp_data['presence_type'],
                confidence_score=comp_data.get('confidence_score', 0.5)
            )
            intel.competitors.append(competitor)
            
        # Recreate conversation starters
        for starter_data in data.get('conversation_starters', []):
            starter = ConversationStarter(
                topic=starter_data['topic'],
                detail=starter_data['detail'],
                source_url='',
                relevance_score=starter_data.get('relevance_score', 0.7)
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
