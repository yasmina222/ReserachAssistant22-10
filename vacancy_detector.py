"""
Protocol Education CI System - Job Vacancy Detector
Searches for active job postings and matches to candidate database
"""

import re
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from models import ConversationStarter

logger = logging.getLogger(__name__)

@dataclass
class JobVacancy:
    """Represents a detected job vacancy"""
    title: str
    school_name: str
    posted_date: Optional[datetime]
    salary_range: Optional[str]
    contract_type: str  # permanent, temporary, maternity cover
    key_requirements: List[str]
    source: str  # school website, TES, etc.
    url: str
    urgency_score: float  # 0-1, based on posting date and role seniority
    competitor_mentioned: Optional[str] = None

class VacancyDetector:
    """Detects and analyzes job vacancies for schools"""
    
    def __init__(self, serper_engine, openai_client):
        self.serper = serper_engine
        self.openai = openai_client
        
        # Senior roles to prioritize
        self.senior_roles = [
            'headteacher', 'head teacher', 'principal',
            'deputy head', 'assistant head',
            'senco', 'sendco',
            'head of department', 'subject lead',
            'phase leader', 'key stage coordinator'
        ]
        
        # Education job boards
        self.job_boards = {
            'tes.com': 'TES Jobs',
            'education.guardian.com': 'Guardian Education',
            'cv-library.co.uk': 'CV-Library',
            'indeed.co.uk': 'Indeed',
            'reed.co.uk': 'Reed',
            'eteach.com': 'eTeach'
        }
    
    def detect_vacancies(self, school_name: str, website: Optional[str] = None) -> Dict[str, Any]:
        """
        Detect job vacancies for a school across multiple sources
        
        Returns:
            Dict containing vacancies, analysis, and conversation starters
        """
        
        logger.info(f"Detecting vacancies for {school_name}")
        
        vacancies = []
        
        # Step 1: Search school website for vacancies
        if website:
            school_vacancies = self._search_school_website(school_name, website)
            vacancies.extend(school_vacancies)
        
        # Step 2: Search job boards
        board_vacancies = self._search_job_boards(school_name)
        vacancies.extend(board_vacancies)
        
        # Step 3: Deduplicate and analyze
        unique_vacancies = self._deduplicate_vacancies(vacancies)
        
        # Step 4: Analyze vacancy patterns
        analysis = self._analyze_vacancy_patterns(unique_vacancies)
        
        # Step 5: Generate conversation starters
        conversation_starters = self._generate_vacancy_conversations(
            unique_vacancies, 
            analysis
        )
        
        return {
            'vacancies': unique_vacancies,
            'total_found': len(unique_vacancies),
            'senior_roles': sum(1 for v in unique_vacancies if v.urgency_score > 0.7),
            'analysis': analysis,
            'conversation_starters': conversation_starters,
            'last_checked': datetime.now().isoformat()
        }
    
    def _search_school_website(self, school_name: str, website: str) -> List[JobVacancy]:
        """Search school website for job vacancies"""
        
        vacancies = []
        
        # Common vacancy page patterns
        vacancy_patterns = [
            'vacancies', 'jobs', 'careers', 'recruitment',
            'work-with-us', 'join-our-team', 'current-vacancies'
        ]
        
        # Search for vacancy pages
        for pattern in vacancy_patterns[:3]:  # Limit searches
            query = f'site:{website} {pattern}'
            results = self.serper.search_web(query, num_results=5)
            
            for result in results:
                if self._is_vacancy_page(result):
                    # Analyze the page content
                    vacancy = self._extract_vacancy_from_result(
                        result, 
                        school_name,
                        'School Website'
                    )
                    if vacancy:
                        vacancies.append(vacancy)
        
        return vacancies
    
    def _search_job_boards(self, school_name: str) -> List[JobVacancy]:
        """Search major job boards for school vacancies"""
        
        vacancies = []
        
        # Search top job boards
        for domain, board_name in list(self.job_boards.items())[:3]:  # Limit to top 3
            query = f'"{school_name}" site:{domain}'
            results = self.serper.search_web(query, num_results=5)
            
            for result in results:
                # Check if it's a recent job posting
                if self._is_recent_job_posting(result):
                    vacancy = self._extract_vacancy_from_result(
                        result,
                        school_name,
                        board_name
                    )
                    if vacancy:
                        vacancies.append(vacancy)
        
        # Special search for TES (most popular education job board)
        tes_query = f'"{school_name}" site:tes.com/jobs posted:"last 30 days"'
        tes_results = self.serper.search_web(tes_query, num_results=5)
        
        for result in tes_results:
            vacancy = self._extract_vacancy_from_result(
                result,
                school_name,
                'TES Jobs'
            )
            if vacancy:
                vacancies.append(vacancy)
        
        return vacancies
    
    def _extract_vacancy_from_result(self, search_result: Dict[str, Any], 
                                   school_name: str, source: str) -> Optional[JobVacancy]:
        """Extract vacancy information from search result"""
        
        title = search_result.get('title', '')
        snippet = search_result.get('snippet', '')
        url = search_result.get('url', '')
        
        # Use GPT to extract structured information
        prompt = f"""
        Extract job vacancy information from this search result:
        
        Title: {title}
        Snippet: {snippet}
        URL: {url}
        School: {school_name}
        
        Extract:
        1. Job title (cleaned)
        2. Posted date (if mentioned)
        3. Salary range (if mentioned)
        4. Contract type (permanent/temporary/maternity cover)
        5. Key requirements (list main ones)
        6. Is this a senior role? (yes/no)
        7. Any recruitment agency mentioned? (name or none)
        
        Return as JSON:
        {{
            "job_title": "string",
            "posted_date": "string or null",
            "salary_range": "string or null",
            "contract_type": "string",
            "key_requirements": ["list"],
            "is_senior": boolean,
            "agency_mentioned": "string or null"
        }}
        
        If this doesn't appear to be a job posting, return {{"is_job": false}}
        """
        
        try:
            response = self.openai.chat.completions.create(
                model="gpt-3.5-turbo",  # Use cheaper model for extraction
                messages=[
                    {"role": "system", "content": "Extract job information from text"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300,
                response_format={"type": "json_object"}
            )
            
            data = eval(response.choices[0].message.content)
            
            if data.get('is_job') == False:
                return None
            
            # Calculate urgency score
            urgency = 0.5
            if data.get('is_senior'):
                urgency += 0.3
            
            # Recent posting
            if data.get('posted_date') and 'day' in str(data['posted_date']).lower():
                urgency += 0.2
            
            return JobVacancy(
                title=data.get('job_title', title),
                school_name=school_name,
                posted_date=self._parse_date(data.get('posted_date')),
                salary_range=data.get('salary_range'),
                contract_type=data.get('contract_type', 'permanent'),
                key_requirements=data.get('key_requirements', []),
                source=source,
                url=url,
                urgency_score=min(urgency, 1.0),
                competitor_mentioned=data.get('agency_mentioned')
            )
            
        except Exception as e:
            logger.debug(f"Failed to extract vacancy: {e}")
            
            # Fallback: Basic extraction
            is_senior = any(role in title.lower() or role in snippet.lower() 
                           for role in self.senior_roles)
            
            return JobVacancy(
                title=title,
                school_name=school_name,
                posted_date=None,
                salary_range=self._extract_salary(snippet),
                contract_type='permanent',
                key_requirements=[],
                source=source,
                url=url,
                urgency_score=0.7 if is_senior else 0.4
            )
    
    def _is_vacancy_page(self, result: Dict[str, Any]) -> bool:
        """Check if search result is likely a vacancy page"""
        
        title = result.get('title', '').lower()
        snippet = result.get('snippet', '').lower()
        url = result.get('url', '').lower()
        
        vacancy_indicators = [
            'vacancy', 'vacancies', 'job', 'career', 'recruitment',
            'position', 'opportunity', 'opening', 'hiring'
        ]
        
        return any(indicator in title or indicator in snippet or indicator in url 
                  for indicator in vacancy_indicators)
    
    def _is_recent_job_posting(self, result: Dict[str, Any]) -> bool:
        """Check if job posting is recent (within 30 days)"""
        
        snippet = result.get('snippet', '').lower()
        
        # Look for date indicators
        recent_patterns = [
            r'\d+ days? ago',
            r'posted today',
            r'posted yesterday',
            r'closing date.*\d{1,2}.*(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
            r'deadline.*\d{1,2}.*(?:january|february|march|april|may|june|july|august|september|october|november|december)'
        ]
        
        for pattern in recent_patterns:
            if re.search(pattern, snippet, re.IGNORECASE):
                return True
        
        # Check for current year
        current_year = datetime.now().year
        if str(current_year) in snippet:
            return True
        
        return False
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse various date formats"""
        
        if not date_str:
            return None
        
        # Handle relative dates
        if 'today' in date_str.lower():
            return datetime.now()
        elif 'yesterday' in date_str.lower():
            return datetime.now() - timedelta(days=1)
        elif match := re.search(r'(\d+) days? ago', date_str.lower()):
            days = int(match.group(1))
            return datetime.now() - timedelta(days=days)
        
        # Try standard parsing
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except:
            return None
    
    def _extract_salary(self, text: str) -> Optional[str]:
        """Extract salary information from text"""
        
        # UK salary patterns
        salary_patterns = [
            r'£[\d,]+\s*-\s*£[\d,]+',  # Range
            r'£[\d,]+\s*to\s*£[\d,]+',  # Range with 'to'
            r'£[\d,]+\s*(?:per\s*annum|pa|p\.a\.)',  # Single salary
            r'(?:MPS|UPS|Main Pay Scale|Upper Pay Scale)',  # Teacher scales
            r'(?:L\d+\s*-\s*L\d+)',  # Leadership scales
        ]
        
        for pattern in salary_patterns:
            if match := re.search(pattern, text, re.IGNORECASE):
                return match.group(0)
        
        return None
    
    def _deduplicate_vacancies(self, vacancies: List[JobVacancy]) -> List[JobVacancy]:
        """Remove duplicate vacancies based on title and school"""
        
        unique = {}
        
        for vacancy in vacancies:
            # Create a key based on cleaned title and school
            key = f"{vacancy.school_name}_{self._clean_job_title(vacancy.title)}"
            
            if key not in unique:
                unique[key] = vacancy
            else:
                # Keep the one with higher urgency score
                if vacancy.urgency_score > unique[key].urgency_score:
                    unique[key] = vacancy
        
        return list(unique.values())
    
    def _clean_job_title(self, title: str) -> str:
        """Clean and normalize job title"""
        
        # Remove common prefixes/suffixes
        title = re.sub(r'^(wanted:|required:|vacancy:|job:)', '', title, flags=re.IGNORECASE)
        title = re.sub(r'[-–—].*$', '', title)  # Remove everything after dash
        title = title.strip()
        
        return title.lower()
    
    def _analyze_vacancy_patterns(self, vacancies: List[JobVacancy]) -> Dict[str, Any]:
        """Analyze patterns in vacancies"""
        
        analysis = {
            'total_vacancies': len(vacancies),
            'senior_positions': 0,
            'subjects_needed': [],
            'contract_types': {},
            'competitors_active': [],
            'urgency_level': 'low',
            'recruitment_challenges': []
        }
        
        subjects = []
        
        for vacancy in vacancies:
            # Count senior positions
            if vacancy.urgency_score > 0.7:
                analysis['senior_positions'] += 1
            
            # Track contract types
            contract = vacancy.contract_type
            analysis['contract_types'][contract] = analysis['contract_types'].get(contract, 0) + 1
            
            # Track competitors
            if vacancy.competitor_mentioned:
                if vacancy.competitor_mentioned not in analysis['competitors_active']:
                    analysis['competitors_active'].append(vacancy.competitor_mentioned)
            
            # Extract subjects from title
            subject_patterns = [
                r'(english|maths|mathematics|science|history|geography|computing|art|music|pe|physical education|mfl|languages)',
                r'(physics|chemistry|biology|french|spanish|german)',
                r'(business|economics|psychology|sociology)',
                r'(eyfs|early years|ks1|ks2|key stage)'
            ]
            
            for pattern in subject_patterns:
                if match := re.search(pattern, vacancy.title, re.IGNORECASE):
                    subjects.append(match.group(1).title())
        
        # Deduplicate subjects
        analysis['subjects_needed'] = list(set(subjects))
        
        # Determine urgency level
        if analysis['senior_positions'] >= 2:
            analysis['urgency_level'] = 'high'
        elif analysis['senior_positions'] >= 1 or len(vacancies) >= 3:
            analysis['urgency_level'] = 'medium'
        
        # Identify recruitment challenges
        if len(vacancies) >= 5:
            analysis['recruitment_challenges'].append('High volume of vacancies suggests retention issues')
        
        if analysis['contract_types'].get('maternity cover', 0) >= 2:
            analysis['recruitment_challenges'].append('Multiple maternity covers needed')
        
        if 'temporary' in analysis['contract_types'] and analysis['contract_types']['temporary'] >= 2:
            analysis['recruitment_challenges'].append('Reliance on temporary staff')
        
        return analysis
    
    def _generate_vacancy_conversations(self, vacancies: List[JobVacancy], 
                                      analysis: Dict[str, Any]) -> List[ConversationStarter]:
        """Generate conversation starters based on vacancies"""
        
        starters = []
        
        # High urgency - senior positions
        senior_vacancies = [v for v in vacancies if v.urgency_score > 0.7]
        if senior_vacancies:
            vacancy = senior_vacancies[0]
            starter = ConversationStarter(
                topic=f"Urgent: {vacancy.title} Vacancy",
                detail=f"I see you're recruiting for a {vacancy.title} - this is a critical position. "
                       f"Protocol Education has an exceptional track record with senior placements. "
                       f"We have pre-vetted candidates who could start immediately. "
                       f"Given the importance of this role, shall we prioritize finding you the perfect match?",
                source_url=vacancy.url,
                relevance_score=0.95,
                date=datetime.now()
            )
            starters.append(starter)
        
        # Multiple vacancies pattern
        if len(vacancies) >= 3:
            starter = ConversationStarter(
                topic="Supporting Your Recruitment Drive",
                detail=f"I noticed you have {len(vacancies)} open positions currently. "
                       f"Managing multiple recruitments can be overwhelming. "
                       f"Protocol Education can handle your entire recruitment campaign - "
                       f"from advertising to vetting to final placement. "
                       f"We recently helped a similar school fill 5 positions in just 3 weeks.",
                source_url=vacancies[0].url if vacancies else '',
                relevance_score=0.9,
                date=datetime.now()
            )
            starters.append(starter)
        
        # Subject-specific needs
        if analysis['subjects_needed']:
            subjects = ', '.join(analysis['subjects_needed'][:3])
            starter = ConversationStarter(
                topic=f"{subjects} Teacher Recruitment",
                detail=f"Your current vacancies in {subjects} align perfectly with our candidate pool. "
                       f"We've noticed these subjects are particularly challenging to recruit for nationally, "
                       f"but Protocol Education has developed specialist networks in these areas. "
                       f"Would you like to see profiles of available {analysis['subjects_needed'][0]} teachers?",
                source_url='',
                relevance_score=0.85,
                date=datetime.now()
            )
            starters.append(starter)
        
        # Competitor activity
        if analysis['competitors_active']:
            starter = ConversationStarter(
                topic="Alternative Recruitment Partner",
                detail=f"I see you're working with {analysis['competitors_active'][0]} for some positions. "
                       f"While they're a good agency, Protocol Education offers more competitive rates "
                       f"and a quality guarantee that many schools find provides better value. "
                       f"Would you consider trialing us for one of your current vacancies to compare?",
                source_url='',
                relevance_score=0.8,
                date=datetime.now()
            )
            starters.append(starter)
        
        # Temporary/supply needs
        temp_count = analysis['contract_types'].get('temporary', 0) + analysis['contract_types'].get('maternity cover', 0)
        if temp_count >= 2:
            starter = ConversationStarter(
                topic="Flexible Staffing Solutions",
                detail=f"With {temp_count} temporary positions to fill, you need reliable cover fast. "
                       f"Protocol Education maintains a pool of immediately available supply teachers "
                       f"who can commit to longer-term placements. "
                       f"Our 48-hour replacement guarantee ensures you're never left without cover.",
                source_url='',
                relevance_score=0.85,
                date=datetime.now()
            )
            starters.append(starter)
        
        return starters


def integrate_vacancy_detector(processor):
    """
    Integration function to add vacancy detection to the processor
    """
    
    def detect_and_add_vacancies(intel, ai_engine):
        """Add vacancy detection to school intelligence"""
        
        try:
            # Initialize detector
            detector = VacancyDetector(
                ai_engine,
                ai_engine.openai_client
            )
            
            # Detect vacancies
            vacancy_data = detector.detect_vacancies(
                intel.school_name,
                intel.website
            )
            
            # Add vacancy-based conversation starters
            for starter in vacancy_data['conversation_starters']:
                intel.conversation_starters.append(starter)
            
            # Store vacancy data
            intel.vacancy_data = vacancy_data
            
            # Update competitive intelligence if competitors found in job ads
            for competitor in vacancy_data['analysis']['competitors_active']:
                # Check if we already have this competitor
                existing = next((c for c in intel.competitors 
                               if c.agency_name == competitor), None)
                
                if not existing:
                    from models import CompetitorPresence
                    intel.competitors.append(
                        CompetitorPresence(
                            agency_name=competitor,
                            presence_type='job_posting',
                            evidence_urls=[v.url for v in vacancy_data['vacancies'] 
                                         if v.competitor_mentioned == competitor][:2],
                            confidence_score=0.9,
                            weaknesses=['May not have exclusive arrangement']
                        )
                    )
            
            logger.info(f"Detected {vacancy_data['total_found']} vacancies for {intel.school_name}")
            
        except Exception as e:
            logger.error(f"Error detecting vacancies: {e}")
        
        return intel
    
    return detect_and_add_vacancies