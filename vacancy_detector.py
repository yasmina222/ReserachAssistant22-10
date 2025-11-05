"""
Protocol Education CI System - Vacancy Detector (ASYNC VERSION)
PHASE 1: Async version with parallel job board searches
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime

# Import models and core logic from original
from vacancy_detector import (
    JobVacancy,
    VacancyDetector,
    ConversationStarter
)

logger = logging.getLogger(__name__)

class VacancyDetectorAsync:
    """Async version of vacancy detector"""
    
    def __init__(self, serper_engine, openai_client):
        self.serper = serper_engine
        self.openai = openai_client
        
        # Create sync detector to reuse its helper methods
        self.sync_detector = VacancyDetector(serper_engine, openai_client)
        
        self.senior_roles = self.sync_detector.senior_roles
        self.job_boards = self.sync_detector.job_boards
    
    async def detect_vacancies(self, school_name: str, website: Optional[str] = None) -> Dict:
        """Detect job vacancies - ASYNC with parallel searches"""
        
        logger.info(f"Detecting vacancies for {school_name}")
        
        # Run website and job board searches in parallel
        tasks = []
        
        if website:
            tasks.append(self._search_school_website(school_name, website))
        
        tasks.append(self._search_job_boards(school_name))
        
        # Execute all searches in parallel
        results = await asyncio.gather(*tasks)
        
        # Flatten results
        vacancies = []
        for result in results:
            if result:
                vacancies.extend(result)
        
        # Reuse sync methods for analysis (CPU-bound, fast)
        unique_vacancies = self.sync_detector._deduplicate_vacancies(vacancies)
        analysis = self.sync_detector._analyze_vacancy_patterns(unique_vacancies)
        conversation_starters = self.sync_detector._generate_vacancy_conversations(
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
    
    async def _search_school_website(self, school_name: str, website: str) -> List[JobVacancy]:
        """Search school website for vacancies - ASYNC"""
        
        vacancies = []
        vacancy_patterns = ['vacancies', 'jobs', 'careers']
        
        # Run all pattern searches in parallel
        search_tasks = [
            self.serper.search_web(f'site:{website} {pattern}', num_results=5)
            for pattern in vacancy_patterns[:3]
        ]
        
        search_results = await asyncio.gather(*search_tasks)
        
        for results in search_results:
            for result in results:
                if self.sync_detector._is_vacancy_page(result):
                    vacancy = await self._extract_vacancy_from_result(
                        result, 
                        school_name,
                        'School Website'
                    )
                    if vacancy:
                        vacancies.append(vacancy)
        
        return vacancies
    
    async def _search_job_boards(self, school_name: str) -> List[JobVacancy]:
        """Search job boards - ASYNC with parallel searches"""
        
        vacancies = []
        
        # Search top job boards in parallel
        search_tasks = []
        for domain, board_name in list(self.sync_detector.job_boards.items())[:3]:
            query = f'"{school_name}" site:{domain}'
            search_tasks.append(
                (self.serper.search_web(query, num_results=5), board_name)
            )
        
        # Execute searches
        searches = [task[0] for task in search_tasks]
        board_names = [task[1] for task in search_tasks]
        
        search_results = await asyncio.gather(*searches)
        
        for results, board_name in zip(search_results, board_names):
            for result in results:
                if self.sync_detector._is_recent_job_posting(result):
                    vacancy = await self._extract_vacancy_from_result(
                        result,
                        school_name,
                        board_name
                    )
                    if vacancy:
                        vacancies.append(vacancy)
        
        return vacancies
    
    async def _extract_vacancy_from_result(self, search_result: Dict, 
                                   school_name: str, source: str) -> Optional[JobVacancy]:
        """Extract vacancy info - ASYNC for GPT call"""
        
        title = search_result.get('title', '')
        snippet = search_result.get('snippet', '')
        url = search_result.get('url', '')
        
        # Quick check if it's likely a vacancy
        vacancy_keywords = ['teacher', 'head', 'senco', 'vacancy', 'job']
        if not any(kw in title.lower() or kw in snippet.lower() for kw in vacancy_keywords):
            return None
        
        prompt = f"""
        Extract job vacancy info from:
        Title: {title}
        Snippet: {snippet}
        School: {school_name}
        
        Return JSON:
        {{
            "job_title": "string",
            "posted_date": "string or null",
            "salary_range": "string or null",
            "contract_type": "permanent/temporary/maternity",
            "key_requirements": ["list"],
            "is_senior": boolean,
            "agency_mentioned": "string or null",
            "is_job": boolean
        }}
        """
        
        try:
            response = await self.openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Extract job information"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300,
                response_format={"type": "json_object"}
            )
            
            import json
            data = json.loads(response.choices[0].message.content)
            
            if not data.get('is_job', True):
                return None
            
            urgency = 0.5
            if data.get('is_senior'):
                urgency += 0.3
            if data.get('posted_date') and 'day' in str(data['posted_date']).lower():
                urgency += 0.2
            
            return JobVacancy(
                title=data.get('job_title', title),
                school_name=school_name,
                posted_date=self.sync_detector._parse_date(data.get('posted_date')),
                salary_range=data.get('salary_range'),
                contract_type=data.get('contract_type', 'permanent'),
                key_requirements=data.get('key_requirements', []),
                source=source,
                url=url,
                urgency_score=min(urgency, 1.0),
                competitor_mentioned=data.get('agency_mentioned')
            )
            
        except Exception as e:
            logger.debug(f"GPT extraction failed: {e}")
            
            # Fallback
            is_senior = any(role in title.lower() or role in snippet.lower() 
                           for role in self.senior_roles)
            
            return JobVacancy(
                title=title,
                school_name=school_name,
                posted_date=None,
                salary_range=self.sync_detector._extract_salary(snippet),
                contract_type='permanent',
                key_requirements=[],
                source=source,
                url=url,
                urgency_score=0.7 if is_senior else 0.4
            )


def integrate_vacancy_detector_async(processor):
    """Async integration function"""
    
    async def detect_and_add_vacancies(intel, ai_engine):
        """Add vacancy detection - ASYNC"""
        try:
            detector = VacancyDetectorAsync(
                ai_engine,
                ai_engine.openai_client
            )
            
            vacancy_data = await detector.detect_vacancies(
                intel.school_name,
                intel.website
            )
            
            for starter in vacancy_data['conversation_starters']:
                intel.conversation_starters.append(starter)
            
            intel.vacancy_data = vacancy_data
            
            # Add competitors from vacancies
            for competitor in vacancy_data['analysis']['competitors_active']:
                existing = next((c for c in intel.competitors 
                               if c.agency_name == competitor), None)
                
                if not existing:
                    from models import CompetitorPresence
                    intel.competitors.append(
                        CompetitorPresence(
                            agency_name=competitor,
                            presence_type='job_posting',
                            evidence_urls=[v.url for v in vacancy_data['vacancies'] 
                                         if hasattr(v, 'competitor_mentioned') and 
                                         v.competitor_mentioned == competitor][:2],
                            confidence_score=0.9,
                            weaknesses=['May not have exclusive arrangement']
                        )
                    )
            
        except Exception as e:
            logger.error(f"Vacancy detection error: {e}")
        
        return intel
    
    return detect_and_add_vacancies
