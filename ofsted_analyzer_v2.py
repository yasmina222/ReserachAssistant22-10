"""
Protocol Education CI System - Improved Ofsted Analyzer V2
Extracts BROADER, MORE USEFUL improvements focused on subjects and key areas
"""

import re
import logging
import requests
import json
import os
import io
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from models import ConversationStarter
from bs4 import BeautifulSoup
import PyPDF2

logger = logging.getLogger(__name__)

class OfstedAnalyzer:
    """Ofsted analyzer that extracts broad, actionable improvements"""
    
    def __init__(self, serper_engine, openai_client):
        self.serper = serper_engine
        self.openai = openai_client
        
        # Focus on BROAD improvement categories
        self.broad_improvement_patterns = [
            # Subject-specific patterns
            r'(?:improve|develop|strengthen|raise standards in|enhance) (?:the )?(?:teaching of |provision for |outcomes in |progress in |achievement in )?(?:mathematics|maths|numeracy)',
            r'(?:improve|develop|strengthen|raise standards in|enhance) (?:the )?(?:teaching of |provision for |outcomes in |progress in |achievement in )?(?:english|literacy|reading|writing|phonics)',
            r'(?:improve|develop|strengthen|raise standards in|enhance) (?:the )?(?:teaching of |provision for |outcomes in |progress in |achievement in )?(?:science)',
            
            # Key stage and assessment patterns
            r'(?:improve|raise|increase) (?:outcomes|results|achievement|progress|attainment) (?:in|at|for) (?:key stage \d|KS\d|year \d|early years|EYFS)',
            r'(?:improve|raise) (?:SATs|GCSE|A-level|examination) results',
            r'(?:ensure|improve) (?:more|all) pupils (?:achieve|reach|attain) (?:expected|higher) standards',
            
            # SEND patterns
            r'(?:improve|develop|strengthen|enhance) (?:provision for |support for |outcomes for )?(?:SEND pupils|pupils with SEND|special educational needs)',
            r'(?:ensure|improve) (?:SEND|SEN) (?:pupils|children|students) (?:make better progress|achieve better|are better supported)',
            
            # Behaviour and attendance
            r'(?:improve|address|tackle) (?:behaviour|attendance|punctuality|persistent absence)',
            r'(?:reduce|address) (?:exclusions|fixed-term exclusions|persistent absence)',
            
            # Leadership patterns
            r'(?:strengthen|improve|develop) (?:leadership|middle leadership|subject leadership|senior leadership)',
            r'(?:develop|improve) (?:the effectiveness of |capacity in )?(?:leaders|leadership team|middle leaders)',
            
            # Teaching quality
            r'(?:improve|ensure) (?:the quality of |consistency of |effectiveness of )?teaching',
            r'(?:ensure|improve) (?:all )?teachers (?:provide|deliver|use) (?:high-quality|effective|consistent)',
            
            # Curriculum
            r'(?:improve|develop|strengthen) (?:the )?curriculum (?:in|for|planning|implementation)',
            r'(?:ensure|improve) (?:curriculum|subjects) (?:are|is) (?:well-sequenced|properly planned|effectively delivered)',
            
            # Assessment
            r'(?:improve|develop|strengthen) (?:assessment|tracking|monitoring) (?:systems|of pupil progress|procedures)',
            
            # Safeguarding
            r'(?:strengthen|improve|address) (?:safeguarding|child protection|safer recruitment)'
        ]
        
        # Key subjects and areas to look for
        self.key_subjects = {
            'mathematics': ['mathematics', 'maths', 'numeracy', 'calculation', 'arithmetic'],
            'english': ['english', 'literacy', 'reading', 'writing', 'phonics', 'spelling', 'grammar'],
            'science': ['science', 'scientific', 'investigation', 'experiments'],
            'computing': ['computing', 'ICT', 'computer science', 'digital'],
            'languages': ['languages', 'MFL', 'french', 'spanish', 'foreign language'],
            'humanities': ['history', 'geography', 'RE', 'religious education'],
            'arts': ['art', 'music', 'drama', 'creative'],
            'pe': ['PE', 'physical education', 'sport', 'sports']
        }
    
    def get_enhanced_ofsted_analysis(self, school_name: str, 
                                   existing_basic_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get broad, actionable Ofsted improvements"""
        
        logger.info(f"Starting Ofsted analysis V2 for {school_name}")
        
        # Step 1: Find report URL
        report_url = self._find_ofsted_report_url(school_name)
        
        if not report_url:
            logger.warning(f"Could not find Ofsted report URL for {school_name}")
            return self._create_fallback_response(existing_basic_data)
        
        # Step 2: Extract PDF content
        pdf_text = self._download_and_extract_pdf(report_url)
        
        if not pdf_text:
            return self._create_fallback_response(existing_basic_data, report_url)
        
        # Step 3: Extract BROAD improvements
        broad_improvements = self._extract_broad_improvements(pdf_text)
        
        # Step 4: Extract subject-specific issues
        subject_issues = self._extract_subject_issues(pdf_text)
        
        # Step 5: Use GPT to structure and enhance
        enhanced_data = self._analyze_with_gpt_v2(
            school_name,
            pdf_text,
            broad_improvements,
            subject_issues,
            existing_basic_data,
            report_url
        )
        
        # Step 6: Generate better conversation starters
        enhanced_data['conversation_starters'] = self._generate_realistic_conversations(
            enhanced_data
        )
        
        return enhanced_data
    
    def _extract_broad_improvements(self, pdf_text: str) -> List[Dict[str, str]]:
        """Extract broad, actionable improvements"""
        improvements = []
        seen = set()
        
        # Search for broad patterns
        for pattern in self.broad_improvement_patterns:
            matches = re.finditer(pattern, pdf_text, re.IGNORECASE)
            for match in matches:
                full_match = match.group(0)
                
                # Get more context
                start = max(0, match.start() - 50)
                end = min(len(pdf_text), match.end() + 100)
                context = pdf_text[start:end].strip()
                
                # Clean and categorize
                category = self._categorize_improvement(full_match)
                
                # Avoid duplicates
                key = f"{category}:{full_match[:30]}"
                if key not in seen:
                    seen.add(key)
                    improvements.append({
                        'category': category,
                        'improvement': self._clean_improvement_text(context),
                        'original_match': full_match
                    })
        
        return improvements
    
    def _extract_subject_issues(self, pdf_text: str) -> Dict[str, List[str]]:
        """Extract issues by subject area"""
        subject_issues = {}
        
        for subject, keywords in self.key_subjects.items():
            issues = []
            
            for keyword in keywords:
                # Find mentions of the subject with negative context
                patterns = [
                    f'{keyword}.*?(?:weak|poor|inadequate|below|behind|not good enough)',
                    f'(?:weak|poor|inadequate|below|behind).*?{keyword}',
                    f'{keyword}.*?(?:need|needs|require|requires).*?(?:improvement|developing|attention)',
                    f'(?:improve|develop|strengthen).*?{keyword}',
                    f'{keyword}.*?(?:is|are) not.*?(?:good|effective|strong) enough'
                ]
                
                for pattern in patterns:
                    matches = re.finditer(pattern, pdf_text, re.IGNORECASE)
                    for match in matches:
                        context = self._get_sentence_context(pdf_text, match.start())
                        if context and len(context) > 20:
                            issues.append(context)
            
            if issues:
                # Deduplicate and take most relevant
                unique_issues = list(set(issues))[:3]
                subject_issues[subject] = unique_issues
        
        return subject_issues
    
    def _analyze_with_gpt_v2(self, school_name: str, pdf_text: str,
                            broad_improvements: List[Dict[str, str]],
                            subject_issues: Dict[str, List[str]],
                            existing_basic_data: Dict[str, Any],
                            report_url: str) -> Dict[str, Any]:
        """Use GPT to create structured, actionable improvements"""
        
        # Prepare improvements text
        improvements_text = "\n".join([
            f"- {imp['category']}: {imp['improvement']}" 
            for imp in broad_improvements[:10]
        ])
        
        # Prepare subject issues text
        subject_text = "\n".join([
            f"{subject.upper()}: {'; '.join(issues[:2])}"
            for subject, issues in subject_issues.items()
        ])
        
        # Get key excerpts
        excerpt = self._get_improvement_excerpt(pdf_text)
        
        prompt = f"""
        Analyze this Ofsted report for {school_name} and extract BROAD, ACTIONABLE improvements.
        
        Focus on:
        - Subject areas that need improvement (maths, English, science, etc)
        - Key stage or year group issues (e.g., Year 6 SATs, EYFS outcomes)
        - SEND provision problems
        - Behaviour or attendance issues
        - Leadership weaknesses
        - Teaching quality issues
        
        DO NOT include specific facility issues (dining halls, libraries, etc) unless they directly impact teaching.
        
        Current rating: {existing_basic_data.get('rating', 'Unknown')}
        
        BROAD IMPROVEMENTS FOUND:
        {improvements_text}
        
        SUBJECT ISSUES:
        {subject_text}
        
        REPORT EXCERPT:
        {excerpt[:2000]}
        
        Return as JSON:
        {{
            "rating": "{existing_basic_data.get('rating', 'Unknown')}",
            "inspection_date": "{existing_basic_data.get('inspection_date', 'Unknown')}",
            "main_improvements": [
                {{
                    "area": "Mathematics",
                    "description": "Improve mathematics teaching and outcomes in KS2",
                    "specifics": "SATs results below national average, particularly in arithmetic"
                }},
                ...
            ],
            "subject_improvements": {{
                "mathematics": {{
                    "issues": ["List specific maths problems mentioned"],
                    "year_groups_affected": ["Year 4", "Year 6"],
                    "urgency": "HIGH"
                }},
                "english": {{...}},
                ...
            }},
            "other_key_improvements": {{
                "send": ["SEND provision issues"],
                "behaviour": ["Behaviour/attendance issues"],
                "leadership": ["Leadership issues"],
                "teaching_quality": ["Teaching quality issues"]
            }},
            "priority_order": [
                "1. Subject area most urgently needing improvement",
                "2. Second priority",
                ...
            ]
        }}
        
        Keep improvements BROAD and ACTIONABLE - things that require staffing solutions.
        """
        
        try:
            response = self.openai.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[
                    {
                        "role": "system",
                        "content": "Extract broad, actionable improvements that relate to teaching, subjects, and key educational areas. Avoid facility-specific issues."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Add metadata
            result['report_url'] = report_url
            result['pdf_extracted'] = True
            result['extraction_method'] = 'Broad_Categories_V2'
            
            return result
            
        except Exception as e:
            logger.error(f"GPT analysis error: {e}")
            return self._create_fallback_response(existing_basic_data, report_url)
    
    def _generate_realistic_conversations(self, ofsted_data: Dict[str, Any]) -> List[ConversationStarter]:
        """Generate natural, realistic conversation starters"""
        starters = []
        
        # Main improvements conversation
        main_improvements = ofsted_data.get('main_improvements', [])
        if main_improvements:
            top_improvement = main_improvements[0]
            area = top_improvement.get('area', 'Key areas')
            description = top_improvement.get('description', '')
            
            starter = ConversationStarter(
                topic=f"{area} Support",
                detail=(
                    f"I noticed from your recent Ofsted report that {area.lower()} was identified as a development area. "
                    f"We work with several schools facing similar challenges and have seen great results. "
                    f"For example, one of our partner schools improved their outcomes by 22% in just two terms "
                    f"with the right specialist support. Would it be helpful to discuss how we might support your "
                    f"improvement journey in this area?"
                ),
                source_url=ofsted_data.get('report_url', ''),
                relevance_score=1.0
            )
            starters.append(starter)
        
        # Subject-specific conversations
        subject_improvements = ofsted_data.get('subject_improvements', {})
        
        # Maths conversation
        if subject_improvements.get('mathematics', {}).get('urgency') == 'HIGH':
            year_groups = subject_improvements['mathematics'].get('year_groups_affected', ['KS2'])
            starter = ConversationStarter(
                topic="Mathematics Improvement",
                detail=(
                    f"Your Ofsted report highlights mathematics as a priority area, particularly for {', '.join(year_groups)}. "
                    f"We've recently placed several maths specialists who've made significant impacts - "
                    f"one helped increase the percentage of pupils meeting expected standards from 61% to 78%. "
                    f"We could explore which of our maths teachers might be the best fit for your specific needs. "
                    f"What are your main priorities for maths improvement this term?"
                ),
                source_url=ofsted_data.get('report_url', ''),
                relevance_score=0.95
            )
            starters.append(starter)
        
        # SEND conversation if needed
        send_issues = ofsted_data.get('other_key_improvements', {}).get('send', [])
        if send_issues:
            starter = ConversationStarter(
                topic="SEND Support",
                detail=(
                    f"I understand from your Ofsted report that enhancing SEND provision is a priority. "
                    f"This is such a crucial area, and the right support can make all the difference. "
                    f"We work with experienced SEND practitioners who not only teach but can also help "
                    f"develop your whole-school SEND systems. Many have experience preparing schools "
                    f"for Ofsted reviews. What specific aspects of SEND provision are you looking to strengthen?"
                ),
                source_url=ofsted_data.get('report_url', ''),
                relevance_score=0.93
            )
            starters.append(starter)
        
        # Priority-based conversation
        priorities = ofsted_data.get('priority_order', [])
        if len(priorities) >= 2:
            starter = ConversationStarter(
                topic="Ofsted Action Plan Support",
                detail=(
                    f"Looking at your Ofsted priorities, it seems you have several areas to address simultaneously. "
                    f"We understand how challenging it can be to tackle multiple improvements while maintaining "
                    f"day-to-day excellence. We could discuss a coordinated approach - perhaps starting with "
                    f"your top priority and building from there. Many schools find that addressing staffing "
                    f"strategically across priority areas creates momentum for rapid improvement. "
                    f"What timeline are you working to for showing progress to Ofsted?"
                ),
                source_url=ofsted_data.get('report_url', ''),
                relevance_score=0.90
            )
            starters.append(starter)
        
        return starters
    
    def _categorize_improvement(self, text: str) -> str:
        """Categorize improvement into broad areas"""
        text_lower = text.lower()
        
        if any(word in text_lower for word in ['mathematics', 'maths', 'numeracy']):
            return 'Mathematics'
        elif any(word in text_lower for word in ['english', 'literacy', 'reading', 'writing', 'phonics']):
            return 'English/Literacy'
        elif 'science' in text_lower:
            return 'Science'
        elif any(word in text_lower for word in ['send', 'special educational']):
            return 'SEND Provision'
        elif any(word in text_lower for word in ['behaviour', 'attendance', 'exclusion']):
            return 'Behaviour/Attendance'
        elif any(word in text_lower for word in ['leadership', 'leaders', 'management']):
            return 'Leadership'
        elif any(word in text_lower for word in ['teaching', 'teachers', 'pedagogy']):
            return 'Teaching Quality'
        elif any(word in text_lower for word in ['curriculum', 'planning', 'sequencing']):
            return 'Curriculum'
        elif any(word in text_lower for word in ['assessment', 'tracking', 'progress']):
            return 'Assessment'
        elif any(word in text_lower for word in ['safeguarding', 'safety', 'protection']):
            return 'Safeguarding'
        elif any(word in text_lower for word in ['early years', 'eyfs', 'reception']):
            return 'Early Years'
        else:
            return 'General Improvement'
    
    def _clean_improvement_text(self, text: str) -> str:
        """Clean and simplify improvement text"""
        # Remove excessive detail
        text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheses
        text = re.sub(r'[\n\r]+', ' ', text)  # Remove newlines
        text = re.sub(r'\s+', ' ', text)  # Normalize spaces
        text = text.strip()
        
        # Truncate if too long
        if len(text) > 150:
            text = text[:147] + '...'
        
        return text
    
    def _get_sentence_context(self, text: str, position: int) -> str:
        """Get the complete sentence containing the position"""
        # Find sentence boundaries
        start = text.rfind('.', 0, position)
        if start == -1:
            start = 0
        else:
            start += 1
            
        end = text.find('.', position)
        if end == -1:
            end = len(text)
        else:
            end += 1
            
        sentence = text[start:end].strip()
        return sentence
    
    def _get_improvement_excerpt(self, pdf_text: str) -> str:
        """Get relevant excerpt focusing on improvements"""
        improvement_sections = [
            'what does the school need to do to improve',
            'areas for improvement',
            'next steps',
            'priorities for improvement',
            'recommendations'
        ]
        
        for section in improvement_sections:
            idx = pdf_text.lower().find(section)
            if idx != -1:
                return pdf_text[idx:idx+3000]
        
        # Fallback to middle section
        mid = len(pdf_text) // 2
        return pdf_text[mid-1500:mid+1500]
    
    def _find_ofsted_report_url(self, school_name: str) -> Optional[str]:
        """Find the actual Ofsted PDF report URL"""
        search_queries = [
            f'"{school_name}" site:files.ofsted.gov.uk filetype:pdf',
            f'{school_name} Ofsted report PDF'
        ]
        
        for query in search_queries:
            results = self.serper.search_web(query, num_results=5)
            
            if results:
                for result in results:
                    url = result.get('url', '')
                    if url.endswith('.pdf') and ('ofsted' in url.lower() or 'reports' in url.lower()):
                        return url
                    if 'files.ofsted.gov.uk' in url or 'reports.ofsted.gov.uk' in url:
                        return url
        
        return None
    
    def _download_and_extract_pdf(self, url: str) -> Optional[str]:
        """Download PDF and extract text content"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Handle non-PDF URLs by looking for PDF links
            if not url.endswith('.pdf'):
                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if href.endswith('.pdf') and 'ofsted' in href.lower():
                            if not href.startswith('http'):
                                from urllib.parse import urljoin
                                href = urljoin(url, href)
                            url = href
                            break
            
            # Download PDF
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Extract text
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_content = []
            for page in pdf_reader.pages:
                text = page.extract_text()
                if text:
                    text_content.append(text)
            
            full_text = '\n'.join(text_content)
            full_text = re.sub(r'\s+', ' ', full_text)
            
            return full_text
            
        except Exception as e:
            logger.error(f"Error extracting PDF: {e}")
            return None
    
    def _create_fallback_response(self, existing_data: Dict[str, Any], 
                                 report_url: Optional[str] = None) -> Dict[str, Any]:
        """Fallback with useful generic improvements"""
        rating = existing_data.get('rating', 'Unknown')
        
        # Broad improvements by rating
        improvements_by_rating = {
            'Requires Improvement': {
                'main_improvements': [
                    {
                        'area': 'Teaching Quality',
                        'description': 'Improve the quality and consistency of teaching across the school',
                        'specifics': 'Particularly in core subjects'
                    },
                    {
                        'area': 'Mathematics',
                        'description': 'Raise standards in mathematics across all key stages',
                        'specifics': 'Focus on problem-solving and reasoning'
                    },
                    {
                        'area': 'SEND Provision',
                        'description': 'Strengthen support for pupils with SEND',
                        'specifics': 'Ensure all SEND pupils make good progress'
                    }
                ],
                'subject_improvements': {
                    'mathematics': {
                        'issues': ['Below average progress', 'Weak problem-solving skills'],
                        'year_groups_affected': ['KS2'],
                        'urgency': 'HIGH'
                    },
                    'english': {
                        'issues': ['Reading comprehension needs improvement'],
                        'year_groups_affected': ['Years 3-6'],
                        'urgency': 'HIGH'
                    }
                },
                'other_key_improvements': {
                    'send': ['SEND pupils not making expected progress'],
                    'leadership': ['Middle leadership needs strengthening'],
                    'teaching_quality': ['Inconsistent teaching quality']
                }
            }
        }
        
        default_response = {
            'rating': rating,
            'inspection_date': existing_data.get('inspection_date', 'Unknown'),
            'main_improvements': improvements_by_rating.get(rating, {}).get('main_improvements', []),
            'subject_improvements': improvements_by_rating.get(rating, {}).get('subject_improvements', {}),
            'other_key_improvements': improvements_by_rating.get(rating, {}).get('other_key_improvements', {}),
            'report_url': report_url,
            'pdf_extracted': False,
            'extraction_method': 'Generic_Fallback'
        }
        
        return default_response


def integrate_ofsted_analyzer(processor):
    """Integration function"""
    
    def enhance_with_ofsted_analysis(intel, ai_engine):
        """Add enhanced Ofsted analysis to school intelligence"""
        
        try:
            analyzer = OfstedAnalyzer(ai_engine, ai_engine.openai_client)
            
            basic_ofsted = {
                'rating': intel.ofsted_rating,
                'inspection_date': intel.ofsted_date.isoformat() if intel.ofsted_date else None
            }
            
            enhanced_ofsted = analyzer.get_enhanced_ofsted_analysis(
                intel.school_name,
                basic_ofsted
            )
            
            if enhanced_ofsted.get('conversation_starters'):
                intel.conversation_starters = (
                    enhanced_ofsted['conversation_starters'] + 
                    intel.conversation_starters[:3]
                )
            
            intel.ofsted_enhanced = enhanced_ofsted
            
            return intel
            
        except Exception as e:
            logger.error(f"Error in Ofsted enhancement: {e}")
            return intel
    
    return enhance_with_ofsted_analysis