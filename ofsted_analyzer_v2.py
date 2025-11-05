"""
Protocol Education CI System - Ofsted Analyzer (ASYNC VERSION)
PHASE 1: Async wrappers around core Ofsted analysis logic
"""

import asyncio
import aiohttp
import io
import logging
from typing import Dict, Optional, Any
from datetime import datetime
import PyPDF2

# Import most logic from the original synchronous version
from ofsted_analyzer_v2 import (
    OfstedAnalyzer,
    ConversationStarter
)

logger = logging.getLogger(__name__)

class OfstedAnalyzerAsync:
    """Async version of Ofsted analyzer"""
    
    def __init__(self, serper_engine, openai_client):
        self.serper = serper_engine
        self.openai = openai_client
        
        # Reuse patterns from original
        self.broad_improvement_patterns = [
            r'(?:improve|develop|strengthen|raise standards in|enhance) (?:the )?(?:teaching of |provision for |outcomes in |progress in |achievement in )?(?:mathematics|maths|numeracy)',
            r'(?:improve|develop|strengthen|raise standards in|enhance) (?:the )?(?:teaching of |provision for |outcomes in |progress in |achievement in )?(?:english|literacy|reading|writing|phonics)',
            r'(?:improve|develop|strengthen|raise standards in|enhance) (?:the )?(?:teaching of |provision for |outcomes in |progress in |achievement in )?(?:science)',
        ]
        
        self.key_subjects = {
            'mathematics': ['mathematics', 'maths', 'numeracy'],
            'english': ['english', 'literacy', 'reading', 'writing', 'phonics'],
            'science': ['science'],
        }
        
        # Create sync analyzer for reusing its methods
        self.sync_analyzer = OfstedAnalyzer(serper_engine, openai_client)
    
    async def get_enhanced_ofsted_analysis(self, school_name: str, 
                                   existing_basic_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get broad, actionable Ofsted improvements - ASYNC"""
        
        logger.info(f"Starting async Ofsted analysis for {school_name}")
        
        # Find report URL
        report_url = await self._find_ofsted_report_url(school_name)
        
        if not report_url:
            return self.sync_analyzer._create_fallback_response(existing_basic_data)
        
        # Download and extract PDF
        pdf_text = await self._download_and_extract_pdf(report_url)
        
        if not pdf_text:
            return self.sync_analyzer._create_fallback_response(existing_basic_data, report_url)
        
        # Reuse sync methods for pattern extraction (CPU-bound, fast enough)
        broad_improvements = self.sync_analyzer._extract_broad_improvements(pdf_text)
        subject_issues = self.sync_analyzer._extract_subject_issues(pdf_text)
        
        # Async GPT analysis
        enhanced_data = await self._analyze_with_gpt_v2(
            school_name,
            pdf_text,
            broad_improvements,
            subject_issues,
            existing_basic_data,
            report_url
        )
        
        # Reuse sync method for conversation generation
        enhanced_data['conversation_starters'] = self.sync_analyzer._generate_realistic_conversations(
            enhanced_data
        )
        
        return enhanced_data
    
    async def _find_ofsted_report_url(self, school_name: str) -> Optional[str]:
        """Find Ofsted PDF URL - ASYNC"""
        search_queries = [
            f'"{school_name}" site:files.ofsted.gov.uk filetype:pdf',
            f'{school_name} Ofsted report PDF'
        ]
        
        for query in search_queries:
            results = await self.serper.search_web(query, num_results=5)
            
            if results:
                for result in results:
                    url = result.get('url', '')
                    if url.endswith('.pdf') and ('ofsted' in url.lower() or 'reports' in url.lower()):
                        return url
                    if 'files.ofsted.gov.uk' in url or 'reports.ofsted.gov.uk' in url:
                        return url
        
        return None
    
    async def _download_and_extract_pdf(self, url: str) -> Optional[str]:
        """Download PDF and extract text - ASYNC"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        pdf_content = await response.read()
                        
                        # PDF extraction is CPU-bound, run in thread pool
                        text_content = await asyncio.to_thread(self._extract_pdf_text, pdf_content)
                        return text_content
                    else:
                        logger.error(f"PDF download failed with status {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error downloading PDF: {e}")
            return None
    
    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes (runs in thread pool)"""
        try:
            pdf_file = io.BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_content = []
            for page in pdf_reader.pages:
                text = page.extract_text()
                if text:
                    text_content.append(text)
            
            import re
            full_text = '\n'.join(text_content)
            full_text = re.sub(r'\s+', ' ', full_text)
            
            return full_text
        except Exception as e:
            logger.error(f"PDF text extraction error: {e}")
            return ""
    
    async def _analyze_with_gpt_v2(self, school_name: str, pdf_text: str,
                            broad_improvements: list,
                            subject_issues: dict,
                            existing_basic_data: dict,
                            report_url: str) -> Dict[str, Any]:
        """GPT analysis - ASYNC"""
        
        improvements_text = "\n".join([
            f"- {imp['category']}: {imp['improvement']}" 
            for imp in broad_improvements[:10]
        ])
        
        subject_text = "\n".join([
            f"{subject.upper()}: {'; '.join(issues[:2])}"
            for subject, issues in subject_issues.items()
        ])
        
        excerpt = self.sync_analyzer._get_improvement_excerpt(pdf_text)
        
        prompt = f"""
        Analyze this Ofsted report for {school_name} and extract actionable improvements.
        
        Focus on subject areas, key stages, SEND, behaviour, leadership, teaching quality.
        
        Current rating: {existing_basic_data.get('rating', 'Unknown')}
        
        BROAD IMPROVEMENTS:
        {improvements_text}
        
        SUBJECT ISSUES:
        {subject_text}
        
        EXCERPT:
        {excerpt[:2000]}
        
        Return JSON with main_improvements, subject_improvements, other_key_improvements, priority_order.
        """
        
        try:
            response = await self.openai.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[
                    {"role": "system", "content": "Extract broad, actionable improvements."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            import json
            result = json.loads(response.choices[0].message.content)
            result['report_url'] = report_url
            result['pdf_extracted'] = True
            result['extraction_method'] = 'Async_Broad_Categories_V2'
            
            return result
            
        except Exception as e:
            logger.error(f"GPT analysis error: {e}")
            return self.sync_analyzer._create_fallback_response(existing_basic_data, report_url)


def integrate_ofsted_analyzer_async(processor):
    """Async integration function"""
    
    async def enhance_with_ofsted_analysis(intel, ai_engine):
        """Add enhanced Ofsted analysis - ASYNC"""
        try:
            analyzer = OfstedAnalyzerAsync(ai_engine, ai_engine.openai_client)
            
            basic_ofsted = {
                'rating': intel.ofsted_rating,
                'inspection_date': intel.ofsted_date.isoformat() if intel.ofsted_date else None
            }
            
            enhanced_ofsted = await analyzer.get_enhanced_ofsted_analysis(
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
            logger.error(f"Ofsted enhancement error: {e}")
            return intel
    
    return enhance_with_ofsted_analysis
