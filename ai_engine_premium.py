"""
Protocol Education CI System - Premium AI Engine (ENHANCED v2.0)
Uses Serper API for web search + GPT-4o for analysis

WHAT'S NEW:
- Upgraded to GPT-4o (faster, smarter, cheaper)
- 13 targeted searches per school (was 5)
- Date filtering (only last 12 months)
- Completely rewritten GPT prompts for staffing-focused conversation starters
- Removed vacancy detection (unreliable)
- Added source diversity scoring
"""

import os
import requests
from openai import OpenAI
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class PremiumAIEngine:
    """Premium research engine using Serper + GPT-4o"""
    
    def __init__(self):
        # Get API keys from Streamlit secrets (Cloud) or environment (Local)
        try:
            import streamlit as st
            openai_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
            serper_key = st.secrets.get("SERPER_API_KEY", os.getenv("SERPER_API_KEY"))
        except:
            openai_key = os.getenv("OPENAI_API_KEY")
            serper_key = os.getenv("SERPER_API_KEY")
        
        self.openai_client = OpenAI(api_key=openai_key)
        self.serper_api_key = serper_key
        
        # UPGRADED MODEL
        self.model = "gpt-4o"  # Was: gpt-4-turbo-preview
        
        # Cost tracking
        self.usage = {
            'searches': 0,
            'search_cost': 0.0,
            'tokens_used': 0,
            'gpt_cost': 0.0,
            'total_cost': 0.0
        }

        
    def search_web(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """Search using Serper API"""
        
        url = "https://google.serper.dev/search"
        
        payload = json.dumps({
            "q": query,
            "gl": "uk",  # UK results
            "hl": "en",
            "num": num_results
        })
        
        headers = {
            'X-API-KEY': self.serper_api_key,
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            
            # Track usage
            self.usage['searches'] += 1
            self.usage['search_cost'] += 0.02  # $50/2500 = $0.02 per search
            
            data = response.json()
            
            # Extract organic results
            results = []
            for item in data.get('organic', []):
                results.append({
                    'title': item.get('title', ''),
                    'url': item.get('link', ''),
                    'snippet': item.get('snippet', ''),
                    'position': item.get('position', 0)
                })
            
            # Also get knowledge graph if available
            if 'knowledgeGraph' in data:
                kg = data['knowledgeGraph']
                results.insert(0, {
                    'title': kg.get('title', ''),
                    'url': kg.get('website', ''),
                    'snippet': kg.get('description', ''),
                    'type': 'knowledge_graph',
                    'attributes': kg.get('attributes', {})
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Serper search error: {e}")
            return []
    
    def research_school(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete school research using search + GPT-4o
        
        NOW WITH 13 TARGETED SEARCHES (was 5)
        """
        
        # Calculate date cutoffs
        twelve_months_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        ninety_days_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        
        # SEARCH 1: General school information
        search_query = f"{school_name} school UK"
        if location:
            search_query = f"{school_name} school {location} UK"
            
        logger.info(f"Starting 13-search research for: {school_name}")
        general_results = self.search_web(search_query)
        
        # SEARCH 2: Ofsted rating and report
        ofsted_results = self.search_web(
            f"{school_name} Ofsted rating latest inspection report"
        )
        
        # SEARCH 3: Staff and contact information
        contact_results = self.search_web(
            f"{school_name} headteacher deputy head staff directory"
        )
        
        # SEARCH 4: Recent news (DATE FILTERED - last 90 days)
        recent_news = self.search_web(
            f"{school_name} after:{ninety_days_ago} news OR achievements OR awards"
        )
        
        # SEARCH 5: School blog/newsletter (DATE FILTERED - last 6 months)
        blog_results = self.search_web(
            f"{school_name} after:{six_months_ago} newsletter OR blog OR news"
        )
        
        # SEARCH 6: Google Reviews (parent feedback)
        review_results = self.search_web(
            f'"{school_name}" site:google.com/maps reviews'
        )
        
        # SEARCH 7: Parent forums (Mumsnet, Netmums)
        forum_results = self.search_web(
            f'"{school_name}" after:{twelve_months_ago} (site:mumsnet.com OR site:netmums.com)'
        )
        
        # SEARCH 8: Local news mentions (last 6 months)
        local_news = self.search_web(
            f'"{school_name}" after:{six_months_ago} site:.co.uk/news'
        )
        
        # SEARCH 9: LinkedIn profiles (public only - leadership team)
        linkedin_results = self.search_web(
            f'"{school_name}" site:linkedin.com/in (headteacher OR deputy OR principal)'
        )
        
        # SEARCH 10: Trust announcements (if academy)
        trust_results = self.search_web(
            f'"{school_name}" after:{six_months_ago} trust OR academy OR MAT announcements'
        )
        
        # SEARCH 11: Governor meeting minutes (public)
        governor_results = self.search_web(
            f'"{school_name}" after:{twelve_months_ago} governors minutes'
        )
        
        # SEARCH 12: Staff changes/recruitment
        staffing_results = self.search_web(
            f'"{school_name}" after:{six_months_ago} (staff OR teacher OR headteacher) (join OR leaving OR appointed OR recruitment)'
        )
        
        # SEARCH 13: Email patterns
        email_results = self.search_web(f"{school_name} school email contact @")
        
        # Combine all search results
        all_results = {
            'general': general_results[:5],
            'ofsted': ofsted_results[:3],
            'contacts': contact_results[:3],
            'recent_news': recent_news[:4],
            'blog_newsletter': blog_results[:3],
            'google_reviews': review_results[:3],
            'parent_forums': forum_results[:2],
            'local_news': local_news[:3],
            'linkedin': linkedin_results[:2],
            'trust_announcements': trust_results[:2],
            'governor_minutes': governor_results[:2],
            'staffing_changes': staffing_results[:3],
            'email_patterns': email_results[:2]
        }
        
        # Update usage tracking
        self.usage['searches'] += 12  # We did 12 additional searches
        self.usage['search_cost'] += 0.24  # 12 * $0.02
        
        # Analyze with GPT-4o using ENHANCED PROMPT
        analysis = self._analyze_with_gpt(school_name, all_results)
        
        # Structure the results
        return {
            'school_name': school_name,
            'location': location,
            'data': analysis,
            'sources': self._extract_sources(all_results),
            'search_timestamp': datetime.now().isoformat(),
            'usage': self.usage.copy()
        }
    
    def _analyze_with_gpt(self, school_name: str, search_results: Dict[str, List]) -> Dict[str, Any]:
        """Analyze search results with GPT-4o - ENHANCED STAFFING-FOCUSED PROMPT"""
        
        # Format search results for GPT
        search_text = self._format_search_results(search_results)
        
        prompt = f"""
        Analyze these search results for {school_name} and extract the following information.
        
        IMPORTANT: Your response must be valid JSON that exactly matches this structure:
        {{
            "BASIC INFORMATION": {{
                "Full school name": "string",
                "School type": "string (primary/secondary/etc)",
                "Website URL": "string (official school website only)",
                "Main phone number": "string",
                "Main email address": "string",
                "Full address": "string",
                "Number of pupils": "string or number"
            }},
            "KEY LEADERSHIP CONTACTS": {{
                "Headteacher/Principal": "string (full name) or Not found",
                "Deputy Headteacher": "string (full name) or Not found",
                "Assistant Headteacher": "string (full name) or Not found",
                "Business Manager": "string (full name) or Not found",
                "SENCO": "string (full name) or Not found"
            }},
            "CONTACT DETAILS": {{
                "Email patterns": "string (e.g., firstname.lastname@school.org.uk)",
                "Direct phone numbers": "string",
                "Best verified email addresses": "string"
            }},
            "OFSTED INFORMATION": {{
                "Current Ofsted rating": "string (Outstanding/Good/Requires Improvement/Inadequate/Not found)",
                "Date of last inspection": "string (e.g., 15 March 2024)",
                "Previous rating": "string or Not found",
                "Key strengths": ["array", "of", "strings"],
                "Areas for improvement": ["array", "of", "strings"]
            }},
            "RECENT SCHOOL NEWS (LAST 12 MONTHS ONLY)": {{
                "Recent achievements or awards": ["array with dates - ONLY from last 12 months"],
                "Leadership changes": ["array - staff joining/leaving with dates"],
                "Major events or initiatives": ["array with dates"],
                "Building projects": ["array"],
                "Challenges mentioned": ["array - ONLY staffing-related challenges"]
            }},
            "RECRUITMENT INTELLIGENCE": {{
                "Any recruitment agencies mentioned in connection with the school": "string or Not found",
                "Recent job postings that mention agencies": "string or Not found",
                "Staff turnover indicators": "string or Not found",
                "Subjects with staffing challenges": ["array of subjects mentioned"]
            }},
            "STAFFING-FOCUSED CONVERSATION STARTERS": [
                "Each must reference 3+ sources, include dates, and connect to staffing needs"
            ],
            "PROTOCOL ADVANTAGES": [
                "How Protocol Education teachers could help based on identified staffing needs"
            ]
        }}

        CRITICAL REQUIREMENTS FOR CONVERSATION STARTERS:
        
        You are an elite recruitment intelligence analyst for Protocol Education, 
        a teacher recruitment agency. Create COMPELLING conversation starters that 
        help recruitment consultants win staffing contracts.

        RULES:
        1. MUST use information from the last 12 MONTHS only (reject anything older)
        2. MUST reference at least 3 different sources per conversation starter
        3. EVERY conversation starter MUST connect to staffing/recruitment needs
        4. Use simple, natural language

        STRUCTURE:
        - HOOK: Recent specific observation (name, number, or date)
        - INSIGHT: Why this creates a staffing challenge
        - SOLUTION: How Protocol Education teachers can solve it
        - QUESTION: Natural conversation opener

        EXAMPLE:
        "I saw in your September newsletter that you've introduced mastery maths across 
        Year 3-6, and your Ofsted report highlighted maths as a development priority. 
        With your Maths Lead leaving last term (mentioned on your website), you're 
        probably finding it tough to keep that momentum going. We've got maths specialists 
        who've done exactly this before - helped schools embed new teaching methods when 
        key staff move on. Would having an experienced maths teacher for a term or two 
        help you keep things on track while you recruit permanently?"

        WHAT COUNTS AS STAFFING ISSUES:
        ✓ Subject teaching quality (needs specialist teachers)
        ✓ Leadership gaps (needs deputy heads, subject leads)
        ✓ SEND provision (needs SEND teachers, SENCOs)
        ✓ Staff leaving/recruitment challenges
        ✓ Curriculum changes (needs teachers trained in new methods)
        ✓ Ofsted improvement plans requiring better teaching
        ✓ New initiatives needing specialist skills

        IGNORE (not staffing related):
        ✗ Pupil attendance issues
        ✗ Building/facilities problems
        ✗ Budget concerns (unless about affording staff)
        ✗ Parent engagement

        Create 5 conversation starters. Each must:
        - Use 3+ different sources
        - Mention something from last 12 months with date
        - Connect directly to a staffing need
        - End with a thoughtful question
        - Use simple language

        If you cannot find 3 recent sources connecting to staffing needs, 
        include fewer conversation starters rather than making up generic statements.

        Search Results:
        {search_text}
        """
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at analyzing search results to extract school information and create staffing-focused recruitment intelligence. Always return valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low for accuracy
                max_tokens=2500,
                response_format={"type": "json_object"}
            )
            
            # Track token usage
            if hasattr(response, 'usage'):
                tokens = response.usage.total_tokens
                self.usage['tokens_used'] += tokens
                # GPT-4o pricing: $2.50/1M input, $10/1M output
                self.usage['gpt_cost'] += (tokens / 1000000) * 5  # Average
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            
            # Normalize and add metadata
            normalized_result = self._normalize_gpt_response(result)
            normalized_result = self._add_confidence_scores(normalized_result)
            
            # Calculate source diversity
            sources = self._extract_sources(search_results)
            normalized_result['source_diversity_score'] = self._calculate_source_diversity(sources)
            
            return normalized_result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error from GPT: {e}")
            return self._get_empty_structure()
        except Exception as e:
            logger.error(f"GPT analysis error: {e}")
            return self._get_empty_structure()
    
    def _calculate_source_diversity(self, sources: List[str]) -> float:
        """Calculate how diverse the sources are (penalize single-source intelligence)"""
        if not sources:
            return 0.0
        
        # Extract domains
        domains = []
        for url in sources:
            try:
                domain = url.split('/')[2].replace('www.', '')
                domains.append(domain)
            except:
                continue
        
        if not domains:
            return 0.0
        
        # Count unique domains
        unique_domains = len(set(domains))
        
        # Target is 3+ different sources
        diversity_score = min(unique_domains / 3.0, 1.0)
        
        return round(diversity_score, 2)
    
    def _normalize_gpt_response(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure GPT response has all required fields with proper types"""
        
        # Define the expected structure with defaults
        template = {
            "BASIC INFORMATION": {
                "Full school name": "Not found",
                "School type": "Not found",
                "Website URL": "Not found",
                "Main phone number": "Not found",
                "Main email address": "Not found",
                "Full address": "Not found",
                "Number of pupils": "Not found"
            },
            "KEY LEADERSHIP CONTACTS": {
                "Headteacher/Principal": "Not found",
                "Deputy Headteacher": "Not found",
                "Assistant Headteacher": "Not found",
                "Business Manager": "Not found",
                "SENCO": "Not found"
            },
            "CONTACT DETAILS": {
                "Email patterns": "Not found",
                "Direct phone numbers": "Not found",
                "Best verified email addresses": "Not found"
            },
            "OFSTED INFORMATION": {
                "Current Ofsted rating": "Not found",
                "Date of last inspection": "Not found",
                "Previous rating": "Not found",
                "Key strengths": [],
                "Areas for improvement": []
            },
            "RECENT SCHOOL NEWS (LAST 12 MONTHS ONLY)": {
                "Recent achievements or awards": [],
                "Leadership changes": [],
                "Major events or initiatives": [],
                "Building projects": [],
                "Challenges mentioned": []
            },
            "RECRUITMENT INTELLIGENCE": {
                "Any recruitment agencies mentioned in connection with the school": "Not found",
                "Recent job postings that mention agencies": "Not found",
                "Staff turnover indicators": "Not found",
                "Subjects with staffing challenges": []
            },
            "STAFFING-FOCUSED CONVERSATION STARTERS": [],
            "PROTOCOL ADVANTAGES": []
        }
        
        # Merge result with template
        normalized = {}
        
        for section, fields in template.items():
            normalized[section] = {}
            
            if isinstance(fields, dict):
                for field, default in fields.items():
                    value = result.get(section, {}).get(field, default)
                    # Ensure lists are lists
                    if isinstance(default, list) and isinstance(value, str):
                        normalized[section][field] = [value] if value != "Not found" else []
                    else:
                        normalized[section][field] = value
            elif isinstance(fields, list):
                # Top-level lists
                normalized[section] = result.get(section, [])
                if not isinstance(normalized[section], list):
                    normalized[section] = []
        
        # Handle conversation starters specifically
        if "STAFFING-FOCUSED CONVERSATION STARTERS" in result:
            starters = result["STAFFING-FOCUSED CONVERSATION STARTERS"]
            normalized["STAFFING-FOCUSED CONVERSATION STARTERS"] = starters if isinstance(starters, list) else []
        
        # Handle old key name (backwards compatibility)
        if "CONVERSATION STARTERS for recruitment consultants" in result:
            old_starters = result["CONVERSATION STARTERS for recruitment consultants"]
            if not normalized.get("STAFFING-FOCUSED CONVERSATION STARTERS"):
                normalized["STAFFING-FOCUSED CONVERSATION STARTERS"] = old_starters if isinstance(old_starters, list) else []
        
        return normalized
    
    def _get_empty_structure(self) -> Dict[str, Any]:
        """Return empty but properly structured data"""
        return {
            "BASIC INFORMATION": {
                "Full school name": "Not found",
                "School type": "Not found",
                "Website URL": "Not found",
                "Main phone number": "Not found",
                "Main email address": "Not found",
                "Full address": "Not found",
                "Number of pupils": "Not found"
            },
            "KEY LEADERSHIP CONTACTS": {
                "Headteacher/Principal": "Not found",
                "Deputy Headteacher": "Not found",
                "Assistant Headteacher": "Not found",
                "Business Manager": "Not found",
                "SENCO": "Not found"
            },
            "CONTACT DETAILS": {
                "Email patterns": "Not found",
                "Direct phone numbers": "Not found",
                "Best verified email addresses": "Not found"
            },
            "OFSTED INFORMATION": {
                "Current Ofsted rating": "Not found",
                "Date of last inspection": "Not found",
                "Previous rating": "Not found",
                "Key strengths": [],
                "Areas for improvement": []
            },
            "RECENT SCHOOL NEWS (LAST 12 MONTHS ONLY)": {
                "Recent achievements or awards": [],
                "Leadership changes": [],
                "Major events or initiatives": [],
                "Building projects": [],
                "Challenges mentioned": []
            },
            "RECRUITMENT INTELLIGENCE": {
                "Any recruitment agencies mentioned in connection with the school": "Not found",
                "Recent job postings that mention agencies": "Not found",
                "Staff turnover indicators": "Not found",
                "Subjects with staffing challenges": []
            },
            "STAFFING-FOCUSED CONVERSATION STARTERS": [],
            "PROTOCOL ADVANTAGES": [],
            "data_quality_score": 0.0,
            "source_diversity_score": 0.0
        }
    
    def _format_search_results(self, results: Dict[str, List]) -> str:
        """Format search results for GPT analysis"""
        
        formatted = []
        
        for category, items in results.items():
            if not items:
                continue
                
            formatted.append(f"\n=== {category.upper().replace('_', ' ')} ===\n")
            
            for i, item in enumerate(items, 1):
                formatted.append(f"{i}. {item.get('title', 'No title')}")
                formatted.append(f"   URL: {item.get('url', 'No URL')}")
                formatted.append(f"   {item.get('snippet', 'No snippet')}")
                
                # Include knowledge graph attributes if present
                if item.get('type') == 'knowledge_graph' and 'attributes' in item:
                    for key, value in item['attributes'].items():
                        formatted.append(f"   {key}: {value}")
                
                formatted.append("")
        
        return "\n".join(formatted)
    
    def _extract_sources(self, results: Dict[str, List]) -> List[str]:
        """Extract unique source URLs"""
        
        sources = set()
        for category, items in results.items():
            for item in items:
                if url := item.get('url'):
                    sources.add(url)
        
        return list(sources)
    
    def _add_confidence_scores(self, data: Dict[str, Any]) -> float:
        """Add confidence scores based on data completeness"""
        
        quality_score = 0.0
        checks = [
            (data.get('BASIC INFORMATION', {}).get('Website URL'), 0.2),
            (data.get('BASIC INFORMATION', {}).get('Main phone number'), 0.1),
            (data.get('OFSTED INFORMATION', {}).get('Current Ofsted rating'), 0.2),
            (data.get('KEY LEADERSHIP CONTACTS', {}).get('Headteacher/Principal'), 0.2),
            (len(data.get('RECENT SCHOOL NEWS (LAST 12 MONTHS ONLY)', {}).get('Recent achievements or awards', [])) > 0, 0.15),
            (len(data.get('STAFFING-FOCUSED CONVERSATION STARTERS', [])) > 0, 0.15)
        ]
        
        for check_value, weight in checks:
            if check_value and check_value != 'Not found':
                quality_score += weight
        
        data['data_quality_score'] = round(quality_score, 2)
        
        return data
    
    def get_usage_report(self) -> Dict[str, Any]:
        """Get current usage and costs"""
        
        self.usage['total_cost'] = self.usage['search_cost'] + self.usage['gpt_cost']
        
        return {
            **self.usage,
            'cost_per_school': self.usage['total_cost'] / max(self.usage['searches'] / 13, 1),  # Adjusted for 13 searches
            'monthly_projection': self.usage['total_cost'] * 30
        }
