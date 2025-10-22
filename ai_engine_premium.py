"""
Protocol Education CI System - Premium AI Engine (FIXED)
Uses Serper API for web search + GPT-4-turbo for analysis
Fixed: Proper JSON response structure for Streamlit display
"""

import os
import requests
from openai import OpenAI
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class PremiumAIEngine:
    """Premium research engine using Serper + GPT-4-turbo"""
    
    def __init__(self):
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.serper_api_key = os.getenv("SERPER_API_KEY")
        self.model = "gpt-4-turbo-preview"
        
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
        """Complete school research using search + GPT-4"""
        
        # Step 1: Search for school information
        search_query = f"{school_name} school UK"
        if location:
            search_query = f"{school_name} school {location} UK"
            
        logger.info(f"Searching for: {search_query}")
        search_results = self.search_web(search_query)
        
        # Step 2: Search for specific information
        ofsted_results = self.search_web(f"{school_name} Ofsted rating latest inspection report")
        contact_results = self.search_web(f"{school_name} headteacher deputy head staff directory")
        news_results = self.search_web(f"{school_name} school news awards achievements 2024")
        email_results = self.search_web(f"{school_name} school email contact @")
        
        # Step 3: Combine all search results
        all_results = {
            'general': search_results[:5],
            'ofsted': ofsted_results[:3],
            'contacts': contact_results[:3],
            'news': news_results[:3],
            'email_patterns': email_results[:2]
        }
        
        # Update usage for additional searches
        self.usage['searches'] += 4  # We added 4 more searches
        self.usage['search_cost'] += 0.08
        
        # Step 4: Analyze with GPT-4
        analysis = self._analyze_with_gpt(school_name, all_results)
        
        # Step 5: Structure the results
        return {
            'school_name': school_name,
            'location': location,
            'data': analysis,
            'sources': self._extract_sources(all_results),
            'search_timestamp': datetime.now().isoformat(),
            'usage': self.usage.copy()
        }
    
    def _analyze_with_gpt(self, school_name: str, search_results: Dict[str, List]) -> Dict[str, Any]:
        """Analyze search results with GPT-4-turbo - FIXED JSON STRUCTURE"""
        
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
            "RECENT SCHOOL NEWS (2023-2024)": {{
                "Recent achievements or awards": ["array", "of", "strings"],
                "Leadership changes": ["array", "of", "strings"],
                "Major events or initiatives": ["array", "of", "strings"],
                "Building projects": ["array", "of", "strings"],
                "Challenges mentioned": ["array", "of", "strings"]
            }},
            "RECRUITMENT INTELLIGENCE": {{
                "Any recruitment agencies mentioned in connection with the school": "string or Not found",
                "Recent job postings that mention agencies": "string or Not found",
                "Staff turnover indicators": "string or Not found"
            }},
            "CONVERSATION STARTERS for recruitment consultants": [
                "Specific talking point about recent Ofsted performance",
                "Specific talking point about leadership changes or initiatives",
                "Specific talking point about achievements or events"
            ],
            "PROTOCOL ADVANTAGES": [
                "How Protocol Education could help based on identified needs"
            ]
        }}

        Use "Not found" for any missing information. Base everything on the search results provided.
        Make sure arrays are properly formatted JSON arrays, not strings.

        Search Results:
        {search_text}
        """
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at analyzing search results to extract school information. Always return valid JSON that exactly matches the requested structure."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Very low for accuracy
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            # Track token usage
            if hasattr(response, 'usage'):
                tokens = response.usage.total_tokens
                self.usage['tokens_used'] += tokens
                # GPT-4-turbo pricing: $0.01/1k input, $0.03/1k output
                self.usage['gpt_cost'] += (tokens / 1000) * 0.02  # Average
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            
            # Ensure proper structure - normalize the data
            normalized_result = self._normalize_gpt_response(result)
            
            # Calculate confidence scores
            normalized_result = self._add_confidence_scores(normalized_result)
            
            return normalized_result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error from GPT: {e}")
            logger.error(f"Raw response: {response.choices[0].message.content if response else 'No response'}")
            return self._get_empty_structure()
        except Exception as e:
            logger.error(f"GPT analysis error: {e}")
            return self._get_empty_structure()
    
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
            "RECENT SCHOOL NEWS (2023-2024)": {
                "Recent achievements or awards": [],
                "Leadership changes": [],
                "Major events or initiatives": [],
                "Building projects": [],
                "Challenges mentioned": []
            },
            "RECRUITMENT INTELLIGENCE": {
                "Any recruitment agencies mentioned in connection with the school": "Not found",
                "Recent job postings that mention agencies": "Not found",
                "Staff turnover indicators": "Not found"
            },
            "CONVERSATION STARTERS for recruitment consultants": [],
            "PROTOCOL ADVANTAGES": []
        }
        
        # Merge the result with the template, ensuring all fields exist
        normalized = {}
        
        for section, fields in template.items():
            normalized[section] = {}
            
            if isinstance(fields, dict):
                for field, default in fields.items():
                    value = result.get(section, {}).get(field, default)
                    # Ensure lists are lists, not strings
                    if isinstance(default, list) and isinstance(value, str):
                        normalized[section][field] = [value] if value != "Not found" else []
                    else:
                        normalized[section][field] = value
            elif isinstance(fields, list):
                # For top-level lists like CONVERSATION STARTERS
                normalized[section] = result.get(section, [])
                if not isinstance(normalized[section], list):
                    normalized[section] = []
        
        # Handle special case for conversation starters and advantages
        if "CONVERSATION STARTERS for recruitment consultants" in result:
            starters = result["CONVERSATION STARTERS for recruitment consultants"]
            normalized["CONVERSATION STARTERS for recruitment consultants"] = starters if isinstance(starters, list) else []
        
        if "PROTOCOL ADVANTAGES" in result:
            advantages = result["PROTOCOL ADVANTAGES"]
            normalized["PROTOCOL ADVANTAGES"] = advantages if isinstance(advantages, list) else []
        
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
            "RECENT SCHOOL NEWS (2023-2024)": {
                "Recent achievements or awards": [],
                "Leadership changes": [],
                "Major events or initiatives": [],
                "Building projects": [],
                "Challenges mentioned": []
            },
            "RECRUITMENT INTELLIGENCE": {
                "Any recruitment agencies mentioned in connection with the school": "Not found",
                "Recent job postings that mention agencies": "Not found",
                "Staff turnover indicators": "Not found"
            },
            "CONVERSATION STARTERS for recruitment consultants": [],
            "PROTOCOL ADVANTAGES": [],
            "data_quality_score": 0.0
        }
    
    def _format_search_results(self, results: Dict[str, List]) -> str:
        """Format search results for GPT analysis"""
        
        formatted = []
        
        for category, items in results.items():
            formatted.append(f"\n=== {category.upper()} SEARCH RESULTS ===\n")
            
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
    
    def _add_confidence_scores(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add confidence scores based on data completeness"""
        
        # Calculate confidence for contacts
        contacts = data.get('KEY LEADERSHIP CONTACTS', {})
        for role, info in contacts.items():
            if info and info != 'Not found':
                # Add confidence based on whether we found the contact
                confidence = 0.8
            else:
                confidence = 0.0
        
        # Overall data quality score
        quality_score = 0.0
        checks = [
            (data.get('BASIC INFORMATION', {}).get('Website URL'), 0.2),
            (data.get('BASIC INFORMATION', {}).get('Main phone number'), 0.1),
            (data.get('BASIC INFORMATION', {}).get('Main email address'), 0.1),
            (data.get('OFSTED INFORMATION', {}).get('Current Ofsted rating'), 0.2),
            (data.get('KEY LEADERSHIP CONTACTS', {}).get('Headteacher/Principal'), 0.2),
            (len(data.get('RECENT SCHOOL NEWS (2023-2024)', {}).get('Recent achievements or awards', [])) > 0, 0.1),
            (len(data.get('CONVERSATION STARTERS for recruitment consultants', [])) > 0, 0.1)
        ]
        
        for check_value, weight in checks:
            if check_value and check_value != 'Not found':
                quality_score += weight
        
        data['data_quality_score'] = quality_score
        
        return data
    
    def get_usage_report(self) -> Dict[str, Any]:
        """Get current usage and costs"""
        
        self.usage['total_cost'] = self.usage['search_cost'] + self.usage['gpt_cost']
        
        return {
            **self.usage,
            'cost_per_school': self.usage['total_cost'] / max(self.usage['searches'] / 5, 1),  # Adjusted for 5 searches per school
            'monthly_projection': self.usage['total_cost'] * 30  # If running daily
        }