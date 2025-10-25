"""
Protocol Education CI System - Financial Data Engine
PROVEN WORKING VERSION from August 2025
Uses: schools-financial-benchmarking.service.gov.uk (OLD SITE THAT WORKS)
"""

import re
import logging
import requests
import os
from typing import Dict, Optional, List, Any
from datetime import datetime
from models import ConversationStarter

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper_engine = serper_engine
        
        # Get ScraperAPI key if available
        try:
            import streamlit as st
            self.scraper_api_key = st.secrets.get('SCRAPER_API_KEY', os.getenv('SCRAPER_API_KEY'))
        except:
            self.scraper_api_key = os.getenv('SCRAPER_API_KEY')
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry point - get financial intelligence for a school
        This is what processor_premium.py calls
        """
        try:
            logger.info(f"🎯 Getting recruitment intelligence for: {school_name}")
            
            # Step 1: Find school URN
            urn_result = self._get_school_urn(school_name, location)
            
            if not urn_result.get('urn'):
                return {
                    'error': 'Could not find school URN',
                    'urn': None,
                    'financial': {}
                }
            
            urn = urn_result['urn']
            logger.info(f"✅ Found URN: {urn}")
            
            # Step 2: Get financial data from OLD SITE (the one that works!)
            financial_data = self._scrape_fbit_data(urn, school_name)
            
            # Step 3: Generate conversation starters
            conversation_starters = self._generate_financial_starters(financial_data, school_name)
            
            return {
                'urn': urn,
                'financial': financial_data,
                'conversation_starters': conversation_starters,
                'source_url': f"https://schools-financial-benchmarking.service.gov.uk/school/{urn}"
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting financial data: {e}")
            return {
                'error': str(e),
                'urn': None,
                'financial': {}
            }
    
    def _get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN using Serper API to search government database
        """
        try:
            # Build search query for GIAS
            query_parts = [f'"{school_name}"']
            if location:
                query_parts.append(location)
            query_parts.append("site:get-information-schools.service.gov.uk URN")
            
            search_query = " ".join(query_parts)
            logger.info(f"🔍 Searching for URN: {search_query}")
            
            # Search using Serper
            results = self.serper_engine.search_web(search_query, num_results=5)
            
            # Extract URN from results
            for result in results:
                snippet = result.get('snippet', '')
                link = result.get('link', '')
                title = result.get('title', '')
                
                # Try to extract URN from URL (most reliable)
                urn_match = re.search(r'/Details/(\d{5,7})', link)
                if urn_match:
                    return {
                        'urn': urn_match.group(1),
                        'official_name': self._extract_school_name(title),
                        'confidence': 0.95
                    }
                
                # Try to extract from snippet
                urn_match = re.search(r'URN[:\s]+(\d{5,7})', snippet)
                if urn_match:
                    return {
                        'urn': urn_match.group(1),
                        'official_name': self._extract_school_name(title),
                        'confidence': 0.85
                    }
            
            # Fallback: Try searching the OLD financial site directly
            logger.info("🔍 Trying OLD financial benchmarking site")
            fallback_query = f'"{school_name}" site:schools-financial-benchmarking.service.gov.uk'
            if location:
                fallback_query += f' {location}'
            
            results = self.serper_engine.search_web(fallback_query, num_results=3)
            
            for result in results:
                link = result.get('link', '')
                # Extract URN from old site URL: /school/123456
                urn_match = re.search(r'/school/(\d{5,7})', link)
                if urn_match:
                    return {
                        'urn': urn_match.group(1),
                        'official_name': self._extract_school_name(result.get('title', '')),
                        'confidence': 0.8
                    }
            
            return {'urn': None, 'confidence': 0.0}
            
        except Exception as e:
            logger.error(f"Error finding URN: {e}")
            return {'urn': None, 'error': str(e)}
    
    def _scrape_fbit_data(self, urn: str, school_name: str) -> Dict[str, Any]:
        """
        Get financial data from OLD government site (schools-financial-benchmarking.service.gov.uk)
        This is the site that actually works!
        """
        
        # ALWAYS create the link first
        financial_data = {
            'urn': urn,
            'school_name': school_name,
            'source_url': f"https://schools-financial-benchmarking.service.gov.uk/school/{urn}",
            'extracted_date': datetime.now().isoformat()
        }
        
        # Try to search for specific data points
        search_queries = [
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "Teaching and Teaching support staff" "per pupil"',
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "In year balance"',
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "Administrative supplies" "per pupil"',
            f'site:schools-financial-benchmarking.service.gov.uk/school/{urn} "Supply staff costs"'
        ]
        
        all_content = ""
        for query in search_queries[:2]:  # Just try first 2 to save API calls
            try:
                results = self.serper_engine.search_web(query, num_results=2)
                for result in results:
                    all_content += result.get('snippet', '') + " "
            except Exception as e:
                logger.warning(f"Search failed: {e}")
        
        # Extract data from search results
        if all_content:
            # Teaching staff per pupil
            teaching_match = re.search(r'Teaching.*?£([\d,]+)\s*per pupil', all_content, re.IGNORECASE)
            if teaching_match:
                financial_data['teaching_staff_per_pupil'] = int(teaching_match.group(1).replace(',', ''))
            
            # In year balance
            balance_match = re.search(r'In year balance.*?[−-]?£([\d,]+)', all_content, re.IGNORECASE | re.DOTALL)
            if balance_match:
                amount = int(balance_match.group(1).replace(',', ''))
                # Check if negative
                if '−' in balance_match.group(0) or 'deficit' in balance_match.group(0).lower():
                    amount = -amount
                financial_data['in_year_balance'] = amount
            
            # Administrative supplies
            admin_match = re.search(r'Administrative supplies.*?£([\d,]+)\s*per pupil', all_content, re.IGNORECASE)
            if admin_match:
                financial_data['admin_supplies_per_pupil'] = int(admin_match.group(1).replace(',', ''))
            
            # Supply staff costs
            supply_match = re.search(r'Supply staff.*?£([\d,]+)', all_content, re.IGNORECASE)
            if supply_match:
                financial_data['supply_staff_costs'] = int(supply_match.group(1).replace(',', ''))
        
        logger.info(f"📊 Extracted financial data: {financial_data}")
        return financial_data
    
    def _generate_financial_starters(self, financial_data: Dict, school_name: str) -> List[str]:
        """Generate conversation starters based on financial data"""
        starters = []
        
        # Always include link to financial data
        starters.append(
            f"I can see {school_name}'s financial data on the government's benchmarking tool. "
            "This provides valuable insights into spending patterns. Protocol Education can help "
            "optimize your recruitment and supply staff costs - would you like to discuss this?"
        )
        
        # Add specific insights if we have the data
        if 'in_year_balance' in financial_data:
            balance = financial_data['in_year_balance']
            if balance < 0:
                starters.append(
                    f"I noticed your school is managing a deficit of £{abs(balance):,}. "
                    "Many schools find that partnering with a reliable recruitment agency helps "
                    "reduce costs while maintaining quality. Shall we explore how Protocol can help?"
                )
            else:
                starters.append(
                    f"Your school has a healthy surplus of £{balance:,}. This is a great position "
                    "to be in - Protocol Education can help you maintain this through cost-effective "
                    "recruitment solutions."
                )
        
        if 'teaching_staff_per_pupil' in financial_data:
            teaching = financial_data['teaching_staff_per_pupil']
            starters.append(
                f"With teaching costs at £{teaching:,} per pupil, ensuring value in recruitment "
                "is essential. Protocol's quality guarantee ensures you get excellent teachers "
                "at competitive rates - shall we discuss your current recruitment challenges?"
            )
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            starters.append(
                f"Your supply staff costs of £{supply:,} suggest there's opportunity to optimize. "
                "Protocol specializes in providing reliable supply teachers at competitive rates - "
                "would you like to see how we can help reduce these costs?"
            )
        
        return starters
    
    def _extract_school_name(self, title: str) -> str:
        """Extract clean school name from search result title"""
        # Remove common suffixes
        name = re.split(r' - URN:| - Get Information| - GOV\.UK| \| ', title)[0]
        return name.strip()


def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    Called from processor_premium.py
    """
    try:
        financial_engine = FinancialDataEngine(serper_engine)
        
        logger.info(f"🏦 Enhancing {intel.school_name} with financial data")
        
        financial_intel = financial_engine.get_recruitment_intelligence(
            intel.school_name,
            intel.address
        )
        
        if not financial_intel.get('error'):
            # Add conversation starters
            if 'conversation_starters' in financial_intel:
                for starter in financial_intel['conversation_starters']:
                    intel.conversation_starters.append(
                        ConversationStarter(
                            topic="Recruitment Costs",
                            detail=starter,
                            source_url=financial_intel.get('source_url', ''),
                            relevance_score=0.9
                        )
                    )
            
            # Store financial data
            intel.financial_data = financial_intel
            
            logger.info(f"✅ Successfully enhanced {intel.school_name} with financial data")
        else:
            logger.warning(f"⚠️ Could not get financial data for {intel.school_name}: {financial_intel.get('error')}")
    
    except Exception as e:
        logger.error(f"❌ Error enhancing school with financial data: {e}")
    
    return intel
