"""
Protocol Education CI System - Export Module
Handles exporting intelligence data to various formats
"""

import csv
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from config import OUTPUT_DIR, EXPORT_FORMATS
from models import SchoolIntelligence, ContactType

logger = logging.getLogger(__name__)

class IntelligenceExporter:
    """Export intelligence data to CSV, Excel, and JSON formats"""
    
    def __init__(self):
        self.output_dir = Path(OUTPUT_DIR)
        self.output_dir.mkdir(exist_ok=True)
    
    def export_single_school(self, intel: SchoolIntelligence, 
                           format: str = 'xlsx') -> str:
        """Export single school intelligence to file"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_name = "".join(c for c in intel.school_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        filename = f"{safe_name}_{timestamp}"
        
        if format == 'csv':
            filepath = self._export_to_csv([intel], filename)
        elif format == 'xlsx':
            filepath = self._export_to_excel([intel], filename)
        elif format == 'json':
            filepath = self._export_to_json([intel], filename)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
        logger.info(f"Exported {intel.school_name} to {filepath}")
        return str(filepath)
    
    def export_borough_results(self, results: List[SchoolIntelligence],
                             borough_name: str, format: str = 'xlsx') -> str:
        """Export borough-wide results"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{borough_name}_schools_{timestamp}"
        
        if format == 'csv':
            filepath = self._export_to_csv(results, filename)
        elif format == 'xlsx':
            filepath = self._export_to_excel(results, filename)
        elif format == 'json':
            filepath = self._export_to_json(results, filename)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
        logger.info(f"Exported {len(results)} schools to {filepath}")
        return str(filepath)
    
    def _export_to_csv(self, results: List[SchoolIntelligence], 
                      filename: str) -> Path:
        """Export to CSV format"""
        
        filepath = self.output_dir / f"{filename}.csv"
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Headers
            headers = [
                'School Name', 'Website', 'Data Quality Score',
                'Deputy Head', 'Deputy Email', 'Deputy Phone', 'Deputy Confidence',
                'Assistant Head', 'Assistant Email', 'Assistant Phone', 'Assistant Confidence',
                'Business Manager', 'Business Email', 'Business Phone', 'Business Confidence',
                'SENCO', 'SENCO Email', 'SENCO Phone', 'SENCO Confidence',
                'Competitors', 'Win-back Strategy',
                'Ofsted Rating', 'Top Conversation Starter',
                'Recent Achievements', 'Evidence URLs'
            ]
            writer.writerow(headers)
            
            # Data rows
            for intel in results:
                row = self._build_csv_row(intel)
                writer.writerow(row)
                
        return filepath
    
    def _export_to_excel(self, results: List[SchoolIntelligence], 
                        filename: str) -> Path:
        """Export to Excel with multiple sheets"""
        
        filepath = self.output_dir / f"{filename}.xlsx"
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # Main overview sheet
            overview_data = []
            for intel in results:
                overview_data.append({
                    'School': intel.school_name,
                    'Website': intel.website,
                    'Quality Score': f"{intel.data_quality_score:.0%}",
                    'Deputy Head': self._get_contact_name(intel, ContactType.DEPUTY_HEAD),
                    'Has Email': self._has_contact_email(intel, ContactType.DEPUTY_HEAD),
                    'Has Phone': self._has_contact_phone(intel, ContactType.DEPUTY_HEAD),
                    'Competitors': ', '.join([c.agency_name for c in intel.competitors]),
                    'Ofsted': intel.ofsted_rating or 'Unknown'
                })
            
            df_overview = pd.DataFrame(overview_data)
            df_overview.to_excel(writer, sheet_name='Overview', index=False)
            
            # Detailed contacts sheet
            contacts_data = []
            for intel in results:
                for contact in intel.contacts:
                    contacts_data.append({
                        'School': intel.school_name,
                        'Role': contact.role.value.replace('_', ' ').title(),
                        'Name': contact.full_name,
                        'Email': contact.email or '',
                        'Phone': contact.phone or '',
                        'Confidence': f"{contact.confidence_score:.0%}",
                        'Verified': contact.verification_method,
                        'Evidence URL': contact.evidence_urls[0] if contact.evidence_urls else ''
                    })
            
            if contacts_data:
                df_contacts = pd.DataFrame(contacts_data)
                df_contacts.to_excel(writer, sheet_name='All Contacts', index=False)
            
            # Competitive analysis sheet
            comp_data = []
            for intel in results:
                for comp in intel.competitors:
                    comp_data.append({
                        'School': intel.school_name,
                        'Competitor': comp.agency_name,
                        'Presence Type': comp.presence_type,
                        'Weaknesses': ', '.join(comp.weaknesses),
                        'Confidence': f"{comp.confidence_score:.0%}"
                    })
                    
            if comp_data:
                df_comp = pd.DataFrame(comp_data)
                df_comp.to_excel(writer, sheet_name='Competitor Analysis', index=False)
            
            # Conversation starters sheet
            conv_data = []
            for intel in results:
                for starter in intel.conversation_starters[:3]:  # Top 3
                    conv_data.append({
                        'School': intel.school_name,
                        'Topic': starter.topic,
                        'Detail': starter.detail,
                        'Relevance': f"{starter.relevance_score:.0%}"
                    })
                    
            if conv_data:
                df_conv = pd.DataFrame(conv_data)
                df_conv.to_excel(writer, sheet_name='Conversation Starters', index=False)
            
            # Format the Excel file
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                    
        return filepath
    
    def _export_to_json(self, results: List[SchoolIntelligence], 
                       filename: str) -> Path:
        """Export to JSON format"""
        
        filepath = self.output_dir / f"{filename}.json"
        
        data = []
        for intel in results:
            data.append({
                'school_name': intel.school_name,
                'website': intel.website,
                'data_quality_score': intel.data_quality_score,
                'contacts': [
                    {
                        'role': c.role.value,
                        'name': c.full_name,
                        'email': c.email,
                        'phone': c.phone,
                        'confidence': c.confidence_score,
                        'evidence_urls': c.evidence_urls
                    }
                    for c in intel.contacts
                ],
                'competitors': [
                    {
                        'agency': c.agency_name,
                        'type': c.presence_type,
                        'weaknesses': c.weaknesses,
                        'confidence': c.confidence_score
                    }
                    for c in intel.competitors
                ],
                'intelligence': {
                    'ofsted_rating': intel.ofsted_rating,
                    'conversation_starters': [
                        {
                            'topic': s.topic,
                            'detail': s.detail,
                            'relevance': s.relevance_score
                        }
                        for s in intel.conversation_starters
                    ],
                    'recent_achievements': intel.recent_achievements,
                    'upcoming_events': intel.upcoming_events,
                    'leadership_changes': intel.leadership_changes
                },
                'metadata': {
                    'last_updated': intel.last_updated.isoformat(),
                    'processing_time': intel.processing_time,
                    'sources_checked': intel.sources_checked
                }
            })
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        return filepath
    
    def _build_csv_row(self, intel: SchoolIntelligence) -> List[str]:
        """Build a CSV row from intelligence data"""
        
        row = [
            intel.school_name,
            intel.website,
            f"{intel.data_quality_score:.0%}"
        ]
        
        # Add contact info for each role
        for role in [ContactType.DEPUTY_HEAD, ContactType.ASSISTANT_HEAD, 
                    ContactType.BUSINESS_MANAGER, ContactType.SENCO]:
            contact = self._get_contact_by_role(intel, role)
            if contact:
                row.extend([
                    contact.full_name,
                    contact.email or '',
                    contact.phone or '',
                    f"{contact.confidence_score:.0%}"
                ])
            else:
                row.extend(['', '', '', ''])
        
        # Competitors
        competitors = ', '.join([c.agency_name for c in intel.competitors])
        row.append(competitors)
        row.append(intel.win_back_strategy or '')
        
        # Intelligence
        row.append(intel.ofsted_rating or '')
        
        # Top conversation starter
        if intel.conversation_starters:
            top_starter = intel.conversation_starters[0]
            row.append(f"{top_starter.topic}: {top_starter.detail}")
        else:
            row.append('')
        
        # Recent achievements
        achievements = '; '.join(intel.recent_achievements[:3])
        row.append(achievements)
        
        # Evidence URLs
        all_urls = set()
        for contact in intel.contacts:
            all_urls.update(contact.evidence_urls)
        row.append(' | '.join(list(all_urls)[:3]))
        
        return row
    
    def _get_contact_by_role(self, intel: SchoolIntelligence, 
                           role: ContactType) -> Optional:
        """Get contact by role type"""
        for contact in intel.contacts:
            if contact.role == role:
                return contact
        return None
    
    def _get_contact_name(self, intel: SchoolIntelligence, 
                         role: ContactType) -> str:
        """Get contact name by role"""
        contact = self._get_contact_by_role(intel, role)
        return contact.full_name if contact else ''
    
    def _has_contact_email(self, intel: SchoolIntelligence, 
                          role: ContactType) -> str:
        """Check if contact has email"""
        contact = self._get_contact_by_role(intel, role)
        return '✓' if contact and contact.email else ''
    
    def _has_contact_phone(self, intel: SchoolIntelligence, 
                          role: ContactType) -> str:
        """Check if contact has phone"""
        contact = self._get_contact_by_role(intel, role)
        return '✓' if contact and contact.phone else ''