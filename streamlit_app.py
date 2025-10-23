"""
Protocol Education CI System - Streamlit Web Interface
User-friendly web application for the intelligence system
Enhanced: Added Ofsted deep analysis and vacancy display
FIXED: Removed all black boxes and improved visibility
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os

from processor_premium import PremiumSchoolProcessor
from exporter import IntelligenceExporter
from cache import IntelligenceCache
from models import ContactType

# Page configuration
st.set_page_config(
    page_title="Protocol Education Research Assistant",
    page_icon="P",
    layout="wide"
)

# Initialize components
@st.cache_resource
def get_processor():
    return PremiumSchoolProcessor()

@st.cache_resource
def get_exporter():
    return IntelligenceExporter()

@st.cache_resource
def get_cache():
    return IntelligenceCache()

processor = get_processor()
exporter = get_exporter()
cache = get_cache()

# FIXED CSS - ONLY BLACK BOX FIX, EVERYTHING ELSE ORIGINAL
st.markdown("""
<style>
    /* White background everywhere */
    .stApp {
        background-color: #FFFFFF !important;
    }
    
    /* ALL TEXT BLACK */
    body, p, span, div, label, li, td, th, h1, h2, h3, h4, h5, h6 {
        color: #000000 !important;
    }
    
    /* Streamlit specific text elements */
    .stMarkdown, .stText {
        color: #000000 !important;
    }
    
    /* Headers BOLD BLACK */
    h1, h2, h3, h4, h5, h6 {
        color: #000000 !important;
        font-weight: 700 !important;
    }
    
    /* Input fields - black text, white background */
    input, textarea, select {
        color: #000000 !important;
        background-color: #FFFFFF !important;
        border: 2px solid #CCCCCC !important;
    }
    
    /* Buttons remain styled */
    button[kind="primary"] {
        background-color: #FF4B4B !important;
        color: #FFFFFF !important;
    }
    
    /* Metrics - BLACK */
    [data-testid="stMetricValue"] {
        color: #000000 !important;
        font-size: 24px !important;
        font-weight: bold !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #000000 !important;
        font-weight: 600 !important;
    }
    
    /* ===== BLACK BOX FIX - EXPANDERS ===== */
    .streamlit-expanderHeader {
        color: #000000 !important;
        font-weight: 600 !important;
        background-color: #F3F4F6 !important;
    }
    
    .streamlit-expanderContent {
        background-color: #FFFFFF !important;
        color: #000000 !important;
    }
    
    /* Fix nested divs inside expanders - THIS IS THE KEY FIX */
    details div {
        background-color: transparent !important;
        color: #000000 !important;
    }
    
    details[open] > div {
        background-color: #FFFFFF !important;
    }
    
    [data-testid="stExpander"] {
        background-color: #FFFFFF !important;
    }
    
    [data-testid="stExpander"] > div {
        background-color: #FFFFFF !important;
    }
    /* ===== END BLACK BOX FIX ===== */
    
    /* Success/Info/Warning/Error boxes - KEEP ORIGINAL COLORS */
    .stAlert {
        color: #000000 !important;
    }
    
    [data-baseweb="notification"] {
        background-color: #FFFFFF !important;
        color: #000000 !important;
    }
    
    .stSuccess {
        background-color: #D1FAE5 !important;
        color: #065F46 !important;
    }
    
    .stInfo {
        background-color: #DBEAFE !important;
        color: #1E40AF !important;
    }
    
    .stWarning {
        background-color: #FEF3C7 !important;
        color: #92400E !important;
    }
    
    .stError {
        background-color: #FEE2E2 !important;
        color: #991B1B !important;
    }
    
    /* Contact cards */
    .contact-card {
        background-color: #F9FAFB !important;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border: 1px solid #E5E7EB;
        color: #000000 !important;
    }
    
    /* Competitor badges */
    .competitor-badge {
        background-color: #EF4444;
        color: #FFFFFF;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        display: inline-block;
        margin-right: 0.5rem;
        font-weight: 600;
    }
    
    /* Confidence colors */
    .confidence-high { 
        color: #16A34A !important;
        font-weight: 600;
    }
    .confidence-medium { 
        color: #EA580C !important;
        font-weight: 600;
    }
    .confidence-low { 
        color: #DC2626 !important;
        font-weight: 600;
    }
    
    /* Sidebar - light background */
    [data-testid="stSidebar"] {
        background-color: #F9FAFB !important;
    }
    
    [data-testid="stSidebar"] * {
        color: #000000 !important;
    }
    
    /* Tabs - BLACK TEXT */
    .stTabs [data-baseweb="tab-list"] button {
        color: #000000 !important;
        font-weight: 600 !important;
    }
    
    .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
        color: #000000 !important;
        border-bottom: 2px solid #FF4B4B !important;
    }
    
    /* Tables */
    table {
        color: #000000 !important;
    }
    
    th {
        background-color: #F3F4F6 !important;
        color: #000000 !important;
        font-weight: 700 !important;
    }
    
    td {
        color: #000000 !important;
        border-bottom: 1px solid #E5E7EB !important;
    }
    
    /* Code blocks */
    code {
        color: #000000 !important;
        background-color: #F3F4F6 !important;
    }
    
    pre {
        color: #000000 !important;
        background-color: #F3F4F6 !important;
    }
</style>
""", unsafe_allow_html=True)

# Define all display functions first
def display_school_intelligence(intel):
    """Display school intelligence in Streamlit"""
    
    # Header metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Data Quality", f"{intel.data_quality_score:.0%}")
    with col2:
        st.metric("Contacts Found", len(intel.contacts))
    with col3:
        st.metric("Competitors", len(intel.competitors))
    with col4:
        st.metric("Processing Time", f"{intel.processing_time:.1f}s")
    
    # School info
    st.subheader(f"{intel.school_name}")
    if intel.website:
        st.write(f"[{intel.website}]({intel.website})")
    
    # Tabs for different sections
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Contacts", "Competitors", "Intelligence", "Financial Data", 
        "Ofsted Analysis", "Vacancies", "Raw Data"
    ])
    
    with tab1:
        display_contacts(intel.contacts)
    
    with tab2:
        display_competitors(intel)
    
    with tab3:
        display_conversation_intel(intel)
    
    with tab4:
        display_financial_data(intel)
    
    with tab5:
        display_ofsted_analysis(intel)
    
    with tab6:
        display_vacancies(intel)
    
    with tab7:
        # Show raw data for debugging
        st.json({
            'school_name': intel.school_name,
            'data_quality_score': intel.data_quality_score,
            'sources_checked': intel.sources_checked,
            'contacts_count': len(intel.contacts),
            'competitors_count': len(intel.competitors),
            'has_ofsted_enhanced': hasattr(intel, 'ofsted_enhanced'),
            'has_vacancy_data': hasattr(intel, 'vacancy_data')
        })

def display_contacts(contacts):
    """Display contact information"""
    
    if not contacts:
        st.warning("No contacts found")
        return
    
    # Group by role
    for role in ContactType:
        role_contacts = [c for c in contacts if c.role == role]
        
        if role_contacts:
            st.write(f"**{role.value.replace('_', ' ').title()}**")
            
            for contact in role_contacts:
                confidence_class = (
                    "confidence-high" if contact.confidence_score > 0.8
                    else "confidence-medium" if contact.confidence_score > 0.5
                    else "confidence-low"
                )
                
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**{contact.full_name}**")
                    if contact.email:
                        st.write(f"Email: {contact.email}")
                    if contact.phone:
                        st.write(f"Phone: {contact.phone}")
                
                with col2:
                    st.markdown(
                        f'<span class="{confidence_class}">Confidence: {contact.confidence_score:.0%}</span>',
                        unsafe_allow_html=True
                    )
                
                st.divider()

def display_competitors(intel):
    """Display competitor analysis"""
    
    if not intel.competitors:
        st.info("No competitors detected")
        return
    
    st.write("**Detected Competitors:**")
    
    for comp in intel.competitors:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(
                f'<span class="competitor-badge">{comp.agency_name}</span>',
                unsafe_allow_html=True
            )
            st.write(f"Type: {comp.presence_type}")
            
            if comp.weaknesses:
                st.write("Weaknesses:")
                for weakness in comp.weaknesses:
                    st.write(f"  • {weakness}")
        
        with col2:
            st.metric("Confidence", f"{comp.confidence_score:.0%}")
    
    if intel.win_back_strategy:
        st.write("**Win-back Strategy:**")
        st.info(intel.win_back_strategy)
    
    if intel.protocol_advantages:
        st.write("**Protocol Advantages:**")
        for advantage in intel.protocol_advantages:
            st.write(f"✓ {advantage}")

def display_conversation_intel(intel):
    """Display conversation intelligence"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        if intel.ofsted_rating:
            st.write(f"**Ofsted Rating:** {intel.ofsted_rating}")
        
        if intel.recent_achievements:
            st.write("**Recent Achievements:**")
            for achievement in intel.recent_achievements[:5]:
                st.write(f"• {achievement}")
    
    with col2:
        if intel.upcoming_events:
            st.write("**Upcoming Events:**")
            for event in intel.upcoming_events[:5]:
                st.write(f"• {event}")
        
        if intel.leadership_changes:
            st.write("**Leadership Changes:**")
            for change in intel.leadership_changes[:3]:
                st.write(f"• {change}")
    
    if intel.conversation_starters:
        st.write("**Top Conversation Starters:**")
        
        # Show top 5 conversation starters
        for i, starter in enumerate(intel.conversation_starters[:5], 1):
            with st.expander(f"{i}. {starter.topic} (Relevance: {starter.relevance_score:.0%})"):
                st.write(starter.detail)
                if hasattr(starter, 'source_url') and starter.source_url:
                    st.caption(f"Source: {starter.source_url}")

def display_financial_data(intel):
    """Display financial data and recruitment costs"""
    
    if hasattr(intel, 'financial_data') and intel.financial_data:
        financial = intel.financial_data
        
        if financial.get('error'):
            st.warning(f"Could not retrieve financial data: {financial['error']}")
            return
        
        # Entity info
        if 'entity_found' in financial:
            entity = financial['entity_found']
            
            if entity['type'] == 'Trust':
                st.info(f"Found trust-level financial data for **{entity['name']}**")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"**Entity:** {entity['name']}")
                st.write(f"**Type:** {entity['type']}")
            with col2:
                st.write(f"**URN:** {entity['urn']}")
            with col3:
                st.write(f"**Confidence:** {entity.get('confidence', 0):.0%}")
        
        st.divider()
        
        # Financial data
        if 'financial' in financial and financial['financial']:
            fin_data = financial['financial']
            
            # Recruitment costs
            if 'recruitment_estimates' in fin_data:
                st.subheader("Annual Recruitment Costs")
                
                estimates = fin_data['recruitment_estimates']
                
                if 'total_trust' in estimates:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Trust Total", f"£{estimates['total_trust']:,}")
                    with col2:
                        st.metric("Per School Avg", f"£{estimates['per_school_avg']:,}")
                    with col3:
                        st.metric("Savings", estimates['economies_of_scale_saving'])
                else:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Low Estimate", f"£{estimates['low']:,}")
                    with col2:
                        st.metric("Best Estimate", f"£{estimates['midpoint']:,}")
                    with col3:
                        st.metric("High Estimate", f"£{estimates['high']:,}")
            
            # Show insights
            if 'insights' in financial and financial['insights']:
                st.subheader("Key Insights")
                for insight in financial['insights']:
                    st.write(f"• {insight}")
        
    else:
        st.info("No financial data available for this school")

def display_ofsted_analysis(intel):
    """Display enhanced Ofsted analysis"""
    
    if hasattr(intel, 'ofsted_enhanced') and intel.ofsted_enhanced:
        ofsted_data = intel.ofsted_enhanced
        
        # Header
        col1, col2, col3 = st.columns(3)
        with col1:
            rating = ofsted_data.get('rating', 'Unknown')
            st.metric("Ofsted Rating", rating)
        with col2:
            if ofsted_data.get('inspection_date'):
                st.metric("Inspection Date", ofsted_data['inspection_date'])
        with col3:
            main_improvements = ofsted_data.get('main_improvements', [])
            st.metric("Key Priorities", len(main_improvements))
        
        if ofsted_data.get('report_url'):
            st.write(f"[View Full Ofsted Report]({ofsted_data['report_url']})")
        
        st.divider()
        
        # Priority order
        priority_order = ofsted_data.get('priority_order', [])
        if priority_order:
            st.error("OFSTED IMPROVEMENT PRIORITIES")
            for i, priority in enumerate(priority_order[:5], 1):
                st.write(f"**{i}. {priority}**")
            st.markdown("---")
        
        # Main improvements
        main_improvements = ofsted_data.get('main_improvements', [])
        if main_improvements:
            st.subheader("Key Areas for Improvement")
            
            for improvement in main_improvements:
                with st.expander(f"**{improvement['area']}**", expanded=True):
                    st.write(improvement['description'])
                    if improvement.get('specifics'):
                        st.write(f"*Details: {improvement['specifics']}*")
        
        # Subject improvements
        subject_improvements = ofsted_data.get('subject_improvements', {})
        if subject_improvements:
            st.subheader("Subject-Specific Improvements")
            
            cols = st.columns(min(3, len(subject_improvements)))
            
            for idx, (subject, details) in enumerate(subject_improvements.items()):
                with cols[idx % 3]:
                    urgency = details.get('urgency', 'MEDIUM')
                    if urgency == 'HIGH':
                        st.error(f"**{subject.upper()}** - HIGH PRIORITY")
                    else:
                        st.warning(f"**{subject.upper()}**")
                    
                    issues = details.get('issues', [])
                    for issue in issues[:2]:
                        st.write(f"• {issue}")
        
    else:
        if intel.ofsted_rating:
            st.info(f"Ofsted Rating: {intel.ofsted_rating}")

def display_vacancies(intel):
    """Display vacancy information"""
    
    if not hasattr(intel, 'vacancy_data') or not intel.vacancy_data:
        st.info("No vacancy data available")
        return
    
    vacancy_data = intel.vacancy_data
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Vacancies", vacancy_data['total_found'])
    with col2:
        st.metric("Senior Roles", vacancy_data['senior_roles'])
    with col3:
        urgency = vacancy_data['analysis']['urgency_level']
        st.metric("Urgency", urgency.title())
    with col4:
        st.metric("Last Checked", datetime.now().strftime('%H:%M'))
    
    st.info(f"Found {vacancy_data['total_found']} active vacancies")
        
def display_borough_summary(results):
    """Display borough sweep summary"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    high_quality = sum(1 for r in results if r.data_quality_score > 0.7)
    with_contacts = sum(1 for r in results if r.contacts)
    with_competitors = sum(1 for r in results if r.competitors)
    avg_quality = sum(r.data_quality_score for r in results) / len(results) if results else 0
    
    with col1:
        st.metric("Schools Processed", len(results))
    with col2:
        st.metric("High Quality Data", f"{high_quality}/{len(results)}")
    with col3:
        st.metric("With Contacts", with_contacts)
    with col4:
        st.metric("Avg Quality", f"{avg_quality:.0%}")

# Header
st.title("Protocol Education Research Assistant")
st.markdown("**Intelligent school research and contact discovery system**")

# Sidebar
with st.sidebar:
    st.header("Controls")
    
    operation_mode = st.radio(
        "Operation Mode",
        ["Single School", "Borough Sweep"]
    )
    
    export_format = st.selectbox(
        "Export Format",
        ["Excel (.xlsx)", "CSV (.csv)", "JSON (.json)"]
    )
    
    st.divider()
    
    # Feature toggles
    st.subheader("Features")
    enable_ofsted = st.checkbox("Enhanced Ofsted Analysis", value=True)
    enable_vacancies = st.checkbox("Vacancy Detection", value=True)
    
    st.divider()
    
    # Cache stats
    if st.button("Show Cache Stats"):
        stats = cache.get_stats()
        st.metric("Active Entries", stats.get('active_entries', 0))
        st.metric("Hit Rate", f"{stats.get('hit_rate', 0):.1%}")
    
    if st.button("Clear Cache"):
        cache.clear_expired()
        st.success("Cache cleared!")
    
    st.divider()
    
    # API usage
    usage = processor.ai_engine.get_usage_report()
    st.metric("API Cost Today", f"${usage['total_cost']:.3f}")

# Main content
if operation_mode == "Single School":
    st.header("Single School Lookup")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        school_name = st.text_input("School Name", placeholder="e.g., St Mary's Primary School")
        website_url = st.text_input("Website URL (optional)", placeholder="https://...")
    
    with col2:
        force_refresh = st.checkbox("Force Refresh")
        
    if st.button("Search School", type="primary"):
        if school_name:
            with st.spinner(f"Processing {school_name}..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("Searching...")
                progress_bar.progress(20)
                
                intel = processor.process_single_school(
                    school_name, 
                    website_url,
                    force_refresh
                )
                
                progress_bar.progress(100)
                status_text.text("Complete!")
                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()
            
            display_school_intelligence(intel)
            
            if st.button("Export Results"):
                format_map = {
                    "Excel (.xlsx)": "xlsx",
                    "CSV (.csv)": "csv",
                    "JSON (.json)": "json"
                }
                filepath = exporter.export_single_school(
                    intel, 
                    format_map[export_format]
                )
                st.success(f"Exported to: {filepath}")

elif operation_mode == "Borough Sweep":
    st.header("Borough-wide Intelligence Sweep")
    
    col1, col2 = st.columns(2)
    
    with col1:
        borough_name = st.text_input("Borough Name")
    
    with col2:
        school_type = st.selectbox("School Type", ["All", "Primary", "Secondary"])
    
    if st.button("Start Borough Sweep", type="primary"):
        if borough_name:
            with st.spinner(f"Processing {borough_name} schools..."):
                results = processor.process_borough(
                    borough_name,
                    school_type.lower()
                )
            
            st.success(f"Processed {len(results)} schools!")
            display_borough_summary(results)

if __name__ == "__main__":
    if not os.path.exists('.env'):
        st.warning(".env file not found")


