"""
Protocol Education CI System - Streamlit Web Interface
User-friendly web application for the intelligence system
Enhanced: Added Ofsted deep analysis and vacancy display
FIXED: Removed all black boxes and changed button to BLUE
FIXED: Corrected attribute names to match SchoolIntelligence model
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

# FIXED CSS - BLACK BOXES REMOVED + BLUE BUTTON
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
    
    /* BLUE BUTTON - FIXED FROM RED */
    button[kind="primary"] {
        background-color: #0066FF !important;
        color: #FFFFFF !important;
        border: none !important;
        font-weight: 600 !important;
    }
    
    button[kind="primary"]:hover {
        background-color: #0052CC !important;
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
    
    /* ===== COMPLETE BLACK BOX FIX - EXPANDERS ===== */
    /* Remove ALL dark backgrounds from expanders */
    .streamlit-expanderHeader {
        color: #000000 !important;
        font-weight: 600 !important;
        background-color: #F3F4F6 !important;
    }
    
    .streamlit-expanderContent {
        background-color: #FFFFFF !important;
        color: #000000 !important;
    }
    
    /* Fix ALL nested divs inside expanders */
    details {
        background-color: transparent !important;
    }
    
    details div {
        background-color: transparent !important;
        color: #000000 !important;
    }
    
    details[open] {
        background-color: #FFFFFF !important;
    }
    
    details[open] > div {
        background-color: #FFFFFF !important;
    }
    
    details > div > div {
        background-color: transparent !important;
    }
    
    /* Target the expander container specifically */
    [data-testid="stExpander"] {
        background-color: #FFFFFF !important;
        border: 1px solid #E5E7EB !important;
    }
    
    [data-testid="stExpander"] > div {
        background-color: #FFFFFF !important;
    }
    
    [data-testid="stExpander"] div {
        background-color: transparent !important;
    }
    
    /* Force all children of expanders to be transparent/white */
    [data-testid="stExpander"] * {
        background-color: transparent !important;
    }
    
    /* Make sure text inside expanders is visible */
    [data-testid="stExpander"] p,
    [data-testid="stExpander"] span,
    [data-testid="stExpander"] div {
        color: #000000 !important;
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
        border-bottom: 2px solid #0066FF !important;
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
        background-color: #F3F4F6 !important;
        color: #000000 !important;
        padding: 0.2rem 0.4rem;
        border-radius: 0.25rem;
    }
    
    /* Progress bar */
    .stProgress > div > div {
        background-color: #0066FF !important;
    }
</style>
""", unsafe_allow_html=True)

def display_school_intelligence(intel):
    """Display comprehensive school intelligence"""
    
    # Header metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("School", intel.school_name)
    with col2:
        if intel.ofsted_rating:
            st.metric("Ofsted Rating", intel.ofsted_rating)
        else:
            st.metric("Ofsted Rating", "Not Found")
    with col3:
        st.metric("Contacts Found", len(intel.contacts))
    with col4:
        quality_pct = f"{intel.data_quality_score:.0%}"
        st.metric("Data Quality", quality_pct)
    
    st.divider()
    
    # Tabs for different sections
    tabs = st.tabs([
        "📋 Overview",
        "👥 Contacts", 
        "📊 Financial Data",
        "📖 Ofsted Analysis",
        "🔍 Vacancies",
        "⚠️ Competitors",
        "💬 Conversation Starters"
    ])
    
    with tabs[0]:  # Overview
        display_overview(intel)
    
    with tabs[1]:  # Contacts
        display_contacts(intel)
    
    with tabs[2]:  # Financial Data
        display_financial_analysis(intel)
    
    with tabs[3]:  # Ofsted
        display_ofsted_analysis(intel)
    
    with tabs[4]:  # Vacancies
        display_vacancies(intel)
    
    with tabs[5]:  # Competitors
        display_competitors(intel)
    
    with tabs[6]:  # Conversation Starters
        display_conversation_starters(intel)

def display_overview(intel):
    """Display school overview information"""
    
    st.subheader("School Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Name:** {intel.school_name}")
        if intel.website:
            st.write(f"**Website:** [{intel.website}]({intel.website})")
        if intel.phone_main:
            st.write(f"**Phone:** {intel.phone_main}")
    
    with col2:
        if intel.address:
            st.write(f"**Address:** {intel.address}")
    
    st.divider()
    
    # Show recent achievements if available
    if intel.recent_achievements:
        st.subheader("Recent Achievements")
        for achievement in intel.recent_achievements[:3]:
            st.write(f"✅ {achievement}")
    
    # Show upcoming events if available
    if intel.upcoming_events:
        st.subheader("Upcoming Events")
        for event in intel.upcoming_events[:3]:
            st.write(f"📅 {event}")
    
    # Show leadership changes if available
    if intel.leadership_changes:
        st.subheader("Leadership Changes")
        for change in intel.leadership_changes:
            st.write(f"👤 {change}")

def display_contacts(intel):
    """Display contact information"""
    
    if not intel.contacts:
        st.info("No contacts found for this school")
        return
    
    st.write(f"Found {len(intel.contacts)} contacts")
    
    # Display all contacts
    for contact in intel.contacts:
        display_contact_card(contact)

def display_contact_card(contact):
    """Display individual contact card"""
    
    # Determine confidence class based on score
    if contact.confidence_score >= 0.7:
        confidence_class = "confidence-high"
        confidence_text = "High"
    elif contact.confidence_score >= 0.5:
        confidence_class = "confidence-medium"
        confidence_text = "Medium"
    else:
        confidence_class = "confidence-low"
        confidence_text = "Low"
    
    st.markdown(f"""
    <div class="contact-card">
        <strong>{contact.full_name}</strong> - {contact.role.value.replace('_', ' ').title()}<br>
        <span class="{confidence_class}">Confidence: {confidence_text}</span>
    </div>
    """, unsafe_allow_html=True)
    
    if contact.email:
        st.write(f"📧 {contact.email}")
    if contact.phone:
        phone_display = contact.phone
        if contact.phone_extension:
            phone_display += f" ext. {contact.phone_extension}"
        st.write(f"📞 {phone_display}")
    
    if contact.evidence_urls:
        with st.expander("View Evidence"):
            for url in contact.evidence_urls[:3]:
                st.write(f"- {url}")

def display_conversation_starters(intel):
    """Display AI-generated conversation starters"""
    
    if not intel.conversation_starters:
        st.info("No conversation starters available")
        return
    
    st.subheader("Conversation Starters")
    st.write("Use these insights to start meaningful conversations:")
    
    for i, starter in enumerate(intel.conversation_starters, 1):
        with st.expander(f"Approach {i}: {starter.topic}"):
            st.write(f"**Topic:** {starter.topic}")
            st.write(f"**Detail:** {starter.detail}")
            if starter.source_url:
                st.write(f"**Source:** {starter.source_url}")
            if starter.relevance_score:
                st.write(f"**Relevance:** {starter.relevance_score:.0%}")

def display_competitors(intel):
    """Display competitor information"""
    
    if not intel.competitors:
        st.success("✅ No competitor agencies detected")
        return
    
    st.warning(f"⚠️ Found {len(intel.competitors)} competitor agencies")
    
    for comp in intel.competitors:
        st.markdown(f"""
        <div style="background-color: #FEE2E2; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem;">
            <span class="competitor-badge">{comp.agency_name}</span>
            <p style="color: #000000 !important; margin-top: 0.5rem;">
                <strong>Type:</strong> {comp.presence_type}<br>
                <strong>Confidence:</strong> {comp.confidence_score:.0%}
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        if comp.weaknesses:
            st.write("**Their Weaknesses:**")
            for weakness in comp.weaknesses:
                st.write(f"• {weakness}")
        
        if comp.evidence_urls:
            with st.expander("View Evidence"):
                for url in comp.evidence_urls[:3]:
                    st.write(f"- {url}")
    
    # Show win-back strategy if available
    if intel.win_back_strategy:
        st.subheader("🎯 Win-Back Strategy")
        st.info(intel.win_back_strategy)
    
    # Show Protocol advantages
    if intel.protocol_advantages:
        st.subheader("💪 Protocol Education Advantages")
        for advantage in intel.protocol_advantages:
            st.write(f"✅ {advantage}")

def display_financial_analysis(intel):
    """Display financial analysis data - SIMPLIFIED to just show the link"""
    
    if hasattr(intel, 'financial_data') and intel.financial_data:
        
        # Check if we have a URL
        if 'url' in intel.financial_data:
            financial_url = intel.financial_data['url']
            urn = intel.financial_data.get('urn', 'Unknown')
            school_name = intel.financial_data.get('school_name', intel.school_name)
            url_valid = intel.financial_data.get('url_valid', True)
            
            st.subheader("📊 School Financial Data")
            
            st.success(f"✅ Financial data found for URN: {urn}")
            
            # Show the clickable link prominently
            st.markdown(f"### [View {school_name}'s Financial Data →]({financial_url})")
            
            st.info(f"**Direct Link:** {financial_url}")
            
            if url_valid:
                st.write("✅ Link verified and working")
            else:
                st.warning("⚠️ Could not verify link (may still work)")
            
            st.write("---")
            st.caption("This link will take you to the UK Government's Financial Benchmarking and Insights Tool where you can view:")
            st.caption("• Teaching staff costs")
            st.caption("• Administrative supplies spending") 
            st.caption("• Revenue reserves and in-year balance")
            st.caption("• Spending priorities and comparisons")
        
        else:
            st.warning("⚠️ Financial data structure not recognized")
            st.write("**Available keys:**", list(intel.financial_data.keys()))
    
    else:
        st.info("💡 No financial data available for this school")
        st.caption("Financial data may not be available if:")
        st.caption("• The school's URN could not be found")
        st.caption("• The school has not published financial data")
        st.caption("• The school is newly opened")
        
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
            if intel.ofsted_date:
                st.write(f"Inspection Date: {intel.ofsted_date.strftime('%B %Y')}")

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
