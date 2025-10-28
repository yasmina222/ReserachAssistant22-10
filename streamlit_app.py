"""
Protocol Education CI System - Streamlit Web Interface
User-friendly web application for the intelligence system
Enhanced: Added Ofsted deep analysis and vacancy display
FIXED: Removed all black boxes and changed button to BLUE
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
        st.write(f"🌐 {intel.website}")
    if intel.ofsted_rating:
        st.write(f"⭐ Ofsted: {intel.ofsted_rating}")
    
    st.divider()
    
    # Create tabs
    tabs = st.tabs(["Conversation Starters", "Contacts", "Competitors", "Financial Analysis", "Ofsted Analysis", "Vacancies"])
    
    # Tab 1: Conversation starters
    with tabs[0]:
        display_conversation_starters(intel)
    
    # Tab 2: Contacts
    with tabs[1]:
        display_contacts(intel)
    
    # Tab 3: Competitors
    with tabs[2]:
        display_competitors(intel)
    
    # Tab 4: Financial Analysis
    with tabs[3]:
        display_financial_analysis(intel)
    
    # Tab 5: Ofsted Analysis
    with tabs[4]:
        display_ofsted_analysis(intel)
    
    # Tab 6: Vacancies
    with tabs[5]:
        display_vacancies(intel)

def display_conversation_starters(intel):
    """Display AI-generated conversation starters"""
    
    if intel.conversation_starters:
        st.info(f"📋 Generated {len(intel.conversation_starters)} conversation starters")
        
        for i, starter in enumerate(intel.conversation_starters, 1):
            with st.expander(f"**Conversation Starter #{i}**", expanded=(i == 1)):
                # Handle different data structures
                if isinstance(starter, str):
                    # If starter is just a string
                    st.write(starter)
                    
                elif hasattr(starter, 'detail'):
                    # ConversationStarter object from models.py
                    # Show topic as header if available
                    if hasattr(starter, 'topic') and starter.topic:
                        st.markdown(f"**{starter.topic}**")
                    
                    # Show the main detail/content
                    st.write(starter.detail)
                    
                    # Show source URL if available
                    if hasattr(starter, 'source_url') and starter.source_url:
                        st.write(f"**Source:** {starter.source_url}")
                    
                    # Show relevance score if available
                    if hasattr(starter, 'relevance_score') and starter.relevance_score:
                        score = starter.relevance_score
                        if score > 0.8:
                            confidence_class = "confidence-high"
                            confidence_label = "HIGH"
                        elif score > 0.6:
                            confidence_class = "confidence-medium"
                            confidence_label = "MEDIUM"
                        else:
                            confidence_class = "confidence-low"
                            confidence_label = "LOW"
                        
                        st.markdown(
                            f'<span class="{confidence_class}">Relevance: {confidence_label} ({score:.0%})</span>',
                            unsafe_allow_html=True
                        )
                    
                    # Show date if available
                    if hasattr(starter, 'date') and starter.date:
                        st.caption(f"Date: {starter.date.strftime('%Y-%m-%d')}")
                        
                elif isinstance(starter, dict):
                    # Dictionary format (fallback)
                    # Look for text in various keys
                    text = starter.get('detail') or starter.get('text') or starter.get('starter') or starter.get('content') or str(starter)
                    st.write(text)
                    
                    # Show topic if available
                    if 'topic' in starter:
                        st.caption(f"Topic: {starter['topic']}")
                    
                    # Show sources if available
                    sources = starter.get('sources', [])
                    if sources:
                        st.write("**Sources:**")
                        for source in sources:
                            st.write(f"• {source}")
                    
                    # Show relevance/confidence if available
                    relevance = starter.get('relevance_score') or starter.get('confidence')
                    if relevance:
                        st.caption(f"Relevance: {relevance:.0%}")
                else:
                    # Fallback: just display whatever it is
                    st.write(str(starter))
    else:
        st.warning("No conversation starters generated")

def display_contacts(intel):
    """Display contact information"""
    
    if intel.contacts:
        st.success(f"Found {len(intel.contacts)} contacts")
        
        for contact in intel.contacts:
            with st.container():
                # Handle Contact objects properly
                if hasattr(contact, 'full_name'):
                    # Contact object from models.py
                    name = contact.full_name
                    role = contact.role.value.replace('_', ' ').title() if hasattr(contact.role, 'value') else str(contact.role)
                    email = contact.email or 'Not available'
                    phone = contact.phone or ''
                    confidence = f"{contact.confidence_score:.0%}" if hasattr(contact, 'confidence_score') else ''
                    
                    st.markdown(f"""
                    <div class="contact-card">
                        <h4>{name}</h4>
                        <p><strong>Role:</strong> {role}</p>
                        <p><strong>Email:</strong> {email}</p>
                        {f'<p><strong>Phone:</strong> {phone}</p>' if phone else ''}
                        {f'<p><strong>Confidence:</strong> {confidence}</p>' if confidence else ''}
                        {f'<p><strong>Notes:</strong> {contact.notes}</p>' if hasattr(contact, 'notes') and contact.notes else ''}
                    </div>
                    """, unsafe_allow_html=True)
                    
                elif isinstance(contact, dict):
                    # Dictionary format (fallback)
                    name = contact.get('name', contact.get('full_name', 'Unknown'))
                    role = contact.get('role', 'Unknown')
                    email = contact.get('email', 'Not available')
                    phone = contact.get('phone', '')
                    source = contact.get('source', '')
                    
                    st.markdown(f"""
                    <div class="contact-card">
                        <h4>{name}</h4>
                        <p><strong>Role:</strong> {role}</p>
                        <p><strong>Email:</strong> {email}</p>
                        {f'<p><strong>Phone:</strong> {phone}</p>' if phone else ''}
                        {f'<p><strong>Source:</strong> {source}</p>' if source else ''}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.write(str(contact))
    else:
        st.info("No contacts found")

def display_competitors(intel):
    """Display competitor agencies"""
    
    if intel.competitors:
        st.warning(f"⚠️ {len(intel.competitors)} competitor(s) detected")
        
        for comp in intel.competitors:
            # Handle CompetitorPresence objects properly
            if hasattr(comp, 'agency_name'):
                # CompetitorPresence object from models.py
                name = comp.agency_name
                presence = comp.presence_type if hasattr(comp, 'presence_type') else 'Unknown'
                confidence = f"{comp.confidence_score:.0%}" if hasattr(comp, 'confidence_score') else ''
                
                # Get evidence
                evidence = ''
                if hasattr(comp, 'evidence_urls') and comp.evidence_urls:
                    evidence = f"Found in: {', '.join(comp.evidence_urls[:2])}"
                
                # Get weaknesses
                weaknesses = ''
                if hasattr(comp, 'weaknesses') and comp.weaknesses:
                    weaknesses = '<br>'.join([f"• {w}" for w in comp.weaknesses[:3]])
                
                st.markdown(f"""
                <div class="contact-card">
                    <span class="competitor-badge">COMPETITOR</span>
                    <strong>{name}</strong>
                    <p><strong>Presence Type:</strong> {presence}</p>
                    {f'<p><strong>Confidence:</strong> {confidence}</p>' if confidence else ''}
                    {f'<p>{evidence}</p>' if evidence else ''}
                    {f'<p><strong>Identified Weaknesses:</strong><br>{weaknesses}</p>' if weaknesses else ''}
                </div>
                """, unsafe_allow_html=True)
                
            elif isinstance(comp, dict):
                # Dictionary format (fallback)
                name = comp.get('name', comp.get('agency_name', 'Unknown'))
                evidence = comp.get('evidence', comp.get('presence_type', ''))
                source = comp.get('source', '')
                
                st.markdown(f"""
                <div class="contact-card">
                    <span class="competitor-badge">COMPETITOR</span>
                    <strong>{name}</strong>
                    <p>{evidence}</p>
                    {f'<p><em>Source: {source}</em></p>' if source else ''}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.write(str(comp))
    else:
        st.success("✅ No competitor agencies detected")

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
