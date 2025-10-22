"""
Protocol Education CI System - Data Models
Defines all data structures and JSON schemas for OpenAI structured outputs
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class ContactType(Enum):
    DEPUTY_HEAD = "deputy_head"
    ASSISTANT_HEAD = "assistant_head"
    BUSINESS_MANAGER = "business_manager"
    SENCO = "senco"

class ConfidenceLevel(Enum):
    VERY_HIGH = (0.9, 1.0, "Direct website evidence")
    HIGH = (0.7, 0.89, "Multiple corroborating sources")
    MEDIUM = (0.5, 0.69, "Single source with patterns")
    LOW = (0.3, 0.49, "Indirect indicators only")
    VERY_LOW = (0.0, 0.29, "No reliable evidence")

@dataclass
class Contact:
    """Represents a school contact with verification data"""
    role: ContactType
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    phone_extension: Optional[str] = None
    confidence_score: float = 0.0
    evidence_urls: List[str] = field(default_factory=list)
    last_verified: datetime = field(default_factory=datetime.now)
    verification_method: str = ""
    notes: Optional[str] = None

@dataclass
class CompetitorPresence:
    """Tracks competitor activity at a school"""
    agency_name: str
    presence_type: str  # 'job_posting', 'testimonial', 'partner_page'
    evidence_urls: List[str] = field(default_factory=list)
    last_seen: datetime = field(default_factory=datetime.now)
    confidence_score: float = 0.0
    weaknesses: List[str] = field(default_factory=list)

@dataclass
class ConversationStarter:
    """Intelligence for engaging school contacts"""
    topic: str
    detail: str
    source_url: str
    date: Optional[datetime] = None
    relevance_score: float = 0.0

@dataclass
class SchoolIntelligence:
    """Complete intelligence package for a school"""
    # Basic Info
    school_name: str
    website: str
    address: Optional[str] = None
    phone_main: Optional[str] = None
    
    # Contacts
    contacts: List[Contact] = field(default_factory=list)
    
    # Competitive Intelligence
    competitors: List[CompetitorPresence] = field(default_factory=list)
    protocol_advantages: List[str] = field(default_factory=list)
    win_back_strategy: Optional[str] = None
    
    # Conversation Intelligence
    ofsted_rating: Optional[str] = None
    ofsted_date: Optional[datetime] = None
    conversation_starters: List[ConversationStarter] = field(default_factory=list)
    recent_achievements: List[str] = field(default_factory=list)
    upcoming_events: List[str] = field(default_factory=list)
    leadership_changes: List[str] = field(default_factory=list)
    
    # Metadata
    last_updated: datetime = field(default_factory=datetime.now)
    data_quality_score: float = 0.0
    processing_time: float = 0.0
    sources_checked: int = 0
    
    # Financial data (optional)
    financial_data: Optional[Dict[str, Any]] = None

# OpenAI Structured Output Schemas
CONTACT_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "contacts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "role": {"type": "string", "enum": ["deputy_head", "assistant_head", "business_manager", "senco"]},
                    "full_name": {"type": "string"},
                    "email": {"type": "string"},
                    "phone": {"type": "string"},
                    "phone_extension": {"type": "string"},
                    "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "evidence_text": {"type": "string"}
                },
                "required": ["role", "full_name", "confidence_score"],
                "additionalProperties": False
            }
        },
        "email_pattern": {"type": "string"},
        "main_phone": {"type": "string"}
    },
    "required": ["contacts"],
    "additionalProperties": False
}

COMPETITOR_ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "competitors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "agency_name": {"type": "string"},
                    "presence_type": {"type": "string"},
                    "evidence_text": {"type": "string"},
                    "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "weaknesses": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["agency_name", "presence_type", "confidence_score"],
                "additionalProperties": False
            }
        },
        "win_back_strategy": {"type": "string"},
        "protocol_advantages": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["competitors"],
    "additionalProperties": False
}

CONVERSATION_INTEL_SCHEMA = {
    "type": "object",
    "properties": {
        "ofsted_rating": {"type": "string"},
        "ofsted_date": {"type": "string"},
        "recent_achievements": {
            "type": "array",
            "items": {"type": "string"}
        },
        "upcoming_events": {
            "type": "array",
            "items": {"type": "string"}
        },
        "leadership_changes": {
            "type": "array",
            "items": {"type": "string"}
        },
        "conversation_starters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "topic": {"type": "string"},
                    "detail": {"type": "string"},
                    "relevance_score": {"type": "number", "minimum": 0, "maximum": 1}
                },
                "required": ["topic", "detail"],
                "additionalProperties": False
            }
        }
    },
    "required": ["conversation_starters"],
    "additionalProperties": False
}