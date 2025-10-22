"""
Protocol Education CI System - Verification Module
Handles email validation, phone normalization, and pattern testing
"""

import re
import socket
import smtplib
import dns.resolver
import phonenumbers
from typing import Tuple, Optional, List, Dict
from email.utils import parseaddr
import logging

logger = logging.getLogger(__name__)

class ContactVerifier:
    """Verifies and normalizes contact information"""
    
    def __init__(self):
        self.email_regex = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        self.uk_phone_regex = re.compile(
            r'^(?:(?:\+44\s?|0)(?:1\d{3}|20|11[3-8]|1[2-9][0-9]{2}|[2-9]\d{2})\s?\d{3}\s?\d{3,4})$'
        )
        
    def smtp_ping(self, email: str, timeout: int = 5) -> Tuple[bool, float]:
        """
        Verify email via SMTP without sending
        Returns: (is_valid, confidence_score)
        """
        try:
            # Basic format check
            if not self.email_regex.match(email):
                return False, 0.0
                
            # Extract domain
            _, domain = email.split('@')
            
            # Get MX records
            mx_records = []
            try:
                mx_answers = dns.resolver.resolve(domain, 'MX')
                mx_records = sorted(
                    [(mx.preference, str(mx.exchange).rstrip('.')) 
                     for mx in mx_answers]
                )
            except Exception as e:
                logger.debug(f"MX lookup failed for {domain}: {e}")
                return False, 0.2
            
            if not mx_records:
                return False, 0.2
                
            # Try SMTP verification on primary MX
            mx_host = mx_records[0][1]
            
            try:
                # Connect to SMTP server
                smtp = smtplib.SMTP(timeout=timeout)
                smtp.connect(mx_host)
                smtp.helo('verify.protocoleducation.com')
                
                # Check if email exists
                code, _ = smtp.mail('verify@protocoleducation.com')
                if code != 250:
                    smtp.quit()
                    return False, 0.3
                    
                code, _ = smtp.rcpt(email)
                smtp.quit()
                
                if code == 250:
                    return True, 0.95  # Email exists
                elif code == 550:
                    return False, 0.9  # Email definitely doesn't exist
                else:
                    return True, 0.7  # Server accepted but uncertain
                    
            except Exception as e:
                logger.debug(f"SMTP verification failed: {e}")
                # If SMTP fails but MX exists, moderate confidence
                return True, 0.6
                
        except Exception as e:
            logger.error(f"Email verification error: {e}")
            return False, 0.0
    
    def normalize_phone(self, phone_raw: str, region: str = 'GB') -> Tuple[Optional[str], float]:
        """
        Normalize UK phone numbers to E.164 format
        Returns: (normalized_number, confidence_score)
        """
        try:
            # Remove common separators and spaces
            cleaned = re.sub(r'[\s\-\(\)\.]+', '', phone_raw)
            
            # Handle extensions
            extension = None
            ext_match = re.search(r'(?:ext|x|extension)\.?\s*(\d+)', cleaned, re.I)
            if ext_match:
                extension = ext_match.group(1)
                cleaned = cleaned[:ext_match.start()]
            
            # Parse phone number
            parsed = phonenumbers.parse(cleaned, region)
            
            if not phonenumbers.is_valid_number(parsed):
                return None, 0.0
                
            # Format to E.164
            formatted = phonenumbers.format_number(
                parsed, 
                phonenumbers.PhoneNumberFormat.E164
            )
            
            # Add extension back if present
            if extension:
                formatted += f' x{extension}'
                
            # Confidence based on number type
            number_type = phonenumbers.number_type(parsed)
            if number_type == phonenumbers.PhoneNumberType.FIXED_LINE:
                confidence = 0.95  # Landlines are good for schools
            elif number_type == phonenumbers.PhoneNumberType.MOBILE:
                confidence = 0.7  # Mobile less likely for main contact
            else:
                confidence = 0.8
                
            return formatted, confidence
            
        except Exception as e:
            logger.debug(f"Phone normalization failed: {e}")
            # Try basic UK pattern matching
            if self.uk_phone_regex.match(cleaned):
                return cleaned, 0.5
            return None, 0.0
    
    def pattern_tester(self, template: str, first_name: str, 
                      last_name: str, domain: str) -> str:
        """
        Generate email from pattern template
        """
        replacements = {
            '{firstname}': first_name.lower(),
            '{lastname}': last_name.lower(),
            '{f}': first_name[0].lower() if first_name else '',
            '{l}': last_name[0].lower() if last_name else '',
            '{domain}': domain
        }
        
        email = template
        for key, value in replacements.items():
            email = email.replace(key, value)
            
        # Clean up any spaces or special chars
        email = re.sub(r'[^a-zA-Z0-9@._-]', '', email)
        
        return email
    
    def detect_email_pattern(self, known_emails: List[Dict[str, str]]) -> Optional[str]:
        """
        Detect email pattern from known email/name pairs
        Input: [{'email': 'j.smith@school.org', 'first': 'John', 'last': 'Smith'}, ...]
        """
        if not known_emails:
            return None
            
        patterns = {
            '{firstname}.{lastname}@{domain}': 0,
            '{f}.{lastname}@{domain}': 0,
            '{firstname}{lastname}@{domain}': 0,
            '{f}{lastname}@{domain}': 0,
            '{firstname}@{domain}': 0,
            '{lastname}@{domain}': 0
        }
        
        for entry in known_emails:
            email = entry.get('email', '').lower()
            first = entry.get('first', '').lower()
            last = entry.get('last', '').lower()
            
            if not email or not '@' in email:
                continue
                
            local, domain = email.split('@')
            
            # Check each pattern
            if local == f"{first}.{last}":
                patterns['{firstname}.{lastname}@{domain}'] += 1
            elif local == f"{first[0]}.{last}" and first:
                patterns['{f}.{lastname}@{domain}'] += 1
            elif local == f"{first}{last}":
                patterns['{firstname}{lastname}@{domain}'] += 1
            elif local == f"{first[0]}{last}" and first:
                patterns['{f}{lastname}@{domain}'] += 1
            elif local == first:
                patterns['{firstname}@{domain}'] += 1
            elif local == last:
                patterns['{lastname}@{domain}'] += 1
        
        # Return most common pattern
        best_pattern = max(patterns.items(), key=lambda x: x[1])
        if best_pattern[1] > 0:
            return best_pattern[0]
            
        return None
    
    def calculate_confidence(self, data_points: Dict[str, float]) -> float:
        """
        Calculate overall confidence score from multiple signals
        """
        if not data_points:
            return 0.0
            
        # Weighted average based on importance
        weights = {
            'smtp_valid': 0.3,
            'phone_valid': 0.2,
            'pattern_match': 0.2,
            'source_quality': 0.2,
            'recency': 0.1
        }
        
        total_weight = 0
        total_score = 0
        
        for key, value in data_points.items():
            if key in weights:
                total_score += value * weights[key]
                total_weight += weights[key]
                
        return total_score / total_weight if total_weight > 0 else 0.0