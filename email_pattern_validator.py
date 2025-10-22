"""
Protocol Education Research Assistant - Email Pattern Validator
Detects email patterns from known contacts and generates emails for others
"""

import re
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)

class EmailPatternValidator:
    """Validates and generates email addresses based on detected patterns"""
    
    def __init__(self):
        # Common UK school email patterns
        self.common_patterns = [
            '{firstname}.{lastname}@{domain}',
            '{f}.{lastname}@{domain}',
            '{firstname}{lastname}@{domain}',
            '{f}{lastname}@{domain}',
            '{lastname}{f}@{domain}',
            '{firstname}@{domain}',
            '{lastname}@{domain}',
            'admin@{domain}',
            'office@{domain}',
            'enquiries@{domain}'
        ]
        
    def detect_pattern(self, known_contacts: List[Dict[str, str]]) -> Optional[str]:
        """
        Detect email pattern from known email/name pairs
        
        Args:
            known_contacts: List of dicts with 'email', 'first_name', 'last_name'
            
        Returns:
            Most likely email pattern or None
        """
        if not known_contacts:
            return None
            
        pattern_scores = {}
        
        for contact in known_contacts:
            email = contact.get('email', '').lower()
            first = contact.get('first_name', '').lower()
            last = contact.get('last_name', '').lower()
            
            if not email or '@' not in email or not first or not last:
                continue
                
            local, domain = email.split('@')
            
            # Test each pattern
            for pattern in self.common_patterns:
                generated = self._generate_email(pattern, first, last, domain)
                if generated == email:
                    pattern_scores[pattern] = pattern_scores.get(pattern, 0) + 1
                    logger.info(f"Pattern match: {pattern} for {email}")
        
        # Return the most common pattern
        if pattern_scores:
            best_pattern = max(pattern_scores.items(), key=lambda x: x[1])[0]
            logger.info(f"Detected pattern: {best_pattern}")
            return best_pattern
            
        return None
    
    def validate_and_generate(self, contact_name: str, known_pattern: Optional[str], 
                            domain: str, known_email: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate known email or generate new one based on pattern
        
        Args:
            contact_name: Full name of the contact
            known_pattern: Detected email pattern
            domain: School domain
            known_email: Existing email to validate (optional)
            
        Returns:
            Dict with email, confidence score, and method
        """
        # Parse name
        name_parts = contact_name.strip().split()
        if len(name_parts) < 2:
            return {
                'email': f'info@{domain}',
                'confidence': 0.3,
                'method': 'fallback',
                'pattern': None
            }
            
        first_name = name_parts[0]
        last_name = name_parts[-1]
        
        # If we have a known email, validate it
        if known_email:
            if self._is_valid_email(known_email):
                return {
                    'email': known_email,
                    'confidence': 1.0,
                    'method': 'verified',
                    'pattern': None
                }
        
        # Generate email based on pattern
        if known_pattern:
            generated = self._generate_email(known_pattern, first_name, last_name, domain)
            return {
                'email': generated,
                'confidence': 0.85,
                'method': 'pattern_match',
                'pattern': known_pattern
            }
        
        # Try common patterns
        for pattern in self.common_patterns[:3]:  # Top 3 most common
            generated = self._generate_email(pattern, first_name, last_name, domain)
            if self._looks_reasonable(generated):
                return {
                    'email': generated,
                    'confidence': 0.6,
                    'method': 'common_pattern',
                    'pattern': pattern
                }
        
        # Fallback
        return {
            'email': f'{first_name.lower()}.{last_name.lower()}@{domain}',
            'confidence': 0.5,
            'method': 'best_guess',
            'pattern': '{firstname}.{lastname}@{domain}'
        }
    
    def _generate_email(self, pattern: str, first_name: str, last_name: str, domain: str) -> str:
        """Generate email from pattern template"""
        
        # Clean names
        first_name = re.sub(r'[^a-zA-Z]', '', first_name).lower()
        last_name = re.sub(r'[^a-zA-Z]', '', last_name).lower()
        
        replacements = {
            '{firstname}': first_name,
            '{lastname}': last_name,
            '{f}': first_name[0] if first_name else '',
            '{l}': last_name[0] if last_name else '',
            '{domain}': domain
        }
        
        email = pattern
        for key, value in replacements.items():
            email = email.replace(key, value)
            
        return email
    
    def _is_valid_email(self, email: str) -> bool:
        """Check if email format is valid"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def _looks_reasonable(self, email: str) -> bool:
        """Check if generated email looks reasonable"""
        if not self._is_valid_email(email):
            return False
            
        local, domain = email.split('@')
        
        # Check local part
        if len(local) < 2 or len(local) > 30:
            return False
            
        # No double dots
        if '..' in local:
            return False
            
        return True
    
    def extract_domain_from_website(self, website_url: str) -> str:
        """Extract email domain from website URL"""
        
        # Remove protocol
        domain = website_url.lower()
        domain = domain.replace('https://', '').replace('http://', '')
        domain = domain.replace('www.', '')
        
        # Remove path
        domain = domain.split('/')[0]
        
        # Common UK school domain patterns
        if '.sch.uk' in domain:
            return domain
        elif '.academy' in domain:
            return domain
        elif '.school' in domain:
            return domain
        else:
            # For other domains, might need to modify
            return domain


# Integration function for the premium processor
def enhance_contacts_with_emails(contacts: List, website_url: str, 
                               known_emails: List[Dict] = None) -> List:
    """
    Enhance contact list with generated emails
    
    Args:
        contacts: List of Contact objects
        website_url: School website
        known_emails: List of known email/name pairs for pattern detection
        
    Returns:
        Enhanced contact list
    """
    validator = EmailPatternValidator()
    
    # Extract domain from website
    domain = validator.extract_domain_from_website(website_url)
    
    # Detect pattern from known emails if available
    pattern = None
    if known_emails:
        pattern = validator.detect_pattern(known_emails)
        logger.info(f"Detected email pattern: {pattern}")
    
    # Enhance each contact
    for contact in contacts:
        if not contact.email or contact.email == 'Not found':
            # Generate email
            result = validator.validate_and_generate(
                contact.full_name,
                pattern,
                domain,
                contact.email if contact.email != 'Not found' else None
            )
            
            contact.email = result['email']
            contact.notes = f"Email {result['method']}: {result['pattern']}" if result['pattern'] else f"Email {result['method']}"
            
            # Adjust confidence based on email generation confidence
            contact.confidence_score = min(contact.confidence_score, result['confidence'])
            
            logger.info(f"Generated email for {contact.full_name}: {contact.email} (confidence: {result['confidence']})")
    
    return contacts


# Test function
if __name__ == "__main__":
    # Test pattern detection
    validator = EmailPatternValidator()
    
    # Example known contacts
    known = [
        {'email': 'john.smith@school.sch.uk', 'first_name': 'John', 'last_name': 'Smith'},
        {'email': 'jane.doe@school.sch.uk', 'first_name': 'Jane', 'last_name': 'Doe'},
    ]
    
    pattern = validator.detect_pattern(known)
    print(f"Detected pattern: {pattern}")
    
    # Test email generation
    result = validator.validate_and_generate(
        "Robert Johnson",
        pattern,
        "school.sch.uk"
    )
    print(f"Generated: {result}")