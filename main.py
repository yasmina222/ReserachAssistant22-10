"""
Protocol Education CI System - Main CLI Application
Command-line interface for running the intelligence system
"""

import argparse
import logging
import sys
from datetime import datetime
import json
from pathlib import Path

from processor import SchoolIntelligenceProcessor
from exporter import IntelligenceExporter
from cache import IntelligenceCache
from config import LOG_LEVEL, LOG_FILE

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class ProtocolCLI:
    """Command-line interface for the Protocol CI System"""
    
    def __init__(self):
        self.processor = SchoolIntelligenceProcessor()
        self.exporter = IntelligenceExporter()
        self.cache = IntelligenceCache()
    
    def run_single_school(self, school_name: str, website: str = None,
                         format: str = 'xlsx', force_refresh: bool = False):
        """Process a single school"""
        
        print(f"\n🎯 Processing: {school_name}")
        print("=" * 50)
        
        # Process school
        intel = self.processor.process_single_school(
            school_name, website, force_refresh
        )
        
        # Display summary
        self._display_summary(intel)
        
        # Export results
        filepath = self.exporter.export_single_school(intel, format)
        print(f"\n✅ Results exported to: {filepath}")
        
        # Show API usage
        usage = self.processor.ai_engine.get_usage_stats()
        print(f"\n📊 API Usage: ${usage['cost_usd']:.2f} ({usage['tokens']} tokens)")
    
    def run_borough_sweep(self, borough_name: str, school_type: str = 'all',
                         format: str = 'xlsx'):
        """Process all schools in a borough"""
        
        print(f"\n🏙️  Borough Sweep: {borough_name}")
        print(f"📚 School Type: {school_type}")
        print("=" * 50)
        
        # Process borough
        results = self.processor.process_borough(borough_name, school_type)
        
        print(f"\n✅ Processed {len(results)} schools")
        
        # Display summary stats
        high_quality = sum(1 for r in results if r.data_quality_score > 0.7)
        with_contacts = sum(1 for r in results if r.contacts)
        with_competitors = sum(1 for r in results if r.competitors)
        
        print(f"\n📈 Summary Statistics:")
        print(f"  • High Quality Data: {high_quality}/{len(results)}")
        print(f"  • With Contacts: {with_contacts}/{len(results)}")
        print(f"  • With Competitors: {with_competitors}/{len(results)}")
        
        # Export results
        filepath = self.exporter.export_borough_results(results, borough_name, format)
        print(f"\n✅ Results exported to: {filepath}")
        
        # Show API usage
        usage = self.processor.ai_engine.get_usage_stats()
        print(f"\n📊 API Usage: ${usage['cost_usd']:.2f} ({usage['tokens']} tokens)")
    
    def show_cache_stats(self):
        """Display cache statistics"""
        
        stats = self.cache.get_stats()
        
        print("\n📦 Cache Statistics")
        print("=" * 50)
        print(f"Status: {'Enabled' if stats['enabled'] else 'Disabled'}")
        
        if stats['enabled']:
            print(f"Total Entries: {stats['total_entries']}")
            print(f"Active Entries: {stats['active_entries']}")
            print(f"Expired Entries: {stats['expired_entries']}")
            print(f"Hit Rate: {stats['hit_rate']:.1%}")
            print(f"Cache Size: {stats['cache_size_mb']} MB")
    
    def clear_cache(self):
        """Clear expired cache entries"""
        
        print("\n🧹 Clearing expired cache entries...")
        self.cache.clear_expired()
        print("✅ Cache cleaned")
    
    def _display_summary(self, intel):
        """Display intelligence summary"""
        
        print(f"\n📋 School: {intel.school_name}")
        print(f"🌐 Website: {intel.website}")
        print(f"⭐ Data Quality: {intel.data_quality_score:.0%}")
        
        # Contacts
        print(f"\n👥 Contacts Found: {len(intel.contacts)}")
        for contact in intel.contacts:
            print(f"  • {contact.role.value.replace('_', ' ').title()}: {contact.full_name}")
            if contact.email:
                print(f"    📧 {contact.email} (confidence: {contact.confidence_score:.0%})")
            if contact.phone:
                print(f"    📞 {contact.phone}")
        
        # Competitors
        if intel.competitors:
            print(f"\n🏢 Competitors: {len(intel.competitors)}")
            for comp in intel.competitors[:3]:
                print(f"  • {comp.agency_name} ({comp.presence_type})")
        
        # Intelligence
        if intel.ofsted_rating:
            print(f"\n🎓 Ofsted: {intel.ofsted_rating}")
        
        if intel.conversation_starters:
            print(f"\n💬 Top Conversation Starters:")
            for starter in intel.conversation_starters[:3]:
                print(f"  • {starter.topic}: {starter.detail[:80]}...")

def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(
        description='Protocol Education Competitive Intelligence System'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Single school command
    school_parser = subparsers.add_parser('school', help='Process a single school')
    school_parser.add_argument('name', help='School name')
    school_parser.add_argument('--website', help='School website URL')
    school_parser.add_argument('--format', choices=['csv', 'xlsx', 'json'], 
                              default='xlsx', help='Export format')
    school_parser.add_argument('--force', action='store_true', 
                              help='Force refresh (ignore cache)')
    
    # Borough sweep command
    borough_parser = subparsers.add_parser('borough', help='Process entire borough')
    borough_parser.add_argument('name', help='Borough name')
    borough_parser.add_argument('--type', choices=['all', 'primary', 'secondary'],
                               default='all', help='School type filter')
    borough_parser.add_argument('--format', choices=['csv', 'xlsx', 'json'],
                               default='xlsx', help='Export format')
    
    # Cache commands
    cache_parser = subparsers.add_parser('cache', help='Cache operations')
    cache_parser.add_argument('action', choices=['stats', 'clear'])
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize CLI
    cli = ProtocolCLI()
    
    try:
        if args.command == 'school':
            cli.run_single_school(
                args.name, 
                args.website,
                args.format,
                args.force
            )
        
        elif args.command == 'borough':
            cli.run_borough_sweep(
                args.name,
                args.type,
                args.format
            )
        
        elif args.command == 'cache':
            if args.action == 'stats':
                cli.show_cache_stats()
            elif args.action == 'clear':
                cli.clear_cache()
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()