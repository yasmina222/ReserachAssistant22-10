"""
Protocol Education CI System - Cache Module
Handles caching of intelligence data to reduce API costs and improve performance
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import hashlib
import logging

logger = logging.getLogger(__name__)

class IntelligenceCache:
    """Cache system for school intelligence data"""
    
    def __init__(self, cache_dir: str = 'cache', ttl_hours: int = 24):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl_hours = ttl_hours
        self.enabled = True
        
        # Stats tracking
        self.stats = {
            'hits': 0,
            'misses': 0,
            'writes': 0
        }
        
    def _get_cache_key(self, school_name: str, data_type: str) -> str:
        """Generate cache key from school name and data type"""
        combined = f"{school_name.lower()}_{data_type}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Get file path for cache key"""
        return self.cache_dir / f"{cache_key}.json"
    
    def get(self, school_name: str, data_type: str = 'full_intelligence') -> Optional[Dict[str, Any]]:
        """
        Retrieve cached data for a school
        
        Args:
            school_name: Name of the school
            data_type: Type of data ('full_intelligence', 'contacts', 'financial', etc.)
            
        Returns:
            Cached data dict or None if not found/expired
        """
        if not self.enabled:
            return None
            
        cache_key = self._get_cache_key(school_name, data_type)
        cache_path = self._get_cache_path(cache_key)
        
        if not cache_path.exists():
            self.stats['misses'] += 1
            logger.debug(f"Cache miss for {school_name}")
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            
            # Check if expired
            cached_time = datetime.fromisoformat(cached_data['cached_at'])
            expiry_time = cached_time + timedelta(hours=self.ttl_hours)
            
            if datetime.now() > expiry_time:
                logger.debug(f"Cache expired for {school_name}")
                self.stats['misses'] += 1
                return None
            
            self.stats['hits'] += 1
            logger.info(f"✅ Cache hit for {school_name}")
            return cached_data
            
        except Exception as e:
            logger.error(f"Error reading cache for {school_name}: {e}")
            self.stats['misses'] += 1
            return None
    
    def set(self, school_name: str, data_type: str, data: Dict[str, Any], 
            sources: List[str] = None) -> bool:
        """
        Store data in cache
        
        Args:
            school_name: Name of the school
            data_type: Type of data being cached
            data: Data to cache
            sources: List of source URLs used
            
        Returns:
            True if successful
        """
        if not self.enabled:
            return False
            
        cache_key = self._get_cache_key(school_name, data_type)
        cache_path = self._get_cache_path(cache_key)
        
        try:
            cache_entry = {
                'school_name': school_name,
                'data_type': data_type,
                'data': data,
                'sources': sources or [],
                'cached_at': datetime.now().isoformat(),
                'ttl_hours': self.ttl_hours
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_entry, f, indent=2, ensure_ascii=False)
            
            self.stats['writes'] += 1
            logger.info(f"💾 Cached data for {school_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing cache for {school_name}: {e}")
            return False
    
    def invalidate(self, school_name: str, data_type: str = None) -> bool:
        """
        Remove cached data for a school
        
        Args:
            school_name: Name of school
            data_type: Specific data type to invalidate, or None for all
            
        Returns:
            True if anything was deleted
        """
        deleted = False
        
        if data_type:
            # Delete specific cache entry
            cache_key = self._get_cache_key(school_name, data_type)
            cache_path = self._get_cache_path(cache_key)
            
            if cache_path.exists():
                cache_path.unlink()
                deleted = True
                logger.info(f"🗑️ Invalidated cache for {school_name} ({data_type})")
        else:
            # Delete all cache entries for this school
            for cache_file in self.cache_dir.glob('*.json'):
                try:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                        if data.get('school_name', '').lower() == school_name.lower():
                            cache_file.unlink()
                            deleted = True
                            logger.info(f"🗑️ Invalidated cache for {school_name}")
                except:
                    pass
        
        return deleted
    
    def clear_expired(self) -> int:
        """
        Remove all expired cache entries
        
        Returns:
            Number of entries deleted
        """
        deleted_count = 0
        current_time = datetime.now()
        
        for cache_file in self.cache_dir.glob('*.json'):
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                
                cached_time = datetime.fromisoformat(data['cached_at'])
                ttl = data.get('ttl_hours', self.ttl_hours)
                expiry_time = cached_time + timedelta(hours=ttl)
                
                if current_time > expiry_time:
                    cache_file.unlink()
                    deleted_count += 1
                    
            except Exception as e:
                logger.error(f"Error checking cache file {cache_file}: {e}")
        
        if deleted_count > 0:
            logger.info(f"🧹 Cleared {deleted_count} expired cache entries")
        
        return deleted_count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = self.stats['hits'] / total_requests if total_requests > 0 else 0
        
        # Calculate cache size
        cache_size_bytes = sum(
            f.stat().st_size for f in self.cache_dir.glob('*.json')
        )
        cache_size_mb = cache_size_bytes / (1024 * 1024)
        
        # Count active vs expired entries
        active_entries = 0
        expired_entries = 0
        current_time = datetime.now()
        
        for cache_file in self.cache_dir.glob('*.json'):
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                
                cached_time = datetime.fromisoformat(data['cached_at'])
                ttl = data.get('ttl_hours', self.ttl_hours)
                expiry_time = cached_time + timedelta(hours=ttl)
                
                if current_time > expiry_time:
                    expired_entries += 1
                else:
                    active_entries += 1
                    
            except:
                pass
        
        return {
            'enabled': self.enabled,
            'total_entries': active_entries + expired_entries,
            'active_entries': active_entries,
            'expired_entries': expired_entries,
            'cache_size_mb': round(cache_size_mb, 2),
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'writes': self.stats['writes'],
            'hit_rate': hit_rate,
            'ttl_hours': self.ttl_hours
        }
    
    def disable(self):
        """Disable caching"""
        self.enabled = False
        logger.info("Cache disabled")
    
    def enable(self):
        """Enable caching"""
        self.enabled = True
        logger.info("Cache enabled")
    
    def clear_all(self) -> int:
        """
        Delete all cache entries
        
        Returns:
            Number of entries deleted
        """
        deleted_count = 0
        
        for cache_file in self.cache_dir.glob('*.json'):
            try:
                cache_file.unlink()
                deleted_count += 1
            except Exception as e:
                logger.error(f"Error deleting {cache_file}: {e}")
        
        logger.info(f"🗑️ Cleared all cache ({deleted_count} entries)")

        return deleted_count

