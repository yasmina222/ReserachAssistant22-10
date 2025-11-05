"""
Protocol Education CI System - Cache Module (ASYNC VERSION)
Handles caching of intelligence data with async file operations
PHASE 1: Converted to async for non-blocking I/O
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import hashlib
import logging
import aiofiles
import asyncio

logger = logging.getLogger(__name__)

class IntelligenceCacheAsync:
    """Async cache system for school intelligence data"""
    
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
    
    async def get(self, school_name: str, data_type: str = 'full_intelligence') -> Optional[Dict[str, Any]]:
        """
        Retrieve cached data for a school - ASYNC version
        
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
            async with aiofiles.open(cache_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                cached_data = json.loads(content)
            
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
    
    async def set(self, school_name: str, data_type: str, data: Dict[str, Any], 
            sources: List[str] = None) -> bool:
        """
        Store data in cache - ASYNC version
        
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
            
            async with aiofiles.open(cache_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(cache_entry, indent=2, ensure_ascii=False))
            
            self.stats['writes'] += 1
            logger.info(f"💾 Cached data for {school_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing cache for {school_name}: {e}")
            return False
    
    async def invalidate(self, school_name: str, data_type: str = None) -> bool:
        """
        Remove cached data for a school - ASYNC version
        
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
                await asyncio.to_thread(cache_path.unlink)
                deleted = True
                logger.info(f"🗑️ Invalidated cache for {school_name} ({data_type})")
        else:
            # Delete all cache entries for this school
            tasks = []
            for cache_file in self.cache_dir.glob('*.json'):
                tasks.append(self._check_and_delete(cache_file, school_name))
            
            results = await asyncio.gather(*tasks)
            deleted = any(results)
        
        return deleted
    
    async def _check_and_delete(self, cache_file: Path, school_name: str) -> bool:
        """Helper to check and delete cache file"""
        try:
            async with aiofiles.open(cache_file, 'r') as f:
                content = await f.read()
                data = json.loads(content)
                if data.get('school_name', '').lower() == school_name.lower():
                    await asyncio.to_thread(cache_file.unlink)
                    logger.info(f"🗑️ Invalidated cache for {school_name}")
                    return True
        except:
            pass
        return False
    
    async def clear_expired(self) -> int:
        """
        Remove all expired cache entries - ASYNC version
        
        Returns:
            Number of entries deleted
        """
        deleted_count = 0
        current_time = datetime.now()
        
        tasks = []
        for cache_file in self.cache_dir.glob('*.json'):
            tasks.append(self._check_and_delete_expired(cache_file, current_time))
        
        results = await asyncio.gather(*tasks)
        deleted_count = sum(results)
        
        if deleted_count > 0:
            logger.info(f"🧹 Cleared {deleted_count} expired cache entries")
        
        return deleted_count
    
    async def _check_and_delete_expired(self, cache_file: Path, current_time: datetime) -> int:
        """Helper to check and delete expired cache file"""
        try:
            async with aiofiles.open(cache_file, 'r') as f:
                content = await f.read()
                data = json.loads(content)
            
            cached_time = datetime.fromisoformat(data['cached_at'])
            ttl = data.get('ttl_hours', self.ttl_hours)
            expiry_time = cached_time + timedelta(hours=ttl)
            
            if current_time > expiry_time:
                await asyncio.to_thread(cache_file.unlink)
                return 1
        except Exception as e:
            logger.error(f"Error checking cache file {cache_file}: {e}")
        
        return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics - synchronous since it's lightweight"""
        
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = self.stats['hits'] / total_requests if total_requests > 0 else 0
        
        # Calculate cache size
        cache_size_bytes = sum(
            f.stat().st_size for f in self.cache_dir.glob('*.json')
        )
        cache_size_mb = cache_size_bytes / (1024 * 1024)
        
        # Count active vs expired entries (sync for now, can be async if needed)
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
    
    async def clear_all(self) -> int:
        """
        Delete all cache entries - ASYNC version
        
        Returns:
            Number of entries deleted
        """
        deleted_count = 0
        
        tasks = []
        for cache_file in self.cache_dir.glob('*.json'):
            tasks.append(asyncio.to_thread(cache_file.unlink))
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            deleted_count = len(tasks)
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
        
        logger.info(f"🗑️ Cleared all cache ({deleted_count} entries)")
        return deleted_count


# Synchronous wrapper for backward compatibility
class IntelligenceCache:
    """Synchronous wrapper that maintains backward compatibility"""
    
    def __init__(self, cache_dir: str = 'cache', ttl_hours: int = 24):
        self.async_cache = IntelligenceCacheAsync(cache_dir, ttl_hours)
    
    def get(self, school_name: str, data_type: str = 'full_intelligence') -> Optional[Dict[str, Any]]:
        """Synchronous wrapper for get"""
        return asyncio.run(self.async_cache.get(school_name, data_type))
    
    def set(self, school_name: str, data_type: str, data: Dict[str, Any], 
            sources: List[str] = None) -> bool:
        """Synchronous wrapper for set"""
        return asyncio.run(self.async_cache.set(school_name, data_type, data, sources))
    
    def invalidate(self, school_name: str, data_type: str = None) -> bool:
        """Synchronous wrapper for invalidate"""
        return asyncio.run(self.async_cache.invalidate(school_name, data_type))
    
    def clear_expired(self) -> int:
        """Synchronous wrapper for clear_expired"""
        return asyncio.run(self.async_cache.clear_expired())
    
    def clear_all(self) -> int:
        """Synchronous wrapper for clear_all"""
        return asyncio.run(self.async_cache.clear_all())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get stats (already synchronous)"""
        return self.async_cache.get_stats()
    
    def disable(self):
        """Disable caching"""
        self.async_cache.disable()
    
    def enable(self):
        """Enable caching"""
        self.async_cache.enable()
    
    @property
    def enabled(self):
        return self.async_cache.enabled
    
    @property
    def ttl_hours(self):
        return self.async_cache.ttl_hours
