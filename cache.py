"""Cache module for Protocol Education CI System"""

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
        self.stats = {'hits': 0, 'misses': 0, 'writes': 0}
        
    def _get_cache_key(self, school_name: str, data_type: str) -> str:
        combined = f"{school_name.lower()}_{data_type}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.json"
    
    def get(self, school_name: str, data_type: str = 'full_intelligence') -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        cache_key = self._get_cache_key(school_name, data_type)
        cache_path = self._get_cache_path(cache_key)
        if not cache_path.exists():
            self.stats['misses'] += 1
            return None
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            cached_time = datetime.fromisoformat(cached_data['cached_at'])
            expiry_time = cached_time + timedelta(hours=self.ttl_hours)
            if datetime.now() > expiry_time:
                self.stats['misses'] += 1
                return None
            self.stats['hits'] += 1
            return cached_data
        except:
            self.stats['misses'] += 1
            return None
    
    def set(self, school_name: str, data_type: str, data: Dict[str, Any], sources: List[str] = None) -> bool:
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
            return True
        except:
            return False
    
    def invalidate(self, school_name: str, data_type: str = None) -> bool:
        deleted = False
        if data_type:
            cache_key = self._get_cache_key(school_name, data_type)
            cache_path = self._get_cache_path(cache_key)
            if cache_path.exists():
                cache_path.unlink()
                deleted = True
        else:
            for cache_file in self.cache_dir.glob('*.json'):
                try:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                        if data.get('school_name', '').lower() == school_name.lower():
                            cache_file.unlink()
                            deleted = True
                except:
                    pass
        return deleted
    
    def clear_expired(self) -> int:
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
            except:
                pass
        return deleted_count
    
    def get_stats(self) -> Dict[str, Any]:
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = self.stats['hits'] / total_requests if total_requests > 0 else 0
        cache_size_bytes = sum(f.stat().st_size for f in self.cache_dir.glob('*.json'))
        cache_size_mb = cache_size_bytes / (1024 * 1024)
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
        self.enabled = False
    
    def enable(self):
        self.enabled = True
    
    def clear_all(self) -> int:
        deleted_count = 0
        for cache_file in self.cache_dir.glob('*.json'):
            try:
                cache_file.unlink()
                deleted_count += 1
            except:
                pass
        return deleted_count
