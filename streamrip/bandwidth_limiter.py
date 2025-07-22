import asyncio
import time
from contextlib import asynccontextmanager
from typing import Optional


class GlobalBandwidthLimiter:
    """Global bandwidth limiter that shares bandwidth across multiple concurrent downloads."""
    
    def __init__(self, limit_mbps: float):
        self.limit_bytes_per_second = limit_mbps * 1024 * 1024 if limit_mbps > 0 else None
        self.active_downloads = set()
        self.lock = asyncio.Lock()
        self.last_reset = time.time()
        self.global_bytes_consumed = 0
        
    @asynccontextmanager
    async def limit_download(self):
        """Context manager to register a download."""
        download_id = id(asyncio.current_task())
        
        async with self.lock:
            self.active_downloads.add(download_id)
            
        try:
            yield DownloadRateLimiter(self, download_id)
        finally:
            async with self.lock:
                self.active_downloads.discard(download_id)
    
    async def consume_bytes(self, download_id: int, bytes_count: int):
        """Consume bytes from the global bandwidth pool."""
        if self.limit_bytes_per_second is None:
            return
            
        async with self.lock:
            now = time.time()
            
            # Reset global counter every second
            if now - self.last_reset >= 1.0:
                self.global_bytes_consumed = 0
                self.last_reset = now
            
            # Check if adding these bytes would exceed the global limit
            if self.global_bytes_consumed + bytes_count > self.limit_bytes_per_second:
                # Calculate how long to sleep to stay within limit
                time_to_next_second = 1.0 - (now - self.last_reset)
                if time_to_next_second > 0:
                    # Release lock during sleep to allow other downloads to check
                    pass
                else:
                    # Already past the second, reset immediately
                    self.global_bytes_consumed = 0
                    self.last_reset = now
            
            self.global_bytes_consumed += bytes_count
            
            # If we've exceeded the limit, sleep until next second
            if self.global_bytes_consumed > self.limit_bytes_per_second:
                sleep_time = 1.0 - (now - self.last_reset)
                if sleep_time > 0:
                    # Release lock during sleep
                    pass
        
        # Sleep outside the lock if needed
        if self.limit_bytes_per_second and self.global_bytes_consumed > self.limit_bytes_per_second:
            now = time.time()
            sleep_time = 1.0 - (now - self.last_reset)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)


class DownloadRateLimiter:
    """Rate limiter for individual downloads that uses the global bandwidth pool."""
    
    def __init__(self, global_limiter: GlobalBandwidthLimiter, download_id: int):
        self.global_limiter = global_limiter
        self.download_id = download_id
        
    async def consume(self, bytes_count: int):
        """Consume bytes through the global bandwidth limiter."""
        await self.global_limiter.consume_bytes(self.download_id, bytes_count)


# Global bandwidth limiter instance
_global_bandwidth_limiter: Optional[GlobalBandwidthLimiter] = None


def get_global_bandwidth_limiter(limit_mbps: float) -> Optional[GlobalBandwidthLimiter]:
    """Get or create the global bandwidth limiter."""
    global _global_bandwidth_limiter
    
    if limit_mbps <= 0:
        return None
        
    if _global_bandwidth_limiter is None:
        _global_bandwidth_limiter = GlobalBandwidthLimiter(limit_mbps)
    
    return _global_bandwidth_limiter
