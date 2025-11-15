interface CachedQuote {
  quote: any; // Jupiter quote response
  timestamp: number;
  inputMint: string;
  outputMint: string;
  amount: number;
}

class QuoteCache {
  private cache = new Map<string, CachedQuote>();
  private readonly TTL = 20000; // 20 seconds validity
  private hits = 0;
  private misses = 0;

  // Generate cache key
  private getCacheKey(inputMint: string, outputMint: string, amount: number): string {
    // Round amount to avoid cache misses from tiny differences
    const roundedAmount = Math.floor(amount / 1000000) * 1000000; // Round to nearest 0.001 SOL
    return `${inputMint}-${outputMint}-${roundedAmount}`;
  }

  // Get cached quote if valid
  get(inputMint: string, outputMint: string, amount: number): any | null {
    const key = this.getCacheKey(inputMint, outputMint, amount);
    const cached = this.cache.get(key);

    if (!cached) {
      this.misses++;
      return null;
    }

    const age = Date.now() - cached.timestamp;
    
    if (age > this.TTL) {
      // Expired
      this.cache.delete(key);
      this.misses++;
      return null;
    }

    this.hits++;
    console.log(`[Cache HIT] Age: ${(age / 1000).toFixed(1)}s, Hit rate: ${this.getHitRate()}%`);
    return cached.quote;
  }

  // Store quote in cache
  set(inputMint: string, outputMint: string, amount: number, quote: any): void {
    const key = this.getCacheKey(inputMint, outputMint, amount);
    
    this.cache.set(key, {
      quote,
      timestamp: Date.now(),
      inputMint,
      outputMint,
      amount
    });

    console.log(`[Cache SET] Key: ${key}, Total cached: ${this.cache.size}`);
  }

  // Clear expired entries
  cleanup(): void {
    const now = Date.now();
    let removed = 0;

    for (const [key, cached] of this.cache.entries()) {
      if (now - cached.timestamp > this.TTL) {
        this.cache.delete(key);
        removed++;
      }
    }

    if (removed > 0) {
      console.log(`[Cache] Cleaned up ${removed} expired entries`);
    }
  }

  // Get statistics
  getStats(): { hits: number; misses: number; hitRate: number; size: number } {
    return {
      hits: this.hits,
      misses: this.misses,
      hitRate: this.getHitRate(),
      size: this.cache.size
    };
  }

  private getHitRate(): number {
    const total = this.hits + this.misses;
    return total > 0 ? Math.round((this.hits / total) * 100) : 0;
  }

  // Clear all cache
  clear(): void {
    this.cache.clear();
    this.hits = 0;
    this.misses = 0;
    console.log('[Cache] Cleared all entries');
  }

  // Reset stats
  resetStats(): void {
    this.hits = 0;
    this.misses = 0;
  }
}

export const quoteCache = new QuoteCache();

// Auto cleanup every 30 seconds
setInterval(() => {
  quoteCache.cleanup();
}, 30000);