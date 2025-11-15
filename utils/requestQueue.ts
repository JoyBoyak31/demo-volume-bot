import { sleep } from './utils';

class RequestQueue {
  private queue: Array<() => Promise<any>> = [];
  private processing = false;
  private requestsPerSecond = 4; // Safe rate: 4 requests per second (tested at 5 req/sec)
  private minDelay = 1000 / this.requestsPerSecond; // ~333ms between requests
  private lastRequestTime = 0;
  private activeRequests = 0;
  private maxConcurrent = 1; // Process one at a time

  async add<T>(fn: () => Promise<T>, priority: 'high' | 'normal' = 'normal'): Promise<T> {
    return new Promise((resolve, reject) => {
      const wrappedFn = async () => {
        try {
          const result = await fn();
          resolve(result);
        } catch (error) {
          reject(error);
        }
      };

      // High priority requests go to front of queue (sells during cooldown)
      if (priority === 'high') {
        this.queue.unshift(wrappedFn);
      } else {
        this.queue.push(wrappedFn);
      }
      
      if (!this.processing) {
        this.process();
      }
    });
  }

  private async process() {
    if (this.queue.length === 0) {
      this.processing = false;
      return;
    }

    this.processing = true;

    // Wait if we're at max concurrent requests
    while (this.activeRequests >= this.maxConcurrent) {
      await sleep(100);
    }

    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequestTime;
    
    // Enforce minimum delay between requests
    if (timeSinceLastRequest < this.minDelay) {
      await sleep(this.minDelay - timeSinceLastRequest);
    }

    const fn = this.queue.shift();
    if (fn) {
      this.activeRequests++;
      this.lastRequestTime = Date.now();
      
      fn().finally(() => {
        this.activeRequests--;
      });
    }

    // Continue processing queue
    setTimeout(() => this.process(), 10);
  }

  getQueueLength(): number {
    return this.queue.length;
  }

  getActiveRequests(): number {
    return this.activeRequests;
  }

  // Adjust rate limit dynamically
  setRequestsPerSecond(rate: number) {
    this.requestsPerSecond = Math.max(1, Math.min(rate, 10)); // Between 1-10 req/s
    this.minDelay = 1000 / this.requestsPerSecond;
    console.log(`Queue rate adjusted to ${this.requestsPerSecond} req/s (${this.minDelay}ms delay)`);
  }

  clearQueue() {
    this.queue = [];
    console.log('Request queue cleared');
  }
}

export const jupiterQueue = new RequestQueue();