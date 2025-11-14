// requestTracker.js
export const tracker = {
  rpc: { total: 0, byMethod: {} },
  jupiter: { total: 0, byType: {} },

  logRpc(method) {
    this.rpc.total++;
    this.rpc.byMethod[method] = (this.rpc.byMethod[method] || 0) + 1;
  },

  logJup(type) {
    this.jupiter.total++;
    this.jupiter.byType[type] = (this.jupiter.byType[type] || 0) + 1;
  },

  report() {
    console.log("\n=========================");
    console.log("📊 REQUEST USAGE REPORT");
    console.log("=========================");
    console.log("🔹 Solana RPC Calls:", this.rpc.total);
    console.log(this.rpc.byMethod);
    console.log("🔹 Jupiter API Calls:", this.jupiter.total);
    console.log(this.jupiter.byType);
    console.log("=========================\n");
  }
};
