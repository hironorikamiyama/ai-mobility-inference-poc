const s = db.serverStatus();

print(JSON.stringify({
  timestamp: new Date().toISOString(),
  connections: {
    current: s.connections.current,
    active: s.connections.active,
    totalCreated: s.connections.totalCreated,
    rejected: s.connections.rejected
  },
  operations: {
    update: Number(s.opcounters.update),
    query: Number(s.opcounters.query)
  },
  lockQueue: s.globalLock.currentQueue,
  wiredTigerCache: s.wiredTiger ? {
    maximumBytes: s.wiredTiger.cache["maximum bytes configured"],
    currentBytes: s.wiredTiger.cache["bytes currently in the cache"],
    dirtyBytes: s.wiredTiger.cache["tracked dirty bytes in the cache"]
  } : null
}));
