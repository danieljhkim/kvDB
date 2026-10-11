const READ_MODE_BY_CONSISTENCY = {
  STRONG: "READ_YOUR_WRITES",
  EVENTUAL: "LOW_LATENCY",
};

const SUPPORTED_DURABILITIES = new Set(["WAL_SYNC", "QUORUM_SYNC"]);

export function buildReadOptions(consistency, requestedReadMode) {
  const readMode = READ_MODE_BY_CONSISTENCY[consistency];
  if (!readMode) {
    throw new Error(`CONSISTENCY must be STRONG or EVENTUAL (got "${consistency}")`);
  }
  if (requestedReadMode !== undefined && requestedReadMode !== readMode) {
    throw new Error(`${requestedReadMode} is not valid for ${consistency} consistency`);
  }

  return {
    consistency,
    read_mode: readMode,
    max_staleness_ms: 0,
  };
}

export function validateBenchmarkOptions(consistency, durability, requestedReadMode) {
  const readOptions = buildReadOptions(consistency, requestedReadMode);
  if (!SUPPORTED_DURABILITIES.has(durability)) {
    throw new Error(
      `DURABILITY must be WAL_SYNC or QUORUM_SYNC; the gateway does not support "${durability}"`
    );
  }

  return readOptions;
}

export function buildGetPayload(ctx, key, consistency) {
  return {
    ctx,
    key,
    options: buildReadOptions(consistency),
    head_only: false,
  };
}
