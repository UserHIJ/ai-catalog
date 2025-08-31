export function jsonSafe<T>(data: T): T {
  // Deep-convert BigInt to string so JSON.stringify works
  return JSON.parse(
    JSON.stringify(data, (_k, v) => (typeof v === "bigint" ? v.toString() : v))
  );
}
