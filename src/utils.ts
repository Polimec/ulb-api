export const safeStringify = (data: unknown) =>
  JSON.stringify(data, (_key, value) => (typeof value === 'bigint' ? value.toString() : value));
