export async function runBoundedShutdown(
  graceful: () => Promise<void>,
  forced: (error: unknown) => Promise<void>,
  timeoutMs: number,
): Promise<{ forced: boolean }> {
  try {
    await withTimeout(graceful(), timeoutMs);
    return { forced: false };
  } catch (error) {
    await forced(error);
    return { forced: true };
  }
}

async function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`Shutdown timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}
