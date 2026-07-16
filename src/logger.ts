export type Logger = {
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
};

const REDACTED = "[REDACTED]";
const secretField = /(?:authorization|token|credential|password|secret|cookie)/i;

function redactString(value: string): string {
  try {
    const url = new URL(value);
    url.username = "";
    url.password = "";
    if (url.search) {
      url.search = "";
    }
    return url.toString();
  } catch {
    // Not a URL; continue with header-style redaction below.
  }

  if (/^(?:authorization|cookie)\s*:/i.test(value)) {
    return REDACTED;
  }

  return value
    .replace(/\b(authorization|cookie)\s*[:=]\s*[^\s,;]+/gi, "$1: [REDACTED]")
    .replace(/\b(token|credential|password|secret)\s*=\s*[^\s&#,;]+/gi, "$1=[REDACTED]")
    .replace(/\b([a-z][a-z\d+.-]*:\/\/[^\s:@/]+):[^\s@/]+@/gi, "$1:[REDACTED]@");
}

function redact(value: unknown): unknown {
  if (typeof value === "string") {
    return redactString(value);
  }
  if (Array.isArray(value)) {
    return value.map(redact);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, secretField.test(key) ? REDACTED : redact(nested)]),
    );
  }
  return value;
}

export function createLogger(service: string): Logger {
  const write = (level: "info" | "warn" | "error", message: string, context?: Record<string, unknown>) => {
    const payload = {
      timestamp: new Date().toISOString(),
      level,
      severity: level,
      service,
      message,
      ...(redact(context) as Record<string, unknown> | undefined),
    };

    console[level](JSON.stringify(payload));
  };

  return {
    info: (message, context) => write("info", message, context),
    warn: (message, context) => write("warn", message, context),
    error: (message, context) => write("error", message, context),
  };
}
