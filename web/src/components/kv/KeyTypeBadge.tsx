/**
 * Infers the value type and renders a colored badge like Redis Insight.
 *
 * Types:
 *  JSON   → valid JSON object or array (cyan)
 *  STRING → plain text (green)
 *  NUMBER → numeric value (yellow)
 *  BINARY → non-printable / binary data (purple)
 */
import { cn } from "../../lib/utils";

export type ValueType = "JSON" | "STRING" | "NUMBER" | "BINARY";

const TYPE_STYLES: Record<ValueType, string> = {
    JSON:   "bg-cyan-500/20 text-cyan-400 border-cyan-500/30",
    STRING: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30",
    NUMBER: "bg-amber-500/20 text-amber-400 border-amber-500/30",
    BINARY: "bg-purple-500/20 text-purple-400 border-purple-500/30",
};

/** Infer a display-type from the raw value string */
export function inferValueType(value: string | object | undefined, _key?: string): ValueType {
    if (value === undefined || value === null) return "STRING";

    if (typeof value === "object") return "JSON";

    const str = String(value);

    // Numeric?
    if (str.length > 0 && !isNaN(Number(str)) && str.trim() !== "") return "NUMBER";

    // Try JSON parse
    try {
        const parsed = JSON.parse(str);
        if (typeof parsed === "object" && parsed !== null) return "JSON";
    } catch {
        // not json
    }

    // Binary detection: non-printable chars
    // eslint-disable-next-line no-control-regex
    if (/[\x00-\x08\x0E-\x1F\x7F]/.test(str)) return "BINARY";

    return "STRING";
}

interface KeyTypeBadgeProps {
    type: ValueType;
    className?: string;
    size?: "sm" | "md";
}

export function KeyTypeBadge({ type, className, size = "sm" }: KeyTypeBadgeProps) {
    return (
        <span
            className={cn(
                "inline-flex items-center justify-center font-bold uppercase border rounded",
                size === "sm"
                    ? "text-[9px] px-1.5 py-0.5 min-w-[42px]"
                    : "text-[10px] px-2 py-0.5 min-w-[50px]",
                TYPE_STYLES[type],
                className
            )}
        >
            {type}
        </span>
    );
}
