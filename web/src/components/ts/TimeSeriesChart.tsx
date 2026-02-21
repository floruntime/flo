import { useMemo } from "react";
import {
    ResponsiveContainer,
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
} from "recharts";
import type { TsSeriesData } from "../../lib/ts-types";

// =============================================================================
// Color Palette for Series Lines
// =============================================================================

export const SERIES_COLORS = [
    "var(--color-primary, #6366f1)",
    "#10b981", // emerald
    "#f59e0b", // amber
    "#ef4444", // red
    "#8b5cf6", // violet
    "#06b6d4", // cyan
    "#ec4899", // pink
    "#14b8a6", // teal
    "#f97316", // orange
    "#84cc16", // lime
];

// =============================================================================
// Time Formatter
// =============================================================================

function formatTime(ms: number): string {
    const d = new Date(ms);
    const hours = d.getHours().toString().padStart(2, "0");
    const mins = d.getMinutes().toString().padStart(2, "0");
    return `${hours}:${mins}`;
}

function formatFullTime(ms: number): string {
    return new Date(ms).toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
    });
}

// =============================================================================
// Tooltip
// =============================================================================

interface TooltipProps {
    active?: boolean;
    payload?: Array<{ name: string; value: number; color: string }>;
    label?: number;
}

/** Max tooltip entries before truncating with "+N more" */
const MAX_TOOLTIP_ENTRIES = 8;

function ChartTooltip({ active, payload, label }: TooltipProps) {
    if (!active || !payload || payload.length === 0) return null;

    // Sort by value descending, cap entries
    const sorted = [...payload]
        .filter((e) => e.value != null)
        .sort((a, b) => (b.value ?? 0) - (a.value ?? 0));
    const visible = sorted.slice(0, MAX_TOOLTIP_ENTRIES);
    const remaining = sorted.length - visible.length;

    return (
        <div className="rounded-lg border border-surface-border bg-surface shadow-lg px-3 py-2 text-xs max-h-[280px] overflow-hidden">
            <p className="text-text-secondary mb-1.5 font-medium">
                {label != null ? formatFullTime(label) : ""}
            </p>
            {visible.map((entry, i) => (
                <div key={i} className="flex items-center justify-between gap-4">
                    <div className="flex items-center gap-1.5">
                        <span
                            className="inline-block w-2.5 h-2.5 rounded-sm"
                            style={{ backgroundColor: entry.color }}
                        />
                        <span className="text-text-secondary truncate max-w-[180px]">{entry.name}</span>
                    </div>
                    <span className="font-mono text-text-primary tabular-nums">
                        {typeof entry.value === "number" ? entry.value.toFixed(2) : "—"}
                    </span>
                </div>
            ))}
            {remaining > 0 && (
                <p className="text-text-secondary mt-1">+{remaining} more series</p>
            )}
        </div>
    );
}

// =============================================================================
// Chart Component
// =============================================================================

/** Above this count, legend is hidden to save space */
const LEGEND_THRESHOLD = 10;

interface TimeSeriesChartProps {
    /** Pre-filtered series to render */
    series: TsSeriesData[];
    height?: number;
    /** Optional stable color map (series key → CSS color). When provided, colors
     *  come from this map so they stay consistent even when the visible subset changes. */
    colorMap?: Record<string, string>;
}

/**
 * Time-series line chart built with Recharts.
 *
 * Takes the unified TsDataResponse from the `/data` endpoint and renders
 * one line per series, with time on the X axis and aggregated values on Y.
 *
 * Uses CSS custom properties for theme-aware colors.
 */
export function TimeSeriesChart({ series, height = 320, colorMap }: TimeSeriesChartProps) {
    // Merge series into a single recharts-compatible dataset.
    const { chartData, seriesKeys } = useMemo(() => {
        if (!series || series.length === 0) {
            return { chartData: [], seriesKeys: [] };
        }

        const keys = series.map((s) => s.key);

        // Build a map: timestamp -> { t, [key]: value }
        const map = new Map<number, Record<string, number | null>>();

        for (const s of series) {
            for (const pt of s.points) {
                let row = map.get(pt.t);
                if (!row) {
                    row = { t: pt.t } as Record<string, number | null>;
                    map.set(pt.t, row);
                }
                (row as Record<string, number | null>)[s.key] = pt.v;
            }
        }

        // Sort by timestamp
        const sorted = Array.from(map.values()).sort(
            (a, b) => (a.t as number) - (b.t as number)
        );

        return { chartData: sorted, seriesKeys: keys };
    }, [series]);

    const showLegend = seriesKeys.length > 1 && seriesKeys.length <= LEGEND_THRESHOLD;

    if (chartData.length === 0) {
        return (
            <div
                className="flex items-center justify-center border border-dashed border-surface-border rounded-lg text-sm text-text-secondary"
                style={{ height }}
            >
                No data points in this time range
            </div>
        );
    }

    // Shorten series key for legend/tooltip (remove measurement prefix if present)
    const shortKey = (key: string) => {
        const comma = key.indexOf(",");
        return comma >= 0 ? key.substring(comma + 1) : key;
    };

    return (
        <ResponsiveContainer width="100%" height={height}>
            <LineChart data={chartData} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="var(--color-surface-border, #374151)"
                    opacity={0.5}
                />
                <XAxis
                    dataKey="t"
                    type="number"
                    domain={["dataMin", "dataMax"]}
                    tickFormatter={formatTime}
                    tick={{ fontSize: 11, fill: "var(--color-text-secondary, #9ca3af)" }}
                    stroke="var(--color-surface-border, #374151)"
                    scale="time"
                />
                <YAxis
                    tick={{ fontSize: 11, fill: "var(--color-text-secondary, #9ca3af)" }}
                    stroke="var(--color-surface-border, #374151)"
                    width={60}
                    tickFormatter={(v: number) =>
                        v >= 1_000_000 ? `${(v / 1_000_000).toFixed(1)}M` :
                        v >= 1_000 ? `${(v / 1_000).toFixed(1)}K` :
                        v.toFixed(1)
                    }
                />
                <Tooltip content={<ChartTooltip />} />
                {showLegend && (
                    <Legend
                        formatter={(value: string) => (
                            <span className="text-xs text-text-secondary">{shortKey(value)}</span>
                        )}
                        iconSize={10}
                    />
                )}
                {seriesKeys.map((key, i) => (
                    <Line
                        key={key}
                        type="monotone"
                        dataKey={key}
                        name={shortKey(key)}
                        stroke={colorMap?.[key] ?? SERIES_COLORS[i % SERIES_COLORS.length]}
                        strokeWidth={1.5}
                        dot={false}
                        activeDot={{ r: 3, strokeWidth: 0 }}
                        connectNulls={false}
                    />
                ))}
            </LineChart>
        </ResponsiveContainer>
    );
}

// =============================================================================
// Utility Exports
// =============================================================================

/** Shorten series key by removing measurement prefix */
export function shortSeriesKey(key: string): string {
    const comma = key.indexOf(",");
    return comma >= 0 ? key.substring(comma + 1) : key;
}

/** Default max series for initial selection */
export const DEFAULT_MAX_SERIES = 20;

