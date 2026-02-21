import { cn } from "../../lib/utils";

interface TTLHealthIndicatorProps {
    expiry?: number; // timestamp
    className?: string;
}

export function TTLHealthIndicator({ expiry, className }: TTLHealthIndicatorProps) {
    if (!expiry) {
        return (
            <div className={cn("h-0.5 w-full bg-surface-border/30 rounded-full overflow-hidden", className)}>
                <div className="h-full w-full bg-text-secondary/20" />
            </div>
        );
    }

    const now = Date.now();
    const totalDuration = 24 * 60 * 60 * 1000; // Assume 24h scale for visual if unknown start
    const remaining = Math.max(0, expiry - now);
    const percentage = Math.min(100, (remaining / totalDuration) * 100);

    let colorClass = "bg-primary"; // Green
    if (remaining <= 0) {
        colorClass = "bg-error animate-pulse"; // Red
    } else if (remaining < 60000 * 10) { // < 10 mins
        colorClass = "bg-warning"; // Yellow
    }

    return (
        <div className={cn("h-0.5 w-full bg-surface-border/30 rounded-full overflow-hidden", className)}>
            <div
                className={cn("h-full transition-all duration-500", colorClass)}
                style={{ width: `${percentage}%` }}
            />
        </div>
    );
}
