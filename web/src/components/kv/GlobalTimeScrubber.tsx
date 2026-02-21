import { cn } from "../../lib/utils";
import { Rewind, FastForward } from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { createPortal } from "react-dom";

interface GlobalTimeScrubberProps {
    currentLsn: number;
    viewLsn?: number;
    onSeek: (lsn: number | undefined) => void;
    className?: string;
}

interface TooltipData {
    lsn: number;
    x: number;
    y: number;
}

export function GlobalTimeScrubber({ currentLsn, viewLsn, onSeek, className }: GlobalTimeScrubberProps) {
    const isHistory = viewLsn !== undefined && viewLsn < currentLsn;
    const [isDragging, setIsDragging] = useState(false);
    const [hoveredTooltip, setHoveredTooltip] = useState<TooltipData | null>(null);
    const seekerRef = useRef<HTMLDivElement>(null);

    // Handle dragging
    useEffect(() => {
        if (!isDragging) return;

        const handleMouseMove = (e: MouseEvent) => {
            if (!seekerRef.current) return;
            const rect = seekerRef.current.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const percent = Math.max(0, Math.min(1, mouseX / rect.width));

            // Calculate LSN range (show last 1000 LSNs for navigation)
            const minLsn = Math.max(0, currentLsn - 1000);
            const newLsn = Math.floor(minLsn + percent * 1000);

            onSeek(newLsn === currentLsn ? undefined : newLsn);
        };

        const handleMouseUp = () => {
            setIsDragging(false);
        };

        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('mouseup', handleMouseUp);

        return () => {
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
        };
    }, [isDragging, currentLsn, onSeek]);

    // Time travel function
    const timeTravel = (delta: number) => {
        const current = viewLsn ?? currentLsn;
        const newLsn = Math.max(0, Math.min(currentLsn, current + delta));
        onSeek(newLsn === currentLsn ? undefined : newLsn);
    };

    // Calculate position percentage
    const getPosition = (lsn: number) => {
        const minLsn = Math.max(0, currentLsn - 1000);
        return ((lsn - minLsn) / 1000) * 100;
    };

    const currentPos = getPosition(viewLsn ?? currentLsn);

    return (
        <div className={cn(
            "fixed bottom-0 left-0 right-0 h-14 border-t bg-surface z-50 flex items-center px-6 gap-4 transition-all duration-300",
            isHistory ? "border-kv-history bg-kv-history/5" : "border-surface-border",
            className
        )}>
            {/* Status Indicator */}
            <div className="flex items-center gap-2 min-w-[140px]">
                {isHistory ? (
                    <div className="flex items-center gap-2 text-kv-history">
                        <Rewind className="w-4 h-4 animate-pulse" />
                        <div className="flex flex-col">
                            <span className="text-[10px] font-bold uppercase tracking-wide">History Mode</span>
                            <span className="text-xs font-mono">LSN {viewLsn}</span>
                        </div>
                    </div>
                ) : (
                    <div className="flex items-center gap-2 text-kv-current">
                        <div className="w-2 h-2 rounded-full bg-kv-current animate-pulse" />
                        <div className="flex flex-col">
                            <span className="text-[10px] font-bold uppercase tracking-wide">Live</span>
                            <span className="text-xs font-mono">LSN {currentLsn}</span>
                        </div>
                    </div>
                )}
            </div>

            {/* Time Travel Controls */}
            <div className="flex items-center gap-2">
                <button
                    onClick={() => timeTravel(-200)}
                    className="px-2.5 py-1.5 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors text-[10px] font-mono"
                    disabled={!viewLsn && currentLsn < 200}
                >
                    −200
                </button>
                <button
                    onClick={() => timeTravel(-50)}
                    className="px-2.5 py-1.5 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors text-[10px] font-mono"
                    disabled={!viewLsn && currentLsn < 50}
                >
                    −50
                </button>
            </div>

            {/* Seeker Bar */}
            <div className="flex-1 relative h-full flex items-center">
                <div
                    ref={seekerRef}
                    className="relative h-3 bg-surface-border/30 w-full cursor-pointer rounded-sm overflow-visible"
                    onClick={(e) => {
                        if (isDragging) return;
                        const rect = e.currentTarget.getBoundingClientRect();
                        const clickX = e.clientX - rect.left;
                        const percent = clickX / rect.width;
                        const minLsn = Math.max(0, currentLsn - 1000);
                        const targetLsn = Math.floor(minLsn + percent * 1000);
                        onSeek(targetLsn === currentLsn ? undefined : targetLsn);
                    }}
                >
                    {/* Background gradient */}
                    <div className={cn(
                        "absolute inset-0 rounded-sm",
                        isHistory
                            ? "bg-gradient-to-r from-kv-history/20 to-kv-history/5"
                            : "bg-gradient-to-r from-surface-border/50 to-surface-border/20"
                    )} />

                    {/* Progress fill */}
                    <div
                        className={cn(
                            "absolute top-0 bottom-0 left-0 rounded-sm transition-all",
                            isHistory ? "bg-kv-history/30" : "bg-kv-current/30"
                        )}
                        style={{ width: `${currentPos}%` }}
                    />

                    {/* Current position marker */}
                    <div
                        className={cn(
                            "absolute top-1/2 -translate-y-1/2 w-1 h-full transition-all z-20",
                            isHistory ? "bg-kv-history" : "bg-kv-current"
                        )}
                        style={{
                            left: `${currentPos}%`,
                            boxShadow: isHistory
                                ? '0 0 12px rgba(139, 92, 246, 0.6)'
                                : '0 0 12px rgba(34, 197, 94, 0.6)'
                        }}
                        onMouseEnter={(e) => {
                            const rect = e.currentTarget.getBoundingClientRect();
                            setHoveredTooltip({
                                lsn: viewLsn ?? currentLsn,
                                x: rect.left + rect.width / 2,
                                y: rect.top
                            });
                        }}
                        onMouseLeave={() => setHoveredTooltip(null)}
                    />

                    {/* Draggable handle */}
                    <div
                        className={cn(
                            "absolute top-1/2 -translate-y-1/2 w-4 h-6 rounded cursor-ew-resize transition-all z-30",
                            isHistory
                                ? "bg-kv-history hover:bg-kv-history/90"
                                : "bg-kv-current hover:bg-kv-current/90"
                        )}
                        style={{
                            left: `${currentPos}%`,
                            transform: 'translate(-50%, -50%)',
                            boxShadow: isHistory
                                ? '0 2px 8px rgba(139, 92, 246, 0.4)'
                                : '0 2px 8px rgba(34, 197, 94, 0.4)'
                        }}
                        onMouseDown={(e) => {
                            e.stopPropagation();
                            setIsDragging(true);
                        }}
                    />

                    {/* HEAD marker at the end */}
                    <div
                        className="absolute top-1/2 -translate-y-1/2 right-0 w-1 h-full bg-primary z-10"
                        style={{ boxShadow: '0 0 8px rgba(62, 207, 142, 0.6)' }}
                        onMouseEnter={(e) => {
                            const rect = e.currentTarget.getBoundingClientRect();
                            setHoveredTooltip({
                                lsn: currentLsn,
                                x: rect.left,
                                y: rect.top
                            });
                        }}
                        onMouseLeave={() => setHoveredTooltip(null)}
                    >
                        <div className="absolute -top-6 right-0 bg-primary text-background text-[9px] font-bold px-1.5 py-0.5 rounded shadow-lg whitespace-nowrap">
                            HEAD
                        </div>
                    </div>
                </div>
            </div>

            {/* Time Travel Controls (Right) */}
            <div className="flex items-center gap-2">
                <button
                    onClick={() => timeTravel(50)}
                    className="px-2.5 py-1.5 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors text-[10px] font-mono"
                >
                    +50
                </button>
                <button
                    onClick={() => timeTravel(200)}
                    className="px-2.5 py-1.5 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors text-[10px] font-mono"
                >
                    +200
                </button>
            </div>

            {/* Go Live Button */}
            {isHistory && (
                <button
                    onClick={() => onSeek(undefined)}
                    className="flex items-center gap-2 px-3 py-1.5 bg-kv-current text-background text-xs font-bold rounded-md hover:bg-kv-current/90 transition-colors shadow-sm"
                >
                    <FastForward className="w-3 h-3" />
                    GO LIVE
                </button>
            )}

            {/* Tooltip Portal */}
            {hoveredTooltip && createPortal(
                <div
                    className="fixed pointer-events-none z-[10000]"
                    style={{
                        left: `${hoveredTooltip.x}px`,
                        top: `${hoveredTooltip.y - 8}px`,
                        transform: 'translate(-50%, -100%)'
                    }}
                >
                    <div className="px-3 py-2 bg-background border border-surface-border rounded-lg shadow-2xl whitespace-nowrap">
                        <div className="text-xs font-bold text-text-primary">LSN {hoveredTooltip.lsn}</div>
                        <div className="text-[10px] text-text-secondary">
                            {new Date().toLocaleString('en-GB', {
                                hour: '2-digit',
                                minute: '2-digit',
                                second: '2-digit',
                                day: '2-digit',
                                month: 'short'
                            })}
                        </div>
                        {/* Arrow */}
                        <div
                            className="absolute top-full left-1/2 -translate-x-1/2 w-0 h-0"
                            style={{
                                borderLeft: '6px solid transparent',
                                borderRight: '6px solid transparent',
                                borderTop: '6px solid var(--surface-border)'
                            }}
                        />
                        <div
                            className="absolute top-full left-1/2 -translate-x-1/2 w-0 h-0"
                            style={{
                                borderLeft: '5px solid transparent',
                                borderRight: '5px solid transparent',
                                borderTop: '5px solid var(--background)',
                                marginTop: '-1px'
                            }}
                        />
                    </div>
                </div>,
                document.body
            )}
        </div>
    );
}
