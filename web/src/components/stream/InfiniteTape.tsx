import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";
import type { StreamMessage, ConsumerGroup } from "../../lib/types";
import { useRef, useEffect, useState } from "react";
import { cn } from "../../lib/utils";
import { createPortal } from "react-dom";
import { DataViewer } from "./DataViewer";

interface InfiniteTapeProps {
    messages: StreamMessage[];
    groups: ConsumerGroup[];
    onSeekerRangeChange?: (range: { start: number; end: number }) => void;
}

interface TooltipData {
    name: string;
    seq: number;
    lag: number;
    x: number;
    y: number;
}

export function InfiniteTape({ messages, groups, onSeekerRangeChange }: InfiniteTapeProps) {

    const virtuosoRef = useRef<VirtuosoHandle>(null);
    const [currentRange, setCurrentRange] = useState({ start: 0, end: 0 });
    const [focusedGroupName, setFocusedGroupName] = useState<string | null>(groups[0]?.name || null);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [seekerRange, setSeekerRange] = useState({ start: 0, end: messages.length - 1 });
    const [isDragging, setIsDragging] = useState<'start' | 'end' | null>(null);
    const [hoveredTooltip, setHoveredTooltip] = useState<TooltipData | null>(null);
    const [isPanning, setIsPanning] = useState(false);
    const [selectedMessage, setSelectedMessage] = useState<StreamMessage | null>(null);
    const [dragStart, setDragStart] = useState<{ x: number; scrollLeft: number } | null>(null);
    const tapeRef = useRef<HTMLDivElement>(null);
    const dropdownButtonRef = useRef<HTMLButtonElement>(null);
    const [dropdownPosition, setDropdownPosition] = useState<{ top: number; right: number } | null>(null);


    // Update dropdown position when opened
    useEffect(() => {
        if (isDropdownOpen && dropdownButtonRef.current) {
            const rect = dropdownButtonRef.current.getBoundingClientRect();
            setDropdownPosition({
                top: rect.bottom + 4,
                right: window.innerWidth - rect.right
            });
        }
    }, [isDropdownOpen]);

    // Sync seekerRange when messages load or change length
    // This fixes the race where messages is empty on first render,
    // leaving seekerRange stuck at {start: 0, end: -1}
    useEffect(() => {
        if (messages.length > 0 && !isDragging) {
            setSeekerRange(prev => {
                // Only update if the range looks uninitialized or messages changed
                if (prev.end < 0 || prev.end >= messages.length) {
                    return { start: 0, end: messages.length - 1 };
                }
                return prev;
            });
        }
    }, [messages.length, isDragging]);

    // Notify parent of seeker range changes
    useEffect(() => {
        onSeekerRangeChange?.(seekerRange);
    }, [seekerRange, onSeekerRangeChange]);

    // Initialize to show a middle range that includes most consumer groups
    useEffect(() => {
        const timer = setTimeout(() => {
            if (groups && groups.length > 0) {
                const groupSeqs = groups.map(g => g.current_seq).sort((a, b) => a - b);
                const medianSeq = groupSeqs[Math.floor(groupSeqs.length / 2)];
                const targetIndex = messages.findIndex(m => m.seq >= medianSeq);

                if (targetIndex !== -1) {
                    virtuosoRef.current?.scrollToIndex({ index: targetIndex, align: "center" });
                }
            } else {
                // If no groups, scroll to the end to show latest messages
                virtuosoRef.current?.scrollToIndex({ index: messages.length - 1, align: "end" });
            }
        }, 100);
        return () => clearTimeout(timer);
    }, [messages, groups]);

    // Handle dragging for trim handles
    useEffect(() => {
        if (!isDragging) return;

        const handleMouseMove = (e: MouseEvent) => {
            const seekerBar = document.querySelector('.seeker-bar') as HTMLElement;
            if (!seekerBar) return;

            const rect = seekerBar.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const percent = Math.max(0, Math.min(1, mouseX / rect.width));
            const newIndex = Math.floor(percent * messages.length);

            if (isDragging === 'start') {
                setSeekerRange(prev => ({
                    start: Math.min(newIndex, prev.end - 10),
                    end: prev.end
                }));
            } else if (isDragging === 'end') {
                setSeekerRange(prev => ({
                    start: prev.start,
                    end: Math.max(newIndex, prev.start + 10)
                }));
            }
        };

        const handleMouseUp = () => {
            setIsDragging(null);
            // Scroll to show the trimmed range
            if (isDragging === 'start') {
                virtuosoRef.current?.scrollToIndex({ index: seekerRange.start, align: "start", behavior: "smooth" });
            } else if (isDragging === 'end') {
                virtuosoRef.current?.scrollToIndex({ index: seekerRange.end, align: "end", behavior: "smooth" });
            }
        };

        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('mouseup', handleMouseUp);

        return () => {
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
        };
    }, [isDragging, messages.length, seekerRange]);


    // Time travel function
    const timeTravel = (direction: 'past' | 'future', amount: number) => {
        const currentIndex = Math.floor((currentRange.start + currentRange.end) / 2);
        const newIndex = direction === 'past'
            ? Math.max(0, currentIndex - amount)
            : Math.min(messages.length - 1, currentIndex + amount);

        virtuosoRef.current?.scrollToIndex({ index: newIndex, align: "center", behavior: "smooth" });
    };

    // Jump to a specific consumer group
    const jumpToGroup = (groupName: string) => {
        const group = groups.find(g => g.name === groupName);
        if (group) {
            const msgIndex = messages.findIndex(m => m.seq === group.current_seq);
            if (msgIndex !== -1) {
                virtuosoRef.current?.scrollToIndex({ index: msgIndex, align: "center", behavior: "smooth" });
            }
            setFocusedGroupName(groupName);
            setIsDropdownOpen(false);
        }
    };

    // Get health status based on lag
    const getHealthGlow = (lag: number): string => {
        if (lag > 1000) return '0 0 16px rgba(239, 68, 68, 0.6)'; // Red glow
        if (lag > 100) return '0 0 16px rgba(245, 158, 11, 0.6)'; // Yellow glow
        return 'none'; // No glow for healthy
    };

    // Sort groups by lag (worst first)
    const sortedGroups = groups ? [...groups].sort((a, b) => b.lag - a.lag) : [];
    const focusedGroup = groups ? groups.find(g => g.name === focusedGroupName) : null;

    // Calculate seeker positions
    const getSeekerPosition = (seq: number) => {
        if (messages.length === 0) return 0;
        const minSeq = messages[0].seq;
        const maxSeq = messages[messages.length - 1].seq;
        if (maxSeq === minSeq) return 50;
        return ((seq - minSeq) / (maxSeq - minSeq)) * 100;
    };

    return (
        <div className="h-96 bg-surface border border-surface-border rounded-lg flex flex-col overflow-hidden relative">
            {/* Header with Focus Group Selector */}
            <div className="h-10 bg-surface-hover/50 border-b border-surface-border flex items-center px-4 justify-between text-xs text-text-secondary relative">
                {/* Stats on the left */}
                <div className="flex items-center gap-3 text-[10px] text-text-secondary/80 font-mono">
                    <span>{Math.round(messages.slice(Math.max(0, seekerRange.start), seekerRange.end + 1).reduce((acc, m) => acc + m.size, 0) / 1024)} KB</span>
                    <span>Entries: {Math.max(0, seekerRange.end - seekerRange.start + 1)}</span>
                </div>

                {/* Focus Group Dropdown - only show if groups exist */}
                {groups && groups.length > 0 && (
                    <div className="relative">
                        <button
                            ref={dropdownButtonRef}
                            onClick={() => setIsDropdownOpen(!isDropdownOpen)}
                            className="flex items-center gap-2 px-3 py-1.5 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors"
                        >
                            <span className="text-xs font-medium text-text-primary">Focus Group:</span>
                            {focusedGroup && (
                                <>
                                    <div className="w-2 h-2 rounded-full" style={{ backgroundColor: focusedGroup.color }} />
                                    <span className="text-xs font-bold max-w-[120px] truncate" style={{ color: focusedGroup.color }}>{focusedGroup.name}</span>
                                </>
                            )}
                            <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                            </svg>
                        </button>
                    </div>
                )}
            </div>

            {/* The Tape Track */}
            <div
                ref={tapeRef}
                className="flex-1 relative bg-background overflow-hidden"
                style={{ cursor: isPanning ? 'grabbing' : 'grab' }}
                onMouseDown={(e) => {
                    // Only start panning if not clicking on interactive elements
                    if ((e.target as HTMLElement).closest('button, a, [role="button"]')) return;

                    setIsPanning(true);
                    setDragStart({
                        x: e.clientX,
                        scrollLeft: currentRange.start
                    });
                }}
                onMouseMove={(e) => {
                    if (!isPanning || !dragStart) return;

                    // Calculate drag delta and convert to message index movement
                    const deltaX = dragStart.x - e.clientX;
                    const sensitivity = 0.5; // Adjust this to control drag speed
                    const indexDelta = Math.round(deltaX * sensitivity);

                    if (Math.abs(indexDelta) > 0) {
                        const newIndex = Math.max(0, Math.min(messages.length - 1, dragStart.scrollLeft + indexDelta));
                        virtuosoRef.current?.scrollToIndex({ index: newIndex, align: "center" });
                    }
                }}
                onMouseUp={() => {
                    setIsPanning(false);
                    setDragStart(null);
                }}
                onMouseLeave={() => {
                    setIsPanning(false);
                    setDragStart(null);
                }}
            >
                {/* Time Range Overlay - minimal text only, toggles on hover */}
                <div className="absolute bottom-2 left-1/2 -translate-x-1/2 z-30 group/timerange">
                    {/* Default view: Timestamps */}
                    <div className="text-[10px] text-text-secondary/80 font-mono text-center whitespace-nowrap group-hover/timerange:hidden">
                        {new Date(messages[seekerRange.start]?.timestamp || Date.now()).toLocaleString('en-GB', {
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit',
                            day: '2-digit',
                            month: 'short',
                            year: 'numeric'
                        })} → {new Date(messages[seekerRange.end]?.timestamp || Date.now()).toLocaleString('en-GB', {
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit',
                            day: '2-digit',
                            month: 'short',
                            year: 'numeric'
                        })} ({Math.round(messages.slice(seekerRange.start, seekerRange.end + 1).reduce((acc, m) => acc + m.size, 0) / 1024)}KB • {seekerRange.end - seekerRange.start + 1} Entries)
                    </div>

                    {/* Hover view: StreamID range */}
                    <div className="hidden group-hover/timerange:block text-[10px] text-text-secondary/80 font-mono text-center whitespace-nowrap">
                        {messages[seekerRange.start]?.id} → {messages[seekerRange.end]?.id} ({Math.round(messages.slice(Math.max(0, seekerRange.start), seekerRange.end + 1).reduce((acc, m) => acc + m.size, 0) / 1024)}KB • {Math.max(0, seekerRange.end - seekerRange.start + 1)} Entries)
                    </div>
                </div>

                <Virtuoso
                    ref={virtuosoRef}
                    totalCount={messages.length}
                    horizontalDirection
                    className="h-full w-full no-scrollbar"
                    style={{ overflowY: "hidden" }}
                    rangeChanged={(range) => setCurrentRange({ start: range.startIndex, end: range.endIndex })}
                    itemContent={(index) => {
                        const msg = messages[index];
                        const activeGroups = groups.filter(g => g.current_seq === msg.seq);

                        const width = Math.max(50, Math.min(150, msg.size / 4));

                        const waveformBars = Array.from({ length: 8 }, (_, i) => {
                            const seed = (msg.seq * 7 + i * 13) % 100;
                            return Math.max(20, seed % 80);
                        });

                        return (
                            <div
                                className="h-full py-6 px-[1px] flex flex-col justify-center relative group"
                                style={{ width: `${width}px` }}
                            >
                                <div className="absolute bottom-0 left-1/2 -translate-x-1/2 h-2 w-[1px] bg-surface-border opacity-50" />

                                <div
                                    className={cn(
                                        "w-full h-40 rounded-[2px] border border-white/5 transition-all duration-200 cursor-pointer",
                                        "bg-gradient-to-br from-surface-hover/60 to-surface-hover/40",
                                        "hover:border-primary/50 hover:shadow-[0_0_20px_rgba(16,185,129,0.2)] hover:scale-[1.02]",
                                        "flex flex-col items-center justify-between p-1.5 overflow-hidden relative"
                                    )}
                                    title={`Id: ${msg.id}\nPartition: ${msg.key}\nSize: ${msg.size}b`}
                                    onClick={() => setSelectedMessage(msg)}
                                >
                                    <span className="text-[9px] text-text-secondary font-mono font-bold opacity-60 z-10">
                                        {msg.seq}
                                    </span>

                                    <div className="flex-1 w-full flex items-end justify-around gap-[1px] px-1 opacity-30">
                                        {waveformBars.map((height, i) => (
                                            <div
                                                key={i}
                                                className="w-full bg-gradient-to-t from-primary/60 to-primary/20 rounded-t-[1px]"
                                                style={{ height: `${height}%` }}
                                            />
                                        ))}
                                    </div>

                                    <span className="text-[8px] text-text-secondary font-mono opacity-50 z-10">
                                        {msg.size}b
                                    </span>
                                </div>

                                {/* Ghost Cursors with Health Glow */}
                                {activeGroups
                                    .filter(g => !focusedGroupName || g.name === focusedGroupName)
                                    .map((g) => (
                                        <div
                                            key={g.name}
                                            className="absolute top-0 bottom-0 left-1/2 w-[3px] z-30 flex flex-col items-center"
                                            style={{ transform: 'translateX(-50%)' }}
                                        >
                                            <div
                                                className="w-full h-full opacity-90"
                                                style={{
                                                    backgroundColor: g.color,
                                                    boxShadow: getHealthGlow(g.lag)
                                                }}
                                            />

                                            <div
                                                className="absolute top-2 whitespace-nowrap text-[9px] font-bold px-2 py-1 rounded-md shadow-lg z-40 pointer-events-none"
                                                style={{
                                                    backgroundColor: g.color,
                                                    color: '#000',
                                                    boxShadow: `0 2px 8px ${g.color}60`
                                                }}
                                            >
                                                {g.name}
                                                <div
                                                    className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent"
                                                    style={{ borderTopColor: g.color }}
                                                />
                                            </div>
                                        </div>
                                    ))}
                            </div>
                        );
                    }}
                    components={{
                        Footer: () => (
                            <div className="h-full flex flex-col justify-center items-center px-6 relative">
                                <div
                                    className="h-full w-[3px] bg-primary opacity-90"
                                    style={{ boxShadow: `0 0 12px #10B98160` }}
                                />

                                <div className="absolute top-2 bg-primary text-black text-[10px] font-bold px-2 py-1 rounded-md shadow-lg whitespace-nowrap">
                                    HEAD
                                    <div className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-primary" />
                                </div>

                                <div className="absolute bottom-0 left-1/2 -translate-x-1/2 h-2 w-[1px] bg-primary opacity-70" />
                            </div>
                        )
                    }}
                />
            </div>

            {/* Timeline Range Seeker */}
            <div className="h-8 bg-surface-hover/30 border-t border-surface-border px-4 py-1.5 relative flex items-center">
                <div
                    className="seeker-bar relative h-2.5 bg-surface-border/30 overflow-visible cursor-pointer w-full"
                    onClick={(e) => {
                        // Click to jump to position
                        if (isDragging) return;
                        const rect = e.currentTarget.getBoundingClientRect();
                        const clickX = e.clientX - rect.left;
                        const percent = clickX / rect.width;
                        const targetIndex = Math.floor(percent * messages.length);
                        virtuosoRef.current?.scrollToIndex({ index: targetIndex, align: "center", behavior: "smooth" });
                    }}
                >
                    {/* Full range background */}
                    <div className="absolute inset-0 bg-gradient-to-r from-surface-border/50 to-surface-border/20" />
                    {/* Consumer group positions as interactive indicators */}
                    {groups && groups.length > 0 && groups.map(g => {
                        const pos = getSeekerPosition(g.current_seq);
                        return (
                            <div
                                key={g.name}
                                className="absolute top-1/2 -translate-y-1/2 h-full opacity-60 hover:opacity-100 transition-all cursor-pointer z-10 group"
                                style={{
                                    left: `${pos}%`,
                                    backgroundColor: g.color,
                                    boxShadow: getHealthGlow(g.lag),
                                    width: '2px'
                                }}
                                onClick={(e) => {
                                    e.stopPropagation();
                                    jumpToGroup(g.name);
                                }}
                                onMouseEnter={(e) => {
                                    const rect = e.currentTarget.getBoundingClientRect();
                                    setHoveredTooltip({
                                        name: g.name,
                                        seq: g.current_seq,
                                        lag: g.lag,
                                        x: rect.left + rect.width / 2,
                                        y: rect.top
                                    });
                                }}
                                onMouseLeave={() => setHoveredTooltip(null)}
                            >
                                {/* Width expander on hover */}
                                <div
                                    className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-all"
                                    style={{
                                        backgroundColor: g.color,
                                        width: '4px',
                                        left: '-1px'
                                    }}
                                />
                            </div>
                        );
                    })}

                    {/* Selected/trimmed range highlight */}
                    <div
                        className="absolute top-0 bottom-0 bg-primary/20 border-primary pointer-events-none"
                        style={{
                            left: `${(seekerRange.start / messages.length) * 100}%`,
                            right: `${100 - (seekerRange.end / messages.length) * 100}%`,
                            borderLeftWidth: '2px',
                            borderRightWidth: '2px'
                        }}
                    />

                    {/* Start trim handle (on bottom) */}
                    <div
                        className="absolute w-2 h-4 bg-primary cursor-ew-resize hover:bg-primary/90 transition-colors z-20"
                        style={{
                            left: `${(seekerRange.start / messages.length) * 100}%`,
                            bottom: 0,
                            transform: 'translate(-50%, 0)'
                        }}
                        onMouseDown={(e) => {
                            e.stopPropagation();
                            setIsDragging('start');
                        }}
                        title={`Start: ${new Date(messages[seekerRange.start]?.timestamp || Date.now()).toLocaleString('en-GB', {
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit',
                            day: '2-digit',
                            month: 'short',
                            year: 'numeric'
                        })}`}
                    />

                    {/* End trim handle (on top) */}
                    <div
                        className="absolute w-2 h-4 bg-primary cursor-ew-resize hover:bg-primary/90 transition-colors z-20"
                        style={{
                            left: `${(seekerRange.end / messages.length) * 100}%`,
                            top: 0,
                            transform: 'translate(-50%, 0)'
                        }}
                        onMouseDown={(e) => {
                            e.stopPropagation();
                            setIsDragging('end');
                        }}
                        title={`End: ${new Date(messages[seekerRange.end]?.timestamp || Date.now()).toLocaleString('en-GB', {
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit',
                            day: '2-digit',
                            month: 'short',
                            year: 'numeric'
                        })}`}
                    />
                </div>
            </div>

            {/* Bottom Navigation Bar with Time Travel Controls */}
            <div className="h-9 bg-surface-hover/30 border-t border-surface-border flex items-center justify-center px-4 text-[10px] text-text-secondary font-mono relative gap-2">
                {/* Time Travel Controls */}
                <button
                    onClick={() => timeTravel('past', 200)}
                    className="px-2.5 py-1 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors"
                >
                    −200
                </button>

                <button
                    onClick={() => timeTravel('past', 50)}
                    className="px-2.5 py-1 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors"
                >
                    −50
                </button>

                <span className="px-2.5 py-1 text-text-primary font-bold">Timeline</span>

                <button
                    onClick={() => timeTravel('future', 50)}
                    className="px-2.5 py-1 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors"
                >
                    +50
                </button>

                <button
                    onClick={() => timeTravel('future', 200)}
                    className="px-2.5 py-1 bg-surface border border-surface-border rounded-md hover:bg-surface-hover transition-colors"
                >
                    <span>+200</span>
                </button>

                <button
                    onClick={() => {
                        virtuosoRef.current?.scrollToIndex({ index: messages.length - 1, align: "end", behavior: "smooth" });
                    }}
                    className="px-2.5 py-1 bg-primary text-black font-bold rounded-md hover:bg-primary/90 transition-colors ml-2"
                >
                    HEAD →
                </button>
            </div>

            {/* Portal-based tooltip that renders outside the overflow container */}
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
                        <div className="text-xs font-bold text-text-primary mb-1">{hoveredTooltip.name}</div>
                        <div className="text-[10px] text-text-secondary">Seq: {hoveredTooltip.seq}</div>
                        <div className="text-[10px] text-text-secondary">Lag: {hoveredTooltip.lag}</div>
                        <div className="text-[9px] text-text-secondary/70 mt-1 italic">Click to jump</div>

                        {/* Pointy arrow */}
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

            {/* Portal-based dropdown menu */}
            {isDropdownOpen && dropdownPosition && createPortal(
                <div
                    className="fixed w-72 bg-surface border border-surface-border rounded-lg shadow-xl z-[10000] max-h-96 overflow-y-auto"
                    style={{
                        top: `${dropdownPosition.top}px`,
                        right: `${dropdownPosition.right}px`
                    }}
                >
                    <div className="p-2 border-b border-surface-border">
                        <span className="text-[10px] font-bold text-text-secondary uppercase">Consumer Groups (Sorted by Lag)</span>
                    </div>
                    {sortedGroups.map((group) => {
                        return (
                            <button
                                key={group.name}
                                onClick={() => jumpToGroup(group.name)}
                                className={cn(
                                    "w-full px-3 py-2 flex items-center justify-between hover:bg-surface-hover transition-colors",
                                    group.name === focusedGroupName && "bg-surface-hover"
                                )}
                            >
                                <div className="flex items-center gap-2 min-w-0 flex-1">
                                    <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: group.color }} />
                                    <span className="text-xs font-medium text-text-primary truncate">{group.name}</span>
                                </div>
                                <div className="flex items-center gap-2 flex-shrink-0 ml-2">
                                    <span className="text-[10px] text-text-secondary">Seq: {group.current_seq}</span>
                                    <span className="text-[10px] text-text-secondary">
                                        Lag: {group.lag}
                                    </span>
                                </div>
                            </button>
                        );
                    })}
                </div>,
                document.body
            )}

            {/* Message Data Viewer Modal */}
            {selectedMessage && (
                <DataViewer
                    data={selectedMessage.payload}
                    streamId={selectedMessage.id}
                    onClose={() => setSelectedMessage(null)}
                />
            )}
        </div>
    );
}
