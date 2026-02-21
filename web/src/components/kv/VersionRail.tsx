import { cn } from "../../lib/utils";
import type { KVVersion } from "../../lib/types";
import { formatDistanceToNow } from "date-fns";

interface VersionRailProps {
    versions: KVVersion[];
    selectedLsn: number;
    diffLsn?: number;
    onSelectVersion: (lsn: number) => void;
    onToggleDiff: (lsn: number) => void;
    className?: string;
}

export function VersionRail({
    versions,
    selectedLsn,
    diffLsn,
    onSelectVersion,
    onToggleDiff,
    className
}: VersionRailProps) {
    // Sort versions by LSN descending (newest first)
    const sortedVersions = [...versions].sort((a, b) => b.lsn - a.lsn);

    return (
        <div className={cn("flex items-center gap-1 overflow-x-auto p-3 min-h-[60px]", className)}>
            {sortedVersions.map((version, index) => {
                const isHead = index === 0;
                const isSelected = version.lsn === selectedLsn;
                const isDiff = version.lsn === diffLsn;
                const isDeleted = version.deleted;

                return (
                    <div key={version.lsn} className="relative group flex flex-col items-center min-w-[32px]">
                        {/* Connector Line */}
                        {index < sortedVersions.length - 1 && (
                            <div className="absolute top-1/2 left-1/2 w-full h-0.5 bg-surface-border -z-10" />
                        )}

                        {/* Node */}
                        <button
                            onClick={(e) => {
                                if (e.shiftKey) {
                                    onToggleDiff(version.lsn);
                                } else {
                                    onSelectVersion(version.lsn);
                                }
                            }}
                            className={cn(
                                "w-3 h-3 rounded-full border-2 transition-all duration-200 z-10",
                                isHead ? "w-5 h-5 border-kv-current bg-kv-current/20" : "border-kv-history bg-surface",
                                isSelected && "ring-2 ring-offset-2 ring-kv-history scale-125",
                                isDiff && "ring-2 ring-offset-2 ring-warning scale-110",
                                isDeleted && "border-error bg-error/10",
                                "hover:scale-110 hover:border-primary"
                            )}
                            title={`LSN: ${version.lsn} - ${version.deleted ? 'Deleted' : 'Active'}`}
                        >
                            {isDeleted && <span className="block w-full h-full text-[8px] text-error text-center leading-none">×</span>}
                        </button>

                        {/* Label */}
                        <div className="mt-1.5 opacity-0 group-hover:opacity-100 transition-opacity text-[9px] text-text-secondary whitespace-nowrap absolute top-full">
                            v{version.lsn}
                            <br />
                            {formatDistanceToNow(version.timestamp, { addSuffix: true })}
                        </div>
                    </div>
                );
            })}
        </div>
    );
}
