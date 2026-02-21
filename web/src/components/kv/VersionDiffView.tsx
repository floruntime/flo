import { cn } from "../../lib/utils";
import type { KVVersion } from "../../lib/types";
import { ArrowRight } from "lucide-react";

interface VersionDiffViewProps {
    oldVersion: KVVersion;
    newVersion: KVVersion;
    className?: string;
}

export function VersionDiffView({ oldVersion, newVersion, className }: VersionDiffViewProps) {
    // Simple mock diff logic for visualization
    // In a real app, use a proper diff library like 'diff' package

    const oldStr = typeof oldVersion.value === 'object'
        ? JSON.stringify(oldVersion.value, null, 2)
        : String(oldVersion.value);

    const newStr = typeof newVersion.value === 'object'
        ? JSON.stringify(newVersion.value, null, 2)
        : String(newVersion.value);

    return (
        <div className={cn("flex flex-col h-full", className)}>
            {/* Header */}
            <div className="flex items-center justify-between px-4 py-2 bg-surface border-b border-surface-border">
                <div className="flex items-center gap-4">
                    <div className="flex flex-col">
                        <span className="text-xs text-text-secondary">BASE</span>
                        <span className="font-mono text-sm font-bold">v{oldVersion.lsn}</span>
                    </div>
                    <ArrowRight className="w-4 h-4 text-text-secondary" />
                    <div className="flex flex-col">
                        <span className="text-xs text-text-secondary">COMPARE</span>
                        <span className="font-mono text-sm font-bold">v{newVersion.lsn}</span>
                    </div>
                </div>
                <div className="text-xs text-text-secondary">
                    {oldVersion.lsn > newVersion.lsn ? "Reverse Time" : "Forward Time"}
                </div>
            </div>

            {/* Diff Content */}
            <div className="flex-1 min-h-0 grid grid-cols-2 divide-x divide-surface-border overflow-hidden">
                {/* Old Side */}
                <div className="overflow-auto p-4 bg-error/5">
                    <pre className="text-xs font-mono text-text-primary whitespace-pre-wrap">
                        {oldStr}
                    </pre>
                </div>

                {/* New Side */}
                <div className="overflow-auto p-4 bg-success/5">
                    <pre className="text-xs font-mono text-text-primary whitespace-pre-wrap">
                        {newStr}
                    </pre>
                </div>
            </div>

            <div className="px-4 py-2 bg-surface border-t border-surface-border text-xs text-text-secondary flex gap-4">
                <span className="flex items-center gap-1">
                    <div className="w-2 h-2 bg-error/20 border border-error rounded-sm" /> Removed
                </span>
                <span className="flex items-center gap-1">
                    <div className="w-2 h-2 bg-success/20 border border-success rounded-sm" /> Added
                </span>
            </div>
        </div>
    );
}
