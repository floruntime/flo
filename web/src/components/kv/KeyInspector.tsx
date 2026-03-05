import { cn } from "../../lib/utils";
import type { KVKey } from "../../lib/types";
import { ValueEditor } from "./ValueEditor";
import { VersionDiffView } from "./VersionDiffView";
import { KeyTypeBadge, inferValueType } from "./KeyTypeBadge";
import { useState, useEffect, useMemo } from "react";
import { api } from "../../lib/api";
import {
    X,
    ArrowClockwise,
    PencilSimple,
    Trash,
    CopySimple,
    ClockCounterClockwise,
    GitCommit,
    Check,
} from "@phosphor-icons/react";

interface KeyInspectorProps {
    kvKey: KVKey;
    className?: string;
    onClose?: () => void;
    onRefresh?: () => void;
    onDeleted?: () => void;
    fullPage?: boolean;
    isLive?: boolean;
}

export function KeyInspector({ kvKey, className, onClose, onRefresh, onDeleted, fullPage: _fullPage, isLive }: KeyInspectorProps) {
    const [selectedVersion, setSelectedVersionNum] = useState<number>(kvKey.current_version);
    const [diffVersion, setDiffVersionNum] = useState<number | undefined>(undefined);
    const [showVersions, setShowVersions] = useState(false);
    const [editing, setEditing] = useState(false);
    const [_saving, setSaving] = useState(false);
    const [copied, setCopied] = useState(false);
    const [deleting, setDeleting] = useState(false);

    const versions = kvKey.versions || [];
    const selectedVer = versions.find(v => v.version === selectedVersion) || versions[0];
    const diffVer = diffVersion ? versions.find(v => v.version === diffVersion) : undefined;
    const vtype = inferValueType(selectedVer?.value as string | undefined, kvKey.key);

    // Count top-level values for JSON
    const topLevelCount = useMemo(() => {
        if (!selectedVer?.value) return 0;
        try {
            const parsed = typeof selectedVer.value === "string"
                ? JSON.parse(selectedVer.value)
                : selectedVer.value;
            if (typeof parsed === "object" && parsed !== null) {
                return Object.keys(parsed).length;
            }
        } catch { /* not json */ }
        return 0;
    }, [selectedVer]);

    useEffect(() => {
        setSelectedVersionNum(kvKey.current_version);
        setDiffVersionNum(undefined);
        setShowVersions(false);
        setEditing(false);
    }, [kvKey.key, kvKey.current_version]);

    const handleSelectVersion = (ver: number) => {
        if (diffVersion === ver) setDiffVersionNum(undefined);
        setSelectedVersionNum(ver);
    };

    const handleToggleDiff = (ver: number) => {
        if (ver === selectedVersion) return;
        setDiffVersionNum(prev => prev === ver ? undefined : ver);
    };

    const handleSave = async (newValue: string) => {
        setSaving(true);
        try {
            await api.putKVKey(kvKey.namespace, kvKey.key, newValue);
            setEditing(false);
            onRefresh?.();
        } catch (err) {
            console.error("Failed to save key:", err);
        } finally {
            setSaving(false);
        }
    };

    const handleCopy = async () => {
        const val = selectedVer?.value ?? "";
        const str = typeof val === "object" ? JSON.stringify(val, null, 2) : String(val);
        try {
            await navigator.clipboard.writeText(str);
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
        } catch { /* clipboard not available */ }
    };

    const handleDelete = async () => {
        if (!confirm(`Delete key "${kvKey.key}"?`)) return;
        setDeleting(true);
        try {
            await api.deleteKVKey(kvKey.namespace, kvKey.key);
            onDeleted?.();
        } catch (err) {
            console.error("Failed to delete key:", err);
        } finally {
            setDeleting(false);
        }
    };

    const sortedVersions = [...versions].sort((a, b) => b.version - a.version);

    return (
        <div className={cn("flex flex-col h-full bg-background", className)}>
            {/* ───── Header ───── */}
            <div className="flex items-center justify-between px-4 py-2.5 border-b border-surface-border bg-surface">
                <div className="flex items-center gap-3 overflow-hidden min-w-0">
                    <KeyTypeBadge type={vtype} size="md" />
                    <div className="flex flex-col min-w-0">
                        <h2 className="text-sm font-semibold text-text-primary truncate font-mono">
                            {kvKey.key}
                        </h2>
                        <div className="text-[10px] text-text-secondary flex items-center gap-2 mt-0.5">
                            <span>{formatBytes(kvKey.size)}</span>
                            {topLevelCount > 0 && (
                                <>
                                    <span className="text-text-secondary/30">·</span>
                                    <span>Top-level values: {topLevelCount}</span>
                                </>
                            )}
                            <span className="text-text-secondary/30">·</span>
                            <span>TTL: {kvKey.ttl_expiry ? formatTTL(kvKey.ttl_expiry) : "No limit"}</span>
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-1">
                    {/* Version toggle */}
                    <button
                        onClick={() => setShowVersions(!showVersions)}
                        className={cn(
                            "flex items-center gap-1.5 px-2 py-1.5 rounded text-xs transition-colors",
                            showVersions
                                ? "bg-purple-500/15 text-purple-400"
                                : "text-text-secondary hover:text-text-primary hover:bg-surface-hover"
                        )}
                        title={`${kvKey.version_count} version${kvKey.version_count !== 1 ? "s" : ""}`}
                    >
                        <ClockCounterClockwise size={14} />
                        <span className="font-medium">{kvKey.version_count}</span>
                    </button>
                    <div className="w-px h-5 bg-surface-border mx-1" />
                    {/* Refresh */}
                    <button
                        onClick={onRefresh}
                        className="p-1.5 rounded text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                        title="Refresh"
                    >
                        <ArrowClockwise size={14} />
                    </button>
                    {/* Edit */}
                    <button
                        onClick={() => setEditing(!editing)}
                        className={cn(
                            "p-1.5 rounded transition-colors",
                            editing
                                ? "bg-primary/15 text-primary"
                                : "text-text-secondary hover:text-text-primary hover:bg-surface-hover"
                        )}
                        title={editing ? "Cancel edit" : "Edit"}
                        disabled={selectedVer?.version !== kvKey.current_version}
                    >
                        <PencilSimple size={14} />
                    </button>
                    {/* Copy */}
                    <button
                        onClick={handleCopy}
                        className="p-1.5 rounded text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                        title="Copy value"
                    >
                        {copied ? <Check size={14} className="text-green-400" /> : <CopySimple size={14} />}
                    </button>
                    {/* Delete */}
                    <button
                        onClick={handleDelete}
                        disabled={deleting}
                        className="p-1.5 rounded text-text-secondary hover:text-error hover:bg-error/10 transition-colors"
                        title="Delete key"
                    >
                        <Trash size={14} />
                    </button>
                    {/* Live indicator */}
                    {isLive && (
                        <div className="flex items-center gap-1 px-1.5" title="Live updates active">
                            <span className="relative flex h-2 w-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
                                <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
                            </span>
                            <span className="text-[10px] text-green-500 font-medium uppercase tracking-wide">Live</span>
                        </div>
                    )}
                    {/* Close */}
                    {onClose && (
                        <>
                            <div className="w-px h-5 bg-surface-border mx-1" />
                            <button
                                onClick={onClose}
                                className="p-1.5 rounded text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                            >
                                <X size={14} />
                            </button>
                        </>
                    )}
                </div>
            </div>

            {/* ───── Main Content ───── */}
            <div className="flex-1 min-h-0 flex">
                {/* Editor / Diff */}
                <div className="flex-1 min-h-0 flex flex-col">
                    {diffVer ? (
                        <VersionDiffView
                            oldVersion={diffVer.version < selectedVer.version ? diffVer : selectedVer}
                            newVersion={diffVer.version < selectedVer.version ? selectedVer : diffVer}
                        />
                    ) : (
                        <ValueEditor
                            value={selectedVer?.value ?? ""}
                            isHistory={selectedVer?.version !== kvKey.current_version}
                            readOnly={selectedVer?.version !== kvKey.current_version}
                            editing={editing && selectedVer?.version === kvKey.current_version}
                            onSave={handleSave}
                            onCancelEdit={() => setEditing(false)}
                        />
                    )}
                </div>

                {/* ───── Version Panel (slide-in) ───── */}
                {showVersions && versions.length > 0 && (
                    <div className="w-56 border-l border-surface-border bg-surface flex flex-col overflow-hidden">
                        <div className="px-3 py-2 border-b border-surface-border">
                            <div className="flex items-center gap-2 text-xs font-medium text-text-primary">
                                <ClockCounterClockwise size={14} />
                                Version History
                            </div>
                            <div className="text-[10px] text-text-secondary mt-0.5">
                                Shift+click to compare
                            </div>
                        </div>
                        <div className="flex-1 overflow-y-auto">
                            {sortedVersions.map((version, index) => {
                                const isHead = index === 0;
                                const isSelected = version.version === selectedVersion;
                                const isDiff = version.version === diffVersion;
                                const isDeleted = version.deleted;

                                return (
                                    <button
                                        key={version.version}
                                        onClick={(e) => {
                                            if (e.shiftKey) {
                                                handleToggleDiff(version.version);
                                            } else {
                                                handleSelectVersion(version.version);
                                            }
                                        }}
                                        className={cn(
                                            "w-full text-left px-3 py-2 border-b border-surface-border/50 transition-colors",
                                            isSelected && "bg-primary/10",
                                            isDiff && "bg-amber-500/10",
                                            !isSelected && !isDiff && "hover:bg-surface-hover/50"
                                        )}
                                    >
                                        <div className="flex items-center gap-2 mb-0.5">
                                            <GitCommit
                                                size={14}
                                                className={cn(
                                                    isHead ? "text-primary" : "text-text-secondary",
                                                    isDeleted && "text-error"
                                                )}
                                            />
                                            <span className={cn(
                                                "font-mono text-xs font-medium",
                                                isSelected ? "text-primary" : "text-text-primary",
                                                isDiff && "text-amber-400"
                                            )}>
                                                v{version.version}
                                            </span>
                                            {isHead && (
                                                <span className="text-[9px] px-1 py-px rounded bg-primary/20 text-primary font-bold uppercase">
                                                    HEAD
                                                </span>
                                            )}
                                            {isDeleted && (
                                                <span className="text-[9px] px-1 py-px rounded bg-error/20 text-error font-bold">
                                                    DEL
                                                </span>
                                            )}
                                            {isDiff && (
                                                <span className="text-[9px] px-1 py-px rounded bg-amber-500/20 text-amber-400 font-bold">
                                                    DIFF
                                                </span>
                                            )}
                                        </div>
                                        <div className="text-[10px] text-text-secondary ml-6">
                                            LSN {version.lsn}
                                        </div>
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function formatTTL(expiry: number): string {
    const remaining = expiry - Date.now();
    if (remaining <= 0) return "Expired";
    const seconds = Math.floor(remaining / 1000);
    if (seconds < 60) return `${seconds} s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes} m`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours} h`;
    const days = Math.floor(hours / 24);
    return `${days} d`;
}
