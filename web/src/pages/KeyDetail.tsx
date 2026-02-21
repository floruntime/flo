import { useParams } from "react-router-dom";
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import {
    Funnel,
    TreeStructure,
    List,
    MagnifyingGlass,
    ArrowsDownUp,
    ArrowClockwise,
    Key,
    CaretDown,
    Plus,
} from "@phosphor-icons/react";
import { api, type KVScanEntry } from "../lib/api";
import { useApi, LoadingState, ErrorState, EmptyState } from "../lib/useApi";
import { useKVWatch } from "../lib/useKVWatch";
import { KeyInspector } from "../components/kv/KeyInspector";
import { TreeNode as TreeNodeComponent } from "../components/kv/TreeNode";
import { KeyTypeBadge, inferValueType } from "../components/kv/KeyTypeBadge";
import type { ValueType } from "../components/kv/KeyTypeBadge";
import { buildKeyTree, filterTree } from "../lib/tree-utils";
import type { KVKey, KVVersion } from "../lib/types";
import { cn } from "../lib/utils";
import { useNamespace } from "../lib/NamespaceContext";

/**
 * KV Key Browser — Redis Insight-inspired layout.
 * Toolbar → summary bar → split pane (key list/tree + inspector).
 */
export function KeyDetail() {
    const { namespace, key: routeKey } = useParams();
    const { selected: contextNamespace } = useNamespace();
    const ns = namespace || contextNamespace || "default";

    const [selectedKey, setSelectedKey] = useState<string | null>(
        routeKey ? decodeURIComponent(routeKey) : null
    );
    const [searchQuery, setSearchQuery] = useState("");
    const [viewMode, setViewMode] = useState<"tree" | "list">("list");
    const [typeFilter, setTypeFilter] = useState<ValueType | "ALL">("ALL");
    const [isTypeDropdownOpen, setIsTypeDropdownOpen] = useState(false);
    const [sortBy, setSortBy] = useState<"name" | "size" | "version">("name");
    const [sortAsc, setSortAsc] = useState(true);
    const [showAddKey, setShowAddKey] = useState(false);
    const [newKeyName, setNewKeyName] = useState("");
    const [newKeyValue, setNewKeyValue] = useState("");
    const [addKeyError, setAddKeyError] = useState<string | null>(null);
    const [addKeySaving, setAddKeySaving] = useState(false);

    // Namespace-level stats
    const { data: nsStats } = useApi(
        () => api.getKVNamespaceDetail(ns), [ns], 10000
    );

    // Scan keys
    const { data: scanResult, loading, error, refetch } = useApi(
        () => api.getKVKeys(ns, "", 500), [ns], 5000
    );

    // Map scan entries → KVKey[] with inferred types
    const keys: KVKey[] = useMemo(() => {
        if (!scanResult?.keys) return [];
        return scanResult.keys.map((entry: KVScanEntry) => ({
            key: entry.key,
            namespace: entry.namespace,
            current_version: 1,
            current_lsn: entry.version,
            version_count: 1,
            size: entry.size,
            last_modified: Date.now(),
            versions: [{
                version: 1,
                lsn: entry.version,
                timestamp: Date.now(),
                value: entry.value,
                deleted: false,
            }],
        }));
    }, [scanResult]);

    // Filtering: type filter + search query
    const filteredKeys = useMemo(() => {
        let result = keys;
        if (typeFilter !== "ALL") {
            result = result.filter(k => {
                const val = k.versions?.[0]?.value;
                return inferValueType(val as string | undefined, k.key) === typeFilter;
            });
        }
        if (searchQuery) {
            const q = searchQuery.toLowerCase();
            result = result.filter(k => k.key.toLowerCase().includes(q));
        }
        return result;
    }, [keys, typeFilter, searchQuery]);

    // Sorting
    const sortedKeys = useMemo(() => {
        const sorted = [...filteredKeys].sort((a, b) => {
            if (sortBy === "size") return a.size - b.size;
            if (sortBy === "version") return a.version_count - b.version_count;
            return a.key.localeCompare(b.key);
        });
        return sortAsc ? sorted : sorted.reverse();
    }, [filteredKeys, sortBy, sortAsc]);

    // Tree
    const keyTree = useMemo(() => buildKeyTree(filteredKeys), [filteredKeys]);
    const filteredTree = useMemo(() => filterTree(keyTree, searchQuery), [keyTree, searchQuery]);

    // On-demand history loading
    const [selectedKeyData, setSelectedKeyData] = useState<KVKey | null>(null);
    const [historyLoading, setHistoryLoading] = useState(false);

    // Stable ref for keys — avoids re-creating loadKeyDetail every scan poll
    const keysRef = useRef(keys);
    keysRef.current = keys;

    // ---- SSE: live updates for the selected key ----
    const { status: sseStatus } = useKVWatch(ns, selectedKey, {
        onUpdate: useCallback((evt) => {
            // Push the new value/version into the inspector without a full reload.
            setSelectedKeyData(prev => {
                if (!prev || prev.key !== evt.key) return prev;
                const newVersion: KVVersion = {
                    version: evt.version ?? prev.current_version + 1,
                    lsn: evt.version ?? 0,
                    timestamp: Date.now(),
                    value: evt.value ?? "",
                    deleted: false,
                };
                const existingVersions = prev.versions ?? [];
                // Avoid duplicate if we already have this version
                const alreadyHas = existingVersions.some(v => v.version === newVersion.version);
                return {
                    ...prev,
                    current_version: newVersion.version,
                    version_count: alreadyHas ? prev.version_count : prev.version_count + 1,
                    versions: alreadyHas ? existingVersions : [newVersion, ...existingVersions],
                };
            });
        }, []),
        onDelete: useCallback(() => {
            // Key was deleted — reload the scan list and deselect.
            refetch();
            setSelectedKey(null);
            setSelectedKeyData(null);
        }, [refetch]),
    });

    const loadKeyDetail = useCallback(async (keyName: string, silent = false) => {
        const baseKey = keysRef.current.find(k => k.key === keyName);
        if (!baseKey) return;
        // Only reset to base (stale) data on first load, not on silent refresh
        if (!silent) setSelectedKeyData(baseKey);
        setHistoryLoading(!silent);

        try {
            const [history, current] = await Promise.all([
                api.getKVKeyHistory(ns, keyName, 20),
                api.getKVKeyValue(ns, keyName),
            ]);

            const versions: KVVersion[] = history.versions.map(v => ({
                version: v.version,
                lsn: v.lsn,
                timestamp: Date.now(),
                value: v.value,
                deleted: v.deleted,
            }));

            const latestVersion = versions.length > 0
                ? Math.max(...versions.map(v => v.version))
                : baseKey.current_version;

            setSelectedKeyData({
                ...baseKey,
                version_count: history.version_count || versions.length,
                current_version: latestVersion,
                current_lsn: current.found ? (current.version ?? baseKey.current_lsn) : baseKey.current_lsn,
                versions: versions.length > 0 ? versions : baseKey.versions,
            });
        } catch (err) {
            console.warn("Failed to load key history:", err);
        } finally {
            setHistoryLoading(false);
        }
    }, [ns]); // keysRef is stable — no dependency on `keys`

    useEffect(() => {
        if (selectedKey) {
            loadKeyDetail(selectedKey);
        } else {
            setSelectedKeyData(null);
        }
    // loadKeyDetail only changes when `ns` changes — safe to include
    }, [selectedKey, loadKeyDetail]);

    const toggleSort = (field: "name" | "size" | "version") => {
        if (sortBy === field) {
            setSortAsc(!sortAsc);
        } else {
            setSortBy(field);
            setSortAsc(true);
        }
    };

    const handleAddKey = async () => {
        if (!newKeyName.trim()) {
            setAddKeyError("Key name is required");
            return;
        }
        setAddKeySaving(true);
        setAddKeyError(null);
        try {
            await api.putKVKey(ns, newKeyName.trim(), newKeyValue);
            setShowAddKey(false);
            setNewKeyName("");
            setNewKeyValue("");
            refetch();
            setSelectedKey(newKeyName.trim());
        } catch (err) {
            setAddKeyError(err instanceof Error ? err.message : "Failed to create key");
        } finally {
            setAddKeySaving(false);
        }
    };

    const handleDeleteKey = () => {
        setSelectedKey(null);
        setSelectedKeyData(null);
        refetch();
    };

    const handleRefreshKey = () => {
        if (selectedKey) loadKeyDetail(selectedKey, true);
    };

    if (loading && !scanResult) return <LoadingState message="Loading keys..." />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;

    const typeOptions: (ValueType | "ALL")[] = ["ALL", "JSON", "STRING", "NUMBER", "BINARY"];

    return (
        <div className="flex flex-col h-[calc(100vh-56px)]">
            {/* ───── Toolbar ───── */}
            <div className="flex items-center gap-2 px-3 py-2 border-b border-surface-border bg-surface">
                {/* Filter toggle */}
                <button
                    className={cn(
                        "p-1.5 rounded transition-colors",
                        searchQuery ? "bg-primary/20 text-primary" : "text-text-secondary hover:text-text-primary hover:bg-surface-hover"
                    )}
                    title="Filter"
                >
                    <Funnel size={18} weight={searchQuery ? "fill" : "regular"} />
                </button>

                {/* Key Type Dropdown */}
                <div className="relative">
                    <button
                        onClick={() => setIsTypeDropdownOpen(!isTypeDropdownOpen)}
                        className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium border border-surface-border rounded bg-background hover:bg-surface-hover transition-colors text-text-primary"
                    >
                        {typeFilter === "ALL" ? "All Key Types" : typeFilter}
                        <CaretDown size={12} className="text-text-secondary" />
                    </button>
                    {isTypeDropdownOpen && (
                        <>
                            <div className="fixed inset-0 z-40" onClick={() => setIsTypeDropdownOpen(false)} />
                            <div className="absolute top-full left-0 mt-1 w-44 bg-surface border border-surface-border rounded-md shadow-xl z-50 py-1">
                                {typeOptions.map(opt => (
                                    <button
                                        key={opt}
                                        onClick={() => { setTypeFilter(opt); setIsTypeDropdownOpen(false); }}
                                        className={cn(
                                            "w-full px-3 py-1.5 text-left text-xs hover:bg-surface-hover transition-colors flex items-center gap-2",
                                            typeFilter === opt ? "text-primary" : "text-text-primary"
                                        )}
                                    >
                                        {opt === "ALL" ? (
                                            <span>All Key Types</span>
                                        ) : (
                                            <>
                                                <KeyTypeBadge type={opt} size="sm" />
                                                <span>{opt}</span>
                                            </>
                                        )}
                                    </button>
                                ))}
                            </div>
                        </>
                    )}
                </div>

                {/* Search Input */}
                <div className="flex-1 relative">
                    <MagnifyingGlass size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-secondary" />
                    <input
                        type="text"
                        placeholder="Filter by Key Name or Pattern"
                        className="w-full pl-8 pr-3 py-1.5 bg-background border border-surface-border rounded text-xs text-text-primary
                                   placeholder:text-text-secondary/50 focus:outline-none focus:border-primary/50 transition-colors"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                    />
                </div>

                {/* Sort */}
                <button
                    onClick={() => toggleSort("name")}
                    className="p-1.5 rounded text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                    title="Sort"
                >
                    <ArrowsDownUp size={18} />
                </button>

                {/* Search */}
                <button className="p-1.5 rounded text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors" title="Advanced Search">
                    <MagnifyingGlass size={18} />
                </button>

                <div className="w-px h-5 bg-surface-border" />

                {/* Add Key */}
                <button
                    onClick={() => setShowAddKey(true)}
                    className="flex items-center gap-1 px-2.5 py-1.5 text-xs font-medium rounded bg-primary/50 text-white hover:bg-primary/60 transition-colors"
                    title="Add new key"
                >
                    <Plus size={14} weight="bold" />
                    Key
                </button>
            </div>

            {/* ───── Summary Bar ───── */}
            <div className="flex items-center gap-3 px-3 py-1.5 border-b border-surface-border bg-surface/50 text-xs text-text-secondary">
                <span className="text-text-primary font-medium">
                    {viewMode === "tree"
                        ? `Results: ${filteredKeys.length}. Scanned ${keys.length} / ${nsStats?.key_count ?? keys.length}`
                        : `Total: ${filteredKeys.length}`
                    }
                </span>
                <span className="text-text-secondary/40">·</span>
                <div className="flex items-center gap-1">
                    <ArrowClockwise size={12} />
                    <span>&lt; 1 min</span>
                </div>
                <button
                    onClick={refetch}
                    className="p-1 rounded hover:bg-surface-hover transition-colors"
                    title="Refresh"
                >
                    <ArrowClockwise size={14} />
                </button>

                {scanResult?.has_more && (
                    <span className="text-amber-400 text-[10px] font-medium">(truncated)</span>
                )}

                <div className="ml-auto flex items-center gap-1">
                    {/* View Toggle */}
                    <button
                        onClick={() => setViewMode("list")}
                        className={cn(
                            "p-1.5 rounded transition-colors",
                            viewMode === "list"
                                ? "bg-primary/20 text-primary"
                                : "text-text-secondary hover:text-text-primary hover:bg-surface-hover"
                        )}
                        title="List view"
                    >
                        <List size={16} />
                    </button>
                    <button
                        onClick={() => setViewMode("tree")}
                        className={cn(
                            "p-1.5 rounded transition-colors",
                            viewMode === "tree"
                                ? "bg-primary/20 text-primary"
                                : "text-text-secondary hover:text-text-primary hover:bg-surface-hover"
                        )}
                        title="Tree view"
                    >
                        <TreeStructure size={16} />
                    </button>
                </div>
            </div>

            {/* ───── Split Pane ───── */}
            {keys.length === 0 ? (
                <EmptyState
                    title="No keys in this namespace"
                    description="Use the CLI to set a key: flo kv set mykey myvalue"
                />
            ) : (
                <div className="flex flex-1 min-h-0 overflow-hidden">
                    {/* Left: Key Browser */}
                    <div className={cn(
                        "flex flex-col border-r border-surface-border overflow-hidden transition-all duration-200",
                        selectedKey ? "w-[520px] hidden lg:flex" : "flex-1"
                    )}>
                        {/* List View */}
                        {viewMode === "list" ? (
                            <div className="flex-1 overflow-y-auto">
                                {sortedKeys.length === 0 ? (
                                    <NoMatches />
                                ) : (
                                    <table className="w-full text-left text-xs">
                                        <thead className="sticky top-0 bg-surface border-b border-surface-border z-[1]">
                                            <tr className="text-text-secondary font-medium">
                                                <th className="px-3 py-2 w-[60px]">Type</th>
                                                <th
                                                    className="px-3 py-2 cursor-pointer hover:text-text-primary transition-colors"
                                                    onClick={() => toggleSort("name")}
                                                >
                                                    Key {sortBy === "name" && (sortAsc ? "↑" : "↓")}
                                                </th>
                                                <th className="px-3 py-2 w-[80px] text-right">
                                                    <button onClick={() => toggleSort("version")} className="hover:text-text-primary transition-colors">
                                                        Versions {sortBy === "version" && (sortAsc ? "↑" : "↓")}
                                                    </button>
                                                </th>
                                                <th className="px-3 py-2 w-[80px] text-right">TTL</th>
                                                <th
                                                    className="px-3 py-2 w-[80px] text-right cursor-pointer hover:text-text-primary transition-colors"
                                                    onClick={() => toggleSort("size")}
                                                >
                                                    Size {sortBy === "size" && (sortAsc ? "↑" : "↓")}
                                                </th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {sortedKeys.map((k) => {
                                                const vtype = inferValueType(
                                                    k.versions?.[0]?.value as string | undefined,
                                                    k.key
                                                );
                                                const isSelected = selectedKey === k.key;
                                                return (
                                                    <tr
                                                        key={k.key}
                                                        onClick={() => setSelectedKey(k.key)}
                                                        className={cn(
                                                            "cursor-pointer transition-colors border-b border-surface-border/50",
                                                            isSelected
                                                                ? "bg-primary/10"
                                                                : "hover:bg-surface-hover/50"
                                                        )}
                                                    >
                                                        <td className="px-3 py-2.5">
                                                            <KeyTypeBadge type={vtype} />
                                                        </td>
                                                        <td className="px-3 py-2.5">
                                                            <div className="flex items-center gap-2 min-w-0">
                                                                <span className={cn(
                                                                    "font-mono text-xs truncate",
                                                                    isSelected ? "text-primary font-medium" : "text-text-primary"
                                                                )}>
                                                                    {k.key}
                                                                </span>
                                                            </div>
                                                        </td>
                                                        <td className="px-3 py-2.5 text-right">
                                                            {k.version_count > 1 ? (
                                                                <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-500/15 text-purple-400 border border-purple-500/20 font-medium">
                                                                    v{k.version_count}
                                                                </span>
                                                            ) : (
                                                                <span className="text-text-secondary/40">1</span>
                                                            )}
                                                        </td>
                                                        <td className="px-3 py-2.5 text-right text-text-secondary">
                                                            {k.ttl_expiry ? formatTTL(k.ttl_expiry) : (
                                                                <span className="text-text-secondary/40">No limit</span>
                                                            )}
                                                        </td>
                                                        <td className="px-3 py-2.5 text-right text-text-secondary">
                                                            {formatBytes(k.size)}
                                                        </td>
                                                    </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                )}
                            </div>
                        ) : (
                            /* Tree View */
                            <div className="flex-1 overflow-y-auto">
                                {filteredTree.length === 0 ? (
                                    <NoMatches />
                                ) : (
                                    <div>
                                        {filteredTree.map((node, i) => (
                                            <TreeNodeComponent
                                                key={`${node.fullPath}-${i}`}
                                                node={node}
                                                selectedKey={selectedKey}
                                                onSelectKey={setSelectedKey}
                                            />
                                        ))}
                                    </div>
                                )}
                            </div>
                        )}
                    </div>

                    {/* Right: Inspector */}
                    <div className={cn(
                        "flex-1 overflow-hidden",
                        !selectedKey && "hidden lg:flex items-center justify-center"
                    )}>
                        {selectedKeyData ? (
                            <div className="relative flex flex-col w-full h-full">
                                {historyLoading && (
                                    <div className="absolute top-3 right-3 z-10">
                                        <div className="w-4 h-4 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                                    </div>
                                )}
                                <KeyInspector
                                    kvKey={selectedKeyData}
                                    onClose={() => setSelectedKey(null)}
                                    onRefresh={handleRefreshKey}
                                    onDeleted={handleDeleteKey}
                                    isLive={sseStatus === "open"}
                                />
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center h-full text-text-secondary">
                                <Key size={48} className="mb-3 opacity-10" />
                                <p className="text-sm">Select the key from the list on the left to see the details of the key.</p>
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* ───── Add Key Modal ───── */}
            {showAddKey && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
                    <div className="bg-surface border border-surface-border rounded-lg shadow-2xl w-full max-w-lg mx-4">
                        <div className="flex items-center justify-between px-5 py-3.5 border-b border-surface-border">
                            <h3 className="text-sm font-semibold text-text-primary">Add New Key</h3>
                            <button
                                onClick={() => { setShowAddKey(false); setAddKeyError(null); }}
                                className="p-1 rounded text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                            >
                                <Plus size={16} className="rotate-45" />
                            </button>
                        </div>
                        <div className="px-5 py-4 space-y-4">
                            <div>
                                <label className="block text-xs font-medium text-text-secondary mb-1.5">Key Name</label>
                                <input
                                    type="text"
                                    value={newKeyName}
                                    onChange={(e) => setNewKeyName(e.target.value)}
                                    placeholder="e.g. user:1234 or config.timeout"
                                    className="w-full px-3 py-2 bg-background border border-surface-border rounded text-sm text-text-primary
                                               placeholder:text-text-secondary/40 focus:outline-none focus:border-primary/50 font-mono"
                                    autoFocus
                                    onKeyDown={(e) => e.key === "Enter" && handleAddKey()}
                                />
                            </div>
                            <div>
                                <label className="block text-xs font-medium text-text-secondary mb-1.5">Value</label>
                                <textarea
                                    value={newKeyValue}
                                    onChange={(e) => setNewKeyValue(e.target.value)}
                                    placeholder='Enter value (text, JSON, number...)'
                                    rows={6}
                                    className="w-full px-3 py-2 bg-background border border-surface-border rounded text-sm text-text-primary
                                               placeholder:text-text-secondary/40 focus:outline-none focus:border-primary/50 font-mono resize-none"
                                />
                            </div>
                            {addKeyError && (
                                <div className="text-xs text-error bg-error/10 border border-error/20 rounded px-3 py-2">
                                    {addKeyError}
                                </div>
                            )}
                        </div>
                        <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-surface-border">
                            <button
                                onClick={() => { setShowAddKey(false); setAddKeyError(null); }}
                                className="px-3 py-1.5 text-xs font-medium rounded border border-surface-border text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleAddKey}
                                disabled={addKeySaving || !newKeyName.trim()}
                                className="px-3 py-1.5 text-xs font-medium rounded bg-primary text-white hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                            >
                                {addKeySaving ? "Creating..." : "Create Key"}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

function NoMatches() {
    return (
        <div className="flex flex-col items-center justify-center h-40 text-text-secondary text-sm">
            <Key size={32} className="mb-2 opacity-20" />
            No keys match
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
