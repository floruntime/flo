import { cn } from "../../lib/utils";
import type { KVKey } from "../../lib/types";
import { KeyInspector } from "./KeyInspector";
import { TTLHealthIndicator } from "./TTLHealthIndicator";
import { TreeNode } from "./TreeNode";
import { useState, useMemo } from "react";
import { Search, Database, List, Network } from "lucide-react";
import { buildKeyTree, filterTree } from "../../lib/tree-utils";

interface KeyBrowserProps {
    keys: KVKey[];
    className?: string;
}

export function KeyBrowser({ keys, className }: KeyBrowserProps) {
    const [selectedKey, setSelectedKey] = useState<string | null>(null);
    const [searchQuery, setSearchQuery] = useState("");
    const [viewMode, setViewMode] = useState<'tree' | 'list'>('tree');

    // Build tree structure
    const keyTree = useMemo(() => buildKeyTree(keys), [keys]);

    // Filter based on search
    const filteredTree = useMemo(() =>
        filterTree(keyTree, searchQuery),
        [keyTree, searchQuery]);

    const filteredKeys = useMemo(() => {
        if (!searchQuery) return keys;
        const lowerQuery = searchQuery.toLowerCase();
        return keys.filter(k => k.key.toLowerCase().includes(lowerQuery));
    }, [keys, searchQuery]);

    const selectedKeyData = useMemo(() =>
        keys.find(k => k.key === selectedKey),
        [keys, selectedKey]);

    return (
        <div className={cn("flex gap-6 h-full overflow-hidden", className)}>
            {/* Left Panel: Key List/Tree */}
            <div className={cn(
                "flex flex-col bg-surface border border-surface-border rounded-lg overflow-hidden transition-all duration-300",
                selectedKey ? "w-[320px] hidden lg:flex" : "w-full lg:w-[320px]"
            )}>
                {/* Search Header */}
                <div className="p-3 border-b border-surface-border">
                    <div className="relative flex gap-2 mb-2">
                        <div className="relative flex-1">
                            <Search className="absolute left-2.5 top-2.5 w-4 h-4 text-text-secondary" />
                            <input
                                type="text"
                                placeholder="Search keys..."
                                className="w-full pl-9 pr-3 py-2 bg-background border border-surface-border rounded-md text-sm focus:outline-none focus:ring-1 focus:ring-primary"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                            />
                        </div>
                    </div>

                    {/* View Mode Toggle */}
                    <div className="flex gap-1 bg-background border border-surface-border rounded-md p-0.5">
                        <button
                            onClick={() => setViewMode('tree')}
                            className={cn(
                                "flex-1 flex items-center justify-center gap-1.5 px-2 py-1 rounded text-xs font-medium transition-colors",
                                viewMode === 'tree'
                                    ? "bg-surface text-text-primary"
                                    : "text-text-secondary hover:text-text-primary"
                            )}
                        >
                            <Network className="w-3.5 h-3.5" />
                            Tree
                        </button>
                        <button
                            onClick={() => setViewMode('list')}
                            className={cn(
                                "flex-1 flex items-center justify-center gap-1.5 px-2 py-1 rounded text-xs font-medium transition-colors",
                                viewMode === 'list'
                                    ? "bg-surface text-text-primary"
                                    : "text-text-secondary hover:text-text-primary"
                            )}
                        >
                            <List className="w-3.5 h-3.5" />
                            List
                        </button>
                    </div>
                </div>

                {/* Key List/Tree */}
                <div className="flex-1 overflow-y-auto">
                    {viewMode === 'tree' ? (
                        // Tree View
                        filteredTree.length === 0 ? (
                            <div className="flex flex-col items-center justify-center h-40 text-text-secondary text-sm">
                                <Database className="w-8 h-8 mb-2 opacity-20" />
                                No keys found
                            </div>
                        ) : (
                            <div>
                                {filteredTree.map((node, index) => (
                                    <TreeNode
                                        key={`${node.fullPath}-${index}`}
                                        node={node}
                                        selectedKey={selectedKey}
                                        onSelectKey={setSelectedKey}
                                    />
                                ))}
                            </div>
                        )
                    ) : (
                        // List View
                        filteredKeys.length === 0 ? (
                            <div className="flex flex-col items-center justify-center h-40 text-text-secondary text-sm">
                                <Database className="w-8 h-8 mb-2 opacity-20" />
                                No keys found
                            </div>
                        ) : (
                            <div className="divide-y divide-surface-border">
                                {filteredKeys.map((key) => (
                                    <button
                                        key={key.key}
                                        onClick={() => setSelectedKey(key.key)}
                                        className={cn(
                                            "w-full text-left px-3 py-2.5 hover:bg-surface-hover transition-colors group relative",
                                            selectedKey === key.key && "bg-surface-hover border-l-2 border-primary"
                                        )}
                                    >
                                        <div className="flex items-center justify-between mb-1">
                                            <span className={cn(
                                                "font-mono text-sm truncate",
                                                selectedKey === key.key ? "text-primary font-medium" : "text-text-primary"
                                            )}>
                                                {key.key}
                                            </span>
                                            {key.version_count > 1 && (
                                                <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-kv-history/10 text-kv-history font-medium">
                                                    v{key.version_count}
                                                </span>
                                            )}
                                        </div>

                                        <div className="flex items-center justify-between text-xs text-text-secondary mb-1.5">
                                            <span>{key.size} B</span>
                                            <span className="opacity-50 text-[10px]">LSN {key.current_lsn}</span>
                                        </div>

                                        {/* TTL Indicator */}
                                        <TTLHealthIndicator expiry={key.ttl_expiry} />
                                    </button>
                                ))}
                            </div>
                        )
                    )}
                </div>
            </div>

            {/* Right Panel: Inspector */}
            <div className={cn(
                "flex-1 bg-surface border border-surface-border rounded-lg overflow-hidden",
                !selectedKey && "hidden lg:flex items-center justify-center",
                selectedKey ? "flex" : "hidden"
            )}>
                {selectedKeyData ? (
                    <KeyInspector
                        kvKey={selectedKeyData}
                        onClose={() => setSelectedKey(null)}
                    />
                ) : (
                    <div className="text-center text-text-secondary">
                        <Database className="w-12 h-12 mx-auto mb-3 opacity-10" />
                        <p className="text-sm">Select a key to inspect</p>
                    </div>
                )}
            </div>
        </div>
    );
}
