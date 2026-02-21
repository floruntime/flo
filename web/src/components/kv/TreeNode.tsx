import { useState } from "react";
import { CaretRight, FolderSimple, FolderOpen } from "@phosphor-icons/react";
import { cn } from "../../lib/utils";
import type { TreeNode as TreeNodeType } from "../../lib/tree-utils";
import { KeyTypeBadge, inferValueType } from "./KeyTypeBadge";

interface TreeNodeProps {
    node: TreeNodeType;
    selectedKey: string | null;
    onSelectKey: (key: string) => void;
    level?: number;
}

export function TreeNode({ node, selectedKey, onSelectKey, level = 0 }: TreeNodeProps) {
    const [isExpanded, setIsExpanded] = useState(level === 0);

    const handleClick = () => {
        if (node.type === 'folder') {
            setIsExpanded(!isExpanded);
        } else if (node.keyData) {
            onSelectKey(node.keyData.key);
        }
    };

    const isSelected = node.type === 'key' && node.keyData?.key === selectedKey;
    const indentPx = level * 20;

    return (
        <div>
            {/* Node Row */}
            <button
                onClick={handleClick}
                className={cn(
                    "w-full text-left px-3 py-1.5 hover:bg-surface-hover transition-colors group relative flex items-center gap-2",
                    isSelected && "bg-primary/10"
                )}
                style={{ paddingLeft: `${indentPx + 12}px` }}
            >
                {/* Folder: Caret + Icon */}
                {node.type === 'folder' && (
                    <>
                        <CaretRight
                            size={14}
                            weight="bold"
                            className={cn(
                                "text-text-secondary transition-transform flex-shrink-0",
                                isExpanded && "rotate-90"
                            )}
                        />
                        {isExpanded ? (
                            <FolderOpen size={16} weight="fill" className="text-text-secondary flex-shrink-0" />
                        ) : (
                            <FolderSimple size={16} weight="fill" className="text-text-secondary flex-shrink-0" />
                        )}
                    </>
                )}

                {/* Key: Type Badge */}
                {node.type === 'key' && node.keyData && (
                    <div className="ml-7 flex-shrink-0">
                        <KeyTypeBadge
                            type={inferValueType(node.keyData.versions?.[0]?.value as string | undefined, node.keyData.key)}
                        />
                    </div>
                )}

                {/* Name */}
                <span className={cn(
                    "font-mono text-xs truncate flex-1",
                    isSelected ? "text-primary font-medium" : "text-text-primary"
                )}>
                    {node.name}
                </span>

                {/* Folder: percentage + count */}
                {node.type === 'folder' && node.count !== undefined && (
                    <div className="flex items-center gap-3 text-xs text-text-secondary tabular-nums">
                        <span>{(node.percentage ?? 0) < 1 ? "<1" : node.percentage}%</span>
                        <span className="font-medium">{node.count}</span>
                    </div>
                )}

                {/* Key: TTL + Size */}
                {node.type === 'key' && node.keyData && (
                    <div className="flex items-center gap-3 text-xs text-text-secondary tabular-nums">
                        <span>{node.keyData.ttl_expiry ? formatTTL(node.keyData.ttl_expiry) : "No limit"}</span>
                        <span>{formatBytes(node.keyData.size)}</span>
                    </div>
                )}
            </button>

            {/* Children */}
            {node.type === 'folder' && isExpanded && node.children && (
                <div>
                    {node.children.map((child, index) => (
                        <TreeNode
                            key={`${child.fullPath}-${index}`}
                            node={child}
                            selectedKey={selectedKey}
                            onSelectKey={onSelectKey}
                            level={level + 1}
                        />
                    ))}
                </div>
            )}
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
