import { cn } from "../../lib/utils";
import { useState, useEffect } from "react";
import { FloppyDisk, X } from "@phosphor-icons/react";

interface ValueEditorProps {
    value: string | object;
    isHistory?: boolean;
    readOnly?: boolean;
    editing?: boolean;
    onSave?: (value: string) => void;
    onCancelEdit?: () => void;
    className?: string;
}

type Format = "JSON" | "HEX" | "TEXT";

export function ValueEditor({ value, isHistory, readOnly, editing, onSave, onCancelEdit, className }: ValueEditorProps) {
    const [format, setFormat] = useState<Format>("JSON");
    const rawStr = typeof value === "object" ? JSON.stringify(value, null, 2) : String(value);
    const [editBuffer, setEditBuffer] = useState(rawStr);

    // Reset buffer when value changes or editing starts/stops
    useEffect(() => {
        setEditBuffer(typeof value === "object" ? JSON.stringify(value, null, 2) : String(value));
    }, [value, editing]);

    const handleSave = () => {
        onSave?.(editBuffer);
    };

    // Parse value if it's a string but we want to show object
    let displayValue = value;
    if (typeof value === 'string' && format === 'JSON') {
        try {
            displayValue = JSON.parse(value);
        } catch {
            // keep as string if not valid json
        }
    }

    const renderContent = () => {
        // Editing mode — always show textarea
        if (editing && !readOnly) {
            return (
                <textarea
                    className="w-full h-full p-4 font-mono text-sm bg-transparent border-none resize-none focus:ring-0 text-text-primary focus:outline-none"
                    value={editBuffer}
                    onChange={(e) => setEditBuffer(e.target.value)}
                    autoFocus
                />
            );
        }

        if (format === "JSON") {
            return (
                <pre className="text-sm font-mono p-4 overflow-auto h-full text-text-primary">
                    {JSON.stringify(displayValue, null, 2)}
                </pre>
            );
        }

        if (format === "HEX") {
            const str = typeof value === 'object' ? JSON.stringify(value) : String(value);
            const hex = str.split('').map(c => c.charCodeAt(0).toString(16).padStart(2, '0')).join(' ');
            return (
                <div className="font-mono text-xs p-4 overflow-auto h-full grid grid-cols-[1fr_300px] gap-4">
                    <div className="text-text-secondary break-all">{hex}</div>
                    <div className="text-text-primary border-l border-surface-border pl-4 opacity-50">{str}</div>
                </div>
            );
        }

        return (
            <textarea
                className="w-full h-full p-4 font-mono text-sm bg-transparent border-none resize-none focus:ring-0"
                value={typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}
                readOnly={readOnly}
            />
        );
    };

    return (
        <div className={cn(
            "flex flex-col h-full bg-background",
            isHistory && "border-l-2 border-kv-history/50",
            className
        )}>
            {/* Compact Header */}
            <div className={cn(
                "flex items-center justify-between px-4 py-2 border-b",
                editing ? "bg-primary/5 border-primary/20" : isHistory ? "bg-kv-history/5 border-kv-history/20" : "bg-surface-hover/30 border-surface-border"
            )}>
                <div className="text-[10px] font-medium text-text-secondary uppercase tracking-wide">
                    {editing ? "Editing" : isHistory ? "Historical Value" : "Current Value"}
                </div>
                <div className="flex items-center gap-2">
                    {editing && (
                        <div className="flex items-center gap-1 mr-2">
                            <button
                                onClick={handleSave}
                                className="flex items-center gap-1 px-2 py-1 text-[10px] font-medium rounded bg-primary text-white hover:bg-primary/90 transition-colors"
                            >
                                <FloppyDisk size={12} />
                                Save
                            </button>
                            <button
                                onClick={onCancelEdit}
                                className="flex items-center gap-1 px-2 py-1 text-[10px] font-medium rounded border border-surface-border text-text-secondary hover:text-text-primary hover:bg-surface-hover transition-colors"
                            >
                                <X size={12} />
                                Cancel
                            </button>
                        </div>
                    )}
                    {!editing && (
                        <div className="flex bg-surface rounded border border-surface-border p-0.5">
                            {(["JSON", "HEX", "TEXT"] as Format[]).map(f => (
                                <button
                                    key={f}
                                    onClick={() => setFormat(f)}
                                    className={cn(
                                        "px-2 py-0.5 text-[10px] font-medium rounded transition-colors",
                                        format === f ? "bg-primary/10 text-primary" : "text-text-secondary hover:text-text-primary"
                                    )}
                                >
                                    {f}
                                </button>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Content */}
            <div className="flex-1 min-h-0 relative">
                {renderContent()}
                {isHistory && (
                    <div className="absolute inset-0 pointer-events-none ring-inset ring-1 ring-kv-history/10" />
                )}
            </div>
        </div>
    );
}
