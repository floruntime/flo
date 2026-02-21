import { useState } from "react";
import { cn } from "../../lib/utils";
import { X, FileJson, FileCode, FileText } from "lucide-react";

interface DataViewerProps {
    data: string;
    streamId?: string;
    onClose: () => void;
}

type ViewFormat = 'json' | 'xml' | 'raw';

export function DataViewer({ data, streamId, onClose }: DataViewerProps) {
    const [format, setFormat] = useState<ViewFormat>('json');

    // Try to detect format from data
    const detectFormat = (): ViewFormat => {
        const trimmed = data.trim();
        if (trimmed.startsWith('{') || trimmed.startsWith('[')) return 'json';
        if (trimmed.startsWith('<')) return 'xml';
        return 'raw';
    };

    // Format the data based on selected format
    const getFormattedData = () => {
        try {
            switch (format) {
                case 'json':
                    // Try to parse and pretty-print JSON
                    try {
                        const parsed = JSON.parse(data);
                        return JSON.stringify(parsed, null, 2);
                    } catch {
                        return data; // If parsing fails, show raw
                    }
                case 'xml':
                    // Basic XML formatting (indent)
                    try {
                        const formatted = data
                            .replace(/>\s*</g, '>\n<')
                            .split('\n')
                            .map((line, idx) => {
                                const depth = (line.match(/<\//g) || []).length;
                                return '  '.repeat(Math.max(0, idx - depth)) + line.trim();
                            })
                            .join('\n');
                        return formatted;
                    } catch {
                        return data;
                    }
                case 'raw':
                default:
                    return data;
            }
        } catch {
            return data;
        }
    };

    const formats: { id: ViewFormat; label: string; icon: typeof FileJson }[] = [
        { id: 'json', label: 'JSON', icon: FileJson },
        { id: 'xml', label: 'XML', icon: FileCode },
        { id: 'raw', label: 'Raw', icon: FileText }
    ];

    // Detect initial format if not explicitly set
    useState(() => {
        setFormat(detectFormat());
    });

    return (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
            <div className="bg-surface border border-surface-border rounded-lg max-w-4xl w-full max-h-[80vh] flex flex-col">
                {/* Header */}
                <div className="flex items-center justify-between px-6 py-4 border-b border-surface-border">
                    <div>
                        <h3 className="text-lg font-bold text-text-primary">Message Data Viewer</h3>
                        {streamId && (
                            <span className="text-xs font-mono text-text-secondary">{streamId}</span>
                        )}
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-surface-hover rounded-md transition-colors"
                    >
                        <X className="w-5 h-5 text-text-secondary" />
                    </button>
                </div>

                {/* Format Toggle */}
                <div className="flex items-center gap-2 px-6 py-3 border-b border-surface-border bg-surface-hover/30">
                    <span className="text-xs text-text-secondary mr-2">View as:</span>
                    {formats.map(({ id, label, icon: Icon }) => (
                        <button
                            key={id}
                            onClick={() => setFormat(id)}
                            className={cn(
                                "flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-medium transition-all",
                                format === id
                                    ? "bg-primary text-background"
                                    : "bg-surface-border/30 text-text-secondary hover:bg-surface-border/50"
                            )}
                        >
                            <Icon className="w-3 h-3" />
                            {label}
                        </button>
                    ))}
                </div>

                {/* Content */}
                <div className="flex-1 overflow-auto p-6">
                    <pre className="font-mono text-xs text-text-primary bg-background p-4 rounded-lg border border-surface-border overflow-x-auto">
                        {getFormattedData()}
                    </pre>
                </div>

                {/* Footer with copy button */}
                <div className="px-6 py-3 border-t border-surface-border flex items-center justify-between">
                    <span className="text-xs text-text-secondary">
                        {data.length} characters
                    </span>
                    <button
                        onClick={() => navigator.clipboard.writeText(getFormattedData())}
                        className="px-3 py-1.5 bg-primary text-background text-xs font-medium rounded-md hover:bg-primary/90 transition-colors"
                    >
                        Copy to Clipboard
                    </button>
                </div>
            </div>
        </div>
    );
}
