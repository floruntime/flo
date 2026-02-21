import { cn } from "../../lib/utils";
import type { KVTransaction } from "../../lib/types";
import { Save, XCircle } from "lucide-react";

interface TransactionDrawerProps {
    transaction: KVTransaction | null;
    onCommit: () => void;
    onRollback: () => void;
    className?: string;
}

export function TransactionDrawer({ transaction, onCommit, onRollback, className }: TransactionDrawerProps) {
    if (!transaction) return null;

    return (
        <div className={cn(
            "fixed bottom-14 right-4 w-[400px] bg-surface border-2 border-kv-transaction rounded-t-lg shadow-2xl z-40 flex flex-col transition-transform transform translate-y-0",
            className
        )}>
            {/* Header */}
            <div className="bg-kv-transaction/10 px-4 py-3 border-b border-kv-transaction/20 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-kv-transaction animate-pulse" />
                    <span className="font-bold text-kv-transaction text-sm">ACTIVE TRANSACTION</span>
                </div>
                <span className="text-xs text-text-secondary font-mono">{transaction.id}</span>
            </div>

            {/* Operations List */}
            <div className="max-h-[300px] overflow-y-auto p-2 space-y-2 bg-surface">
                {transaction.operations.map((op, i) => (
                    <div key={i} className="flex items-start gap-3 p-2 rounded bg-surface-hover/50 border border-surface-border text-sm">
                        <span className={cn(
                            "font-bold text-[10px] px-1.5 py-0.5 rounded uppercase",
                            op.type === 'PUT' ? "bg-primary/10 text-primary" : "bg-error/10 text-error"
                        )}>
                            {op.type}
                        </span>
                        <div className="flex-1 min-w-0">
                            <div className="font-mono text-text-primary truncate">{op.key}</div>
                            {op.value && (
                                <div className="text-xs text-text-secondary truncate opacity-70">
                                    {JSON.stringify(op.value)}
                                </div>
                            )}
                        </div>
                    </div>
                ))}
                {transaction.operations.length === 0 && (
                    <div className="text-center py-8 text-text-secondary italic text-sm">
                        No operations buffered yet...
                    </div>
                )}
            </div>

            {/* Actions */}
            <div className="p-4 border-t border-surface-border grid grid-cols-2 gap-3 bg-surface">
                <button
                    onClick={onRollback}
                    className="flex items-center justify-center gap-2 px-4 py-2 rounded border border-error text-error hover:bg-error/5 transition-colors text-sm font-medium"
                >
                    <XCircle className="w-4 h-4" />
                    Rollback
                </button>
                <button
                    onClick={onCommit}
                    className="flex items-center justify-center gap-2 px-4 py-2 rounded bg-kv-transaction text-white hover:bg-kv-transaction/90 transition-colors text-sm font-medium shadow-sm"
                >
                    <Save className="w-4 h-4" />
                    Commit
                </button>
            </div>
        </div>
    );
}
