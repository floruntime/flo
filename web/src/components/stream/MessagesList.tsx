import { useState } from "react";

import type { StreamMessage } from "../../lib/types";
import { DataViewer } from "./DataViewer";


interface MessagesListProps {
    messages: StreamMessage[];
    seekerRange?: { start: number; end: number };
}

export function MessagesList({ messages, seekerRange }: MessagesListProps) {
    const [selectedMessage, setSelectedMessage] = useState<StreamMessage | null>(null);
    const [filter, setFilter] = useState("");
    const [currentPage, setCurrentPage] = useState(0);
    const PAGE_SIZE = 20;

    // Filter messages by seeker range first, then by key filter
    const rangeFilteredMessages = seekerRange
        ? messages.slice(seekerRange.start, seekerRange.end + 1)
        : messages;

    const filteredMessages = filter
        ? rangeFilteredMessages.filter(m => m.key.toLowerCase().includes(filter.toLowerCase()))
        : rangeFilteredMessages;

    // Pagination
    const totalPages = Math.ceil(filteredMessages.length / PAGE_SIZE);
    const safeCurrentPage = currentPage >= totalPages ? 0 : currentPage;
    const paginatedMessages = filteredMessages.slice(
        safeCurrentPage * PAGE_SIZE,
        (safeCurrentPage + 1) * PAGE_SIZE
    );

    const formatTime = (timestamp: number) => {
        return new Date(timestamp).toLocaleString('en-GB', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            day: '2-digit',
            month: 'short',
            year: 'numeric'
        });
    };


    return (
        <div>
            {/* Filter Bar */}
            <div className="px-6 py-4 border-b border-surface-border bg-surface-hover/30">
                <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-4">
                        <span className="text-xs text-text-secondary">
                            Showing <strong className="text-text-primary">{paginatedMessages.length}</strong> of <strong className="text-text-primary">{filteredMessages.length}</strong> messages
                        </span>
                    </div>
                    <input
                        type="text"
                        placeholder="Filter by key..."
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                        className="px-3 py-1.5 bg-surface border border-surface-border rounded-md text-xs text-text-primary placeholder-text-secondary focus:outline-none focus:ring-1 focus:ring-primary"
                    />
                </div>

                {/* Pagination Controls */}
                {totalPages > 1 && (
                    <div className="flex items-center justify-between">
                        <span className="text-[10px] text-text-secondary">
                            Page {safeCurrentPage + 1} of {totalPages}
                        </span>
                        <div className="flex items-center gap-2">
                            <button
                                onClick={() => setCurrentPage(p => Math.max(0, p - 1))}
                                disabled={safeCurrentPage === 0}
                                className="px-2 py-1 text-[10px] bg-surface border border-surface-border rounded hover:bg-surface-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                            >
                                Previous
                            </button>
                            <button
                                onClick={() => setCurrentPage(p => Math.min(totalPages - 1, p + 1))}
                                disabled={safeCurrentPage === totalPages - 1}
                                className="px-2 py-1 text-[10px] bg-surface border border-surface-border rounded hover:bg-surface-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                            >
                                Next
                            </button>
                        </div>
                    </div>
                )}
            </div>

            {/* Messages Table */}
            <div className="overflow-x-auto">
                <table className="w-full">
                    <thead>
                        <tr className="border-b border-surface-border bg-surface-hover/50">
                            <th className="w-48 px-4 py-3 text-left text-[10px] font-semibold text-text-secondary uppercase tracking-wider">Stream ID</th>
                            <th className="px-4 py-3 text-left text-[10px] font-semibold text-text-secondary uppercase tracking-wider">Size</th>
                            <th className="px-4 py-3 text-left text-[10px] font-semibold text-text-secondary uppercase tracking-wider">Payload</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-surface-border">
                        {paginatedMessages.length === 0 ? (
                            <tr>
                                <td colSpan={3} className="px-4 py-8 text-center text-sm text-text-secondary">
                                    {filter ? "No messages match the filter" : seekerRange ? "No messages in seeker range" : "No messages"}
                                </td>
                            </tr>
                        ) : (
                            paginatedMessages.map((msg) => (
                                <tr
                                    key={msg.id}
                                    onClick={() => setSelectedMessage(msg)}
                                    className="border-b border-surface-border hover:bg-surface-hover/50 transition-colors cursor-pointer"
                                >
                                    <td className="w-48 px-4 py-3 text-xs text-text-secondary font-mono">
                                        <div>{msg.id}</div>
                                        <div className="text-[10px] text-text-secondary/70 mt-1">{formatTime(msg.timestamp)}</div>
                                    </td>
                                    <td className="px-4 py-3 text-xs text-text-secondary">{msg.size}b</td>
                                    <td className="px-4 py-3 text-xs text-text-primary font-mono">
                                        <div className="max-w-md truncate">{msg.payload}</div>
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            {/* Data Viewer Modal */}
            {selectedMessage && (
                <DataViewer
                    data={selectedMessage.payload}
                    onClose={() => setSelectedMessage(null)}
                />
            )}
        </div>
    );
}
