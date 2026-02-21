import { useState } from "react";
import { TriageWard } from "./TriageWard";
import { ExclusiveLeaseLock } from "./ExclusiveLeaseLock";
import { MessagesList } from "./MessagesList";
import type { PendingMessage, ExclusiveGroup, StreamMessage } from "../../lib/types";
import { cn } from "../../lib/utils";

interface OperationalViewsProps {
    messages: StreamMessage[];
    seekerRange?: { start: number; end: number };
    pendingMessages: PendingMessage[];
    exclusiveGroups: ExclusiveGroup[];
    onReclaimNow?: (messageId: string) => void;
    onTouch?: (messageId: string) => void;
    onNack?: (messageId: string) => void;
    // onClaimLeadership?: (groupName: string, consumerId: string) => void;
}

type TabType = 'messages' | 'triage' | 'exclusive';

export function OperationalViews({
    messages,
    seekerRange,
    pendingMessages,
    exclusiveGroups,
    onReclaimNow,
    onTouch,
    onNack
}: OperationalViewsProps) {
    const [activeTab, setActiveTab] = useState<TabType>('messages');

    const tabs = [
        {
            id: 'messages' as TabType,
            label: 'Messages',
            count: seekerRange
                ? messages.slice(Math.max(0, seekerRange.start), seekerRange.end + 1).length
                : messages.length,
            color: 'text-blue-400'
        },
        {
            id: 'triage' as TabType,
            label: 'Triage Ward',
            count: pendingMessages.length,
            color: 'text-warning'
        },
        {
            id: 'exclusive' as TabType,
            label: 'Exclusive Groups',
            count: exclusiveGroups.length,
            color: 'text-primary'
        }
    ];

    return (
        <div className="bg-surface border border-surface-border rounded-lg overflow-hidden">
            {/* Tab Header */}
            <div className="border-b border-surface-border bg-surface-hover/30">
                <div className="flex items-center">
                    {tabs.map((tab) => {
                        const isActive = activeTab === tab.id;

                        return (
                            <button
                                key={tab.id}
                                onClick={() => setActiveTab(tab.id)}
                                className={cn(
                                    "px-4 py-3 text-sm font-medium transition-all relative",
                                    isActive
                                        ? "text-text-primary"
                                        : "text-text-secondary hover:text-text-primary"
                                )}
                            >
                                <div className="flex items-center gap-2">
                                    <span>{tab.label}</span>
                                    <span className={cn(
                                        "px-1.5 py-0.5 rounded text-[10px] font-bold",
                                        isActive
                                            ? "bg-primary/20 text-primary"
                                            : "bg-surface-border text-text-secondary"
                                    )}>
                                        {tab.count}
                                    </span>
                                </div>
                                {isActive && (
                                    <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary" />
                                )}
                            </button>
                        );
                    })}
                </div>
            </div>

            {/* Tab Content */}
            <div className="bg-surface">
                {activeTab === 'messages' && (
                    <MessagesList messages={messages} seekerRange={seekerRange} />
                )}

                {activeTab === 'triage' && (
                    <TriageWard
                        pendingMessages={pendingMessages}
                        onReclaimNow={onReclaimNow}
                        onTouch={onTouch}
                        onNack={onNack}
                    />
                )}

                {activeTab === 'exclusive' && (
                    <ExclusiveLeaseLock
                        groups={exclusiveGroups}
                    />
                )}
            </div>
        </div>
    );
}
