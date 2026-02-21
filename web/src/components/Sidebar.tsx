import { useState } from "react";
import { NavLink } from "react-router-dom";
import {
    LayoutGrid,
    Database,
    GitGraph,
    PanelLeft,
    Check,
    Settings,
    Layers,
    MessageSquare,
    Zap,
    Activity,
    Users,
    BarChart3
} from "lucide-react";
import { cn } from "../lib/utils";

type SidebarMode = 'expanded' | 'collapsed' | 'hover';

const NAV_ITEMS = [
    {
        section: "Cluster",
        items: [
            { name: "Overview", path: "/", icon: LayoutGrid },
        ]
    },
    {
        section: "Data Layer",
        items: [
            { name: "Streams", path: "/streams", icon: Layers },
            { name: "KV Store", path: "/kv", icon: Database },
            { name: "Queues", path: "/queues", icon: MessageSquare },
            { name: "Time Series", path: "/timeseries", icon: BarChart3 },
        ]
    },
    {
        section: "Compute",
        items: [
            { name: "Actions", path: "/actions", icon: Zap },
            { name: "Workers", path: "/workers", icon: Users },
            { name: "Processing", path: "/processing", icon: Activity },
            { name: "Workflows", path: "/workflows", icon: GitGraph },
        ]
    },
    {
        section: "Settings",
        items: [
            { name: "Configuration", path: "/settings", icon: Settings },
        ]
    }
];

export function Sidebar() {
    const [isHovered, setIsHovered] = useState(false);
    const [mode, setMode] = useState<SidebarMode>('hover');
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    // Calculate effective width state
    const isExpanded = mode === 'expanded' || (mode === 'hover' && isHovered);

    return (
        <>
            {/* Layout Placeholder - reserves space in the main layout */}
            <div
                className={cn(
                    "h-full flex-shrink-0 bg-surface border-r border-surface-border transition-all duration-300 ease-in-out",
                    mode === 'expanded' ? "w-64" : "w-16"
                )}
            />

            {/* Actual Sidebar */}
            <div
                className={cn(
                    "fixed left-0 top-14 h-[calc(100vh-3.5rem)] border-r border-surface-border bg-surface flex flex-col transition-all duration-300 ease-in-out z-40",
                    isExpanded ? "w-64 shadow-2xl" : "w-16"
                )}
                onMouseEnter={() => mode === 'hover' && setIsHovered(true)}
                onMouseLeave={() => mode === 'hover' && setIsHovered(false)}
            >
                {/* Navigation */}
                <nav className="flex-1 overflow-y-auto py-4 space-y-6 scrollbar-hide">
                    {NAV_ITEMS.map((section, idx) => (
                        <div key={idx} className="space-y-0.5">
                            {section.items.map((item) => (
                                <NavLink
                                    key={item.path}
                                    to={item.path}
                                    className={({ isActive }) => cn(
                                        "flex items-center gap-3 h-10 text-sm transition-colors whitespace-nowrap relative",
                                        "pl-6", // Fixed padding
                                        isActive
                                            ? "text-primary font-medium"
                                            : "text-text-secondary hover:text-text-primary hover:bg-surface-hover"
                                    )}
                                    title={!isExpanded ? item.name : undefined}
                                >
                                    {({ isActive }) => (
                                        <>
                                            {/* Active Indicator */}
                                            {isActive && (
                                                <div className="absolute left-0 top-0 bottom-0 w-1 bg-primary rounded-r-full" />
                                            )}

                                            <item.icon className={cn("w-4 h-4 min-w-[16px] flex-shrink-0", isActive ? "text-primary" : "text-text-secondary")} />
                                            <span className={cn("transition-opacity duration-200 leading-none", isExpanded ? "opacity-100" : "opacity-0 w-0 hidden")}>
                                                {item.name}
                                            </span>
                                        </>
                                    )}
                                </NavLink>
                            ))}
                        </div>
                    ))}
                </nav>

                {/* Bottom Actions */}
                <div className="flex flex-col border-t border-surface-border">
                    {/* Sidebar Control */}
                    <div className="relative">
                        <div
                            className={cn(
                                "h-[50px] flex items-center overflow-hidden hover:bg-surface-hover transition-colors cursor-pointer",
                                "pl-5"
                            )}
                            onClick={() => setIsMenuOpen(!isMenuOpen)}
                        >
                            <div className="w-6 h-6 min-w-[24px] flex items-center justify-center text-text-secondary flex-shrink-0">
                                <PanelLeft className="w-4 h-4" />
                            </div>
                        </div>

                        {/* Settings Menu */}
                        {isMenuOpen && (
                            <>
                                <div className="fixed inset-0 z-50" onClick={() => setIsMenuOpen(false)} />
                                <div className="absolute bottom-full left-2 mb-2 w-48 bg-surface border border-surface-border rounded-md shadow-xl z-[60] overflow-hidden">
                                    <div className="px-3 py-2 border-b border-surface-border text-xs font-medium text-text-secondary">
                                        Sidebar control
                                    </div>
                                    <div className="py-1">
                                        {[
                                            { id: 'expanded', label: 'Expanded' },
                                            { id: 'collapsed', label: 'Collapsed' },
                                            { id: 'hover', label: 'Expand on hover' }
                                        ].map((option) => (
                                            <button
                                                key={option.id}
                                                className="w-full px-3 py-2 text-left text-sm text-text-primary hover:bg-surface-hover flex items-center justify-between"
                                                onClick={() => {
                                                    setMode(option.id as SidebarMode);
                                                    setIsMenuOpen(false);
                                                }}
                                            >
                                                <span>{option.label}</span>
                                                {mode === option.id && <Check className="w-3.5 h-3.5 text-primary" />}
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            </>
                        )}
                    </div>
                </div>
            </div>
        </>
    );
}
