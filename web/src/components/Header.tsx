import { useState, useEffect } from "react";
import {
    Zap,
    ChevronsUpDown,
    Check,
    Plus,
    Sun,
    Moon,
    Database,
    Shield,
} from "lucide-react";
import { useNamespace } from "../lib/NamespaceContext";
import { useAuth } from "../lib/AuthContext";
import type { NamespaceInfo } from "../lib/api";

function totalResources(ns: NamespaceInfo): number {
    return (
        (ns.stream_count || 0) +
        (ns.queue_count || 0) +
        (ns.kv_count || 0) +
        (ns.workflow_count || 0) +
        (ns.processing_count || 0) +
        (ns.action_count || 0)
    );
}

/*
 * NOTE: The "weaved" project selector and cloud/cluster mode toggle have been
 * removed. The header now shows a global namespace selector that is always
 * visible, powered by NamespaceContext. When cloud/multi-tenant features are
 * needed, re-add the project breadcrumbs here.
 */

export function Header() {
    const { namespaces, selected, setSelected, createNamespace } = useNamespace();
    const { auth, logout } = useAuth();
    const [isNamespaceOpen, setIsNamespaceOpen] = useState(false);
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    const [isCreating, setIsCreating] = useState(false);
    const [newName, setNewName] = useState("");
    const [createError, setCreateError] = useState("");
    const [theme, setTheme] = useState<'dark' | 'light'>(() => {
        const saved = localStorage.getItem('theme');
        return (saved as 'dark' | 'light') || 'dark';
    });

    useEffect(() => {
        document.documentElement.dataset.theme = theme;
        localStorage.setItem('theme', theme);
    }, [theme]);

    const toggleTheme = () => {
        setTheme(prev => prev === 'dark' ? 'light' : 'dark');
    };

    return (
        <header className="h-14 border-b border-surface-border bg-surface flex items-center justify-between px-4 sticky top-0 z-10">
            {/* Left: Logo + Namespace Selector */}
            <div className="flex items-center gap-2 text-sm text-text-secondary">
                <div className="w-6 h-6 rounded bg-primary/20 text-primary flex items-center justify-center">
                    <Zap className="w-3.5 h-3.5 fill-current" />
                </div>
                <span className="text-surface-border mx-1">/</span>

                {/* Global Namespace Selector — always visible */}
                <div className="relative">
                    <button
                        className="flex items-center gap-2 hover:bg-surface-hover px-2 py-1 rounded transition-colors text-text-primary font-medium"
                        onClick={() => setIsNamespaceOpen(!isNamespaceOpen)}
                    >
                        <Database className="w-3.5 h-3.5" />
                        <span>{selected}</span>
                        <ChevronsUpDown className="w-3 h-3 text-text-secondary" />
                    </button>

                    {isNamespaceOpen && (
                        <>
                            <div className="fixed inset-0 z-40" onClick={() => { setIsNamespaceOpen(false); setIsCreating(false); setNewName(""); setCreateError(""); }} />
                            <div className="absolute top-full left-0 mt-1 w-64 bg-surface border border-surface-border rounded-md shadow-xl z-50 overflow-hidden">
                                <div className="p-2 border-b border-surface-border">
                                    <div className="text-[10px] font-bold text-text-secondary uppercase">Select Namespace</div>
                                </div>
                                <div className="py-1 max-h-64 overflow-y-auto">
                                    {namespaces.map((ns) => (
                                        <button
                                            key={ns.name}
                                            onClick={() => {
                                                setSelected(ns.name);
                                                setIsNamespaceOpen(false);
                                            }}
                                            className="w-full px-3 py-2 flex items-center justify-between hover:bg-surface-hover transition-colors text-left"
                                        >
                                            <div className="flex flex-col">
                                                <span className="text-sm font-medium text-text-primary">{ns.name}</span>
                                                <span className="text-[10px] text-text-secondary">{totalResources(ns)} resources</span>
                                            </div>
                                            {ns.name === selected && (
                                                <Check className="w-3.5 h-3.5 text-primary" />
                                            )}
                                        </button>
                                    ))}
                                </div>
                                <div className="p-2 border-t border-surface-border">
                                    {isCreating ? (
                                        <form
                                            onSubmit={async (e) => {
                                                e.preventDefault();
                                                const trimmed = newName.trim();
                                                if (!trimmed) return;
                                                setCreateError("");
                                                try {
                                                    await createNamespace(trimmed);
                                                    setNewName("");
                                                    setIsCreating(false);
                                                    setIsNamespaceOpen(false);
                                                } catch (err) {
                                                    setCreateError(err instanceof Error ? err.message : "Failed to create namespace");
                                                }
                                            }}
                                            className="flex flex-col gap-1.5"
                                        >
                                            <div className="flex items-center gap-1.5">
                                                <input
                                                    autoFocus
                                                    type="text"
                                                    value={newName}
                                                    onChange={(e) => { setNewName(e.target.value); setCreateError(""); }}
                                                    onKeyDown={(e) => { if (e.key === "Escape") { setIsCreating(false); setNewName(""); setCreateError(""); } }}
                                                    placeholder="namespace-name"
                                                    className="flex-1 text-xs bg-surface-hover border border-surface-border rounded px-2 py-1 text-text-primary placeholder:text-text-secondary/50 outline-none focus:border-primary"
                                                />
                                                <button type="submit" className="text-xs bg-primary text-white px-2 py-1 rounded hover:bg-primary/80 transition-colors">
                                                    Create
                                                </button>
                                            </div>
                                            {createError && <span className="text-[10px] text-red-400">{createError}</span>}
                                        </form>
                                    ) : (
                                        <button
                                            onClick={() => setIsCreating(true)}
                                            className="flex items-center gap-2 text-xs text-text-secondary hover:text-text-primary transition-colors w-full"
                                        >
                                            <Plus className="w-3.5 h-3.5" />
                                            New namespace
                                        </button>
                                    )}
                                </div>
                            </div>
                        </>
                    )}
                </div>
            </div>

            {/* Right: Actions */}
            <div className="flex items-center gap-3">
                <div className="flex items-center gap-2 text-text-secondary">
                    <button
                        onClick={toggleTheme}
                        className="hover:text-text-primary transition-colors"
                        title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
                    >
                        {theme === 'dark' ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
                    </button>
                </div>

                {/* Profile Dropdown */}
                <div className="relative">
                    <button
                        onClick={() => setIsProfileOpen(!isProfileOpen)}
                        className="w-8 h-8 rounded-full bg-gradient-to-tr from-primary to-blue-500 border border-surface-border cursor-pointer hover:opacity-90 transition-opacity"
                    />

                    {isProfileOpen && (
                        <>
                            <div className="fixed inset-0 z-40" onClick={() => setIsProfileOpen(false)} />
                            <div className="absolute top-full right-0 mt-2 w-56 bg-surface/95 backdrop-blur-sm border border-surface-border rounded-lg shadow-xl z-50 overflow-hidden">
                                {/* User Info */}
                                <div className="px-3 py-2 border-b border-surface-border/50">
                                    <div className="font-medium text-text-primary text-sm flex items-center gap-1.5">
                                        <Shield className="w-3.5 h-3.5 text-primary" />
                                        {auth?.role ?? "unknown"}
                                    </div>
                                    <div className="text-[11px] text-text-secondary mt-0.5">Flo Dashboard Session</div>
                                </div>

                                {/* Menu Items */}
                                <div className="py-0.5">
                                    <button className="w-full px-3 py-1.5 flex items-center gap-2 hover:bg-surface-hover/50 transition-colors text-left text-xs text-text-primary">
                                        <span className="text-text-secondary text-sm">⚙️</span>
                                        Account preferences
                                    </button>
                                    <button className="w-full px-3 py-1.5 flex items-center gap-2 hover:bg-surface-hover/50 transition-colors text-left text-xs text-text-primary">
                                        <span className="text-text-secondary text-sm">🧪</span>
                                        Feature previews
                                    </button>
                                </div>

                                {/* Theme Selection */}
                                <div className="border-t border-surface-border/50">
                                    <div className="px-3 py-1.5 text-[10px] font-semibold text-text-secondary uppercase tracking-wide">Theme</div>
                                    <button
                                        onClick={() => {
                                            setTheme('dark');
                                        }}
                                        className="w-full px-3 py-1.5 flex items-center justify-between hover:bg-surface-hover/50 transition-colors text-left"
                                    >
                                        <span className="text-text-primary text-xs">Dark</span>
                                        {theme === 'dark' && <Check className="w-3.5 h-3.5 text-primary" />}
                                    </button>
                                    <button
                                        onClick={() => {
                                            setTheme('light');
                                        }}
                                        className="w-full px-3 py-1.5 flex items-center justify-between hover:bg-surface-hover/50 transition-colors text-left"
                                    >
                                        <span className="text-text-primary text-xs">Light</span>
                                        {theme === 'light' && <Check className="w-3.5 h-3.5 text-primary" />}
                                    </button>
                                </div>

                                {/* Log out */}
                                <div className="border-t border-surface-border/50">
                                    <button
                                        onClick={() => {
                                            setIsProfileOpen(false);
                                            logout();
                                        }}
                                        className="w-full px-3 py-1.5 text-left text-xs text-text-secondary hover:text-error hover:bg-surface-hover/50 transition-colors"
                                    >
                                        Log out
                                    </button>
                                </div>
                            </div>
                        </>
                    )}
                </div>
            </div>
        </header>
    );
}
