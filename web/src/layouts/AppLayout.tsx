import { Outlet, useLocation } from "react-router-dom";
import { Sidebar } from "../components/Sidebar";
import { Header } from "../components/Header";

export function AppLayout() {
    const { pathname } = useLocation();
    // Workflow pages manage their own layout (sidebar + scrolling)
    const isFullBleed = pathname.startsWith('/workflows');

    return (
        <div className="flex flex-col h-screen w-full bg-background text-text-primary font-sans overflow-hidden">
            <Header />
            <div className="flex flex-1 overflow-hidden relative">
                <Sidebar />
                <main className={`flex-1 ${isFullBleed ? 'overflow-hidden flex flex-col' : 'overflow-y-auto'}`}>
                    {isFullBleed ? (
                        <Outlet />
                    ) : (
                        <div className="container mx-auto p-8 max-w-7xl">
                            <Outlet />
                        </div>
                    )}
                </main>
            </div>
        </div>
    );
}
