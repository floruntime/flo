import { useState, useEffect, type FormEvent } from "react";
import { Zap, AlertCircle, Eye, EyeOff } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../lib/AuthContext";

export function LoginPage() {
  const { login, loggingIn, loginError, isAuthenticated, authRequired, authChecked } = useAuth();
  const navigate = useNavigate();
  const [apiKey, setApiKey] = useState("");
  const [showKey, setShowKey] = useState(false);

  // Redirect away from login if auth isn't required or already authenticated
  useEffect(() => {
    if (authChecked && (!authRequired || isAuthenticated)) {
      navigate("/", { replace: true });
    }
  }, [authChecked, authRequired, isAuthenticated, navigate]);

  // Apply saved theme so login page matches the user's preference
  useEffect(() => {
    const saved = localStorage.getItem("theme") as "dark" | "light" | null;
    document.documentElement.dataset.theme = saved || "dark";
  }, []);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!apiKey.trim()) return;
    await login(apiKey.trim());
  };

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <div className="w-full max-w-md space-y-8">
        {/* Logo */}
        <div className="text-center space-y-2">
          <div className="w-12 h-12 rounded-xl bg-primary/20 text-primary flex items-center justify-center mx-auto">
            <Zap className="w-6 h-6 fill-current" />
          </div>
          <h1 className="text-2xl font-bold text-text-primary">Flo Dashboard</h1>
          <p className="text-sm text-text-secondary">
            Enter your API key to access the dashboard
          </p>
        </div>

        {/* Login Form */}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <label htmlFor="api-key" className="text-sm font-medium text-text-primary">
              API Key
            </label>
            <div className="relative">
              <input
                id="api-key"
                type={showKey ? "text" : "password"}
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="flo_sk_admin_..."
                autoComplete="off"
                spellCheck={false}
                className="w-full px-3 py-2.5 pr-10 bg-surface border border-surface-border rounded-lg text-sm text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary transition-colors font-mono"
              />
              <button
                type="button"
                onClick={() => setShowKey(!showKey)}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-text-secondary hover:text-text-primary transition-colors"
              >
                {showKey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
          </div>

          {loginError && (
            <div className="flex items-start gap-2 p-3 bg-error/10 border border-error/20 rounded-lg">
              <AlertCircle className="w-4 h-4 text-error flex-shrink-0 mt-0.5" />
              <p className="text-sm text-error">{loginError}</p>
            </div>
          )}

          <button
            type="submit"
            disabled={loggingIn || !apiKey.trim()}
            className="w-full py-2.5 bg-primary hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer text-white text-sm font-medium rounded-lg transition-colors flex items-center justify-center gap-2"
          >
            {loggingIn ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Authenticating...
              </>
            ) : (
              "Sign in"
            )}
          </button>
        </form>

        {/* Help Text */}
        <div className="text-center space-y-1">
          <p className="text-xs text-text-secondary">
            Generate an API key with{" "}
            <code className="px-1.5 py-0.5 bg-surface rounded text-text-primary font-mono text-[11px]">
              flo server bootstrap
            </code>
          </p>
          <p className="text-xs text-text-secondary">
            or{" "}
            <code className="px-1.5 py-0.5 bg-surface rounded text-text-primary font-mono text-[11px]">
              flo auth create-key --role admin --name dashboard
            </code>
          </p>
        </div>
      </div>
    </div>
  );
}
