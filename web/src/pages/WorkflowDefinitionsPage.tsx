import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  GitGraph,
  FileCode,
  Radio,
  Shield,
  Layers,
  ChevronDown,
  ChevronRight,
  CalendarClock,
  Search,
} from 'lucide-react';
import { Card } from '../components/ui/Card';
import { cn } from '../lib/utils';
import { useWorkflowDefinitions } from '../lib/workflow-hooks';
import type { DefinitionListEntry } from '../lib/workflow-api';
import * as workflowApi from '../lib/workflow-api';
import { Button } from '../components/ui/Button';

// =============================================================================
// Helpers
// =============================================================================

function formatDate(ts: number): string {
  return new Date(ts).toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
}

// =============================================================================
// Definition Card
// =============================================================================

function DefinitionCard({ def, onToggle }: { def: DefinitionListEntry; onToggle: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const [toggling, setToggling] = useState(false);
  const navigate = useNavigate();

  const handleToggle = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    setToggling(true);
    try {
      if (def.enabled) {
        await workflowApi.disableDefinition(def.name);
      } else {
        await workflowApi.enableDefinition(def.name);
      }
      onToggle();
    } catch (err) {
      console.error('Failed to toggle definition:', err);
    } finally {
      setToggling(false);
    }
  }, [def.enabled, def.name, onToggle]);

  return (
    <Card className="overflow-hidden">
      {/* Header */}
      <div
        className="flex items-center justify-between p-4 cursor-pointer hover:bg-surface-hover transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-3">
          <div className="rounded-md p-2 bg-primary/10">
            <GitGraph className="w-4 h-4 text-primary" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-semibold">{def.name}</span>
              <span className="text-[10px] text-text-secondary font-mono">v{def.version}</span>
              <span className={cn(
                'text-[10px] px-2 py-0.5 rounded-full font-medium',
                def.enabled ? 'bg-success/10 text-success' : 'bg-error/10 text-error'
              )}>
                {def.enabled ? 'Enabled' : 'Disabled'}
              </span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-4">
          {/* Quick Stats */}
          <div className="hidden md:flex items-center gap-3 text-[10px] text-text-secondary">
            <span className="flex items-center gap-1">
              <Layers className="w-3 h-3" /> {def.step_count} steps
            </span>
            {def.plan_count > 0 && (
              <span className="flex items-center gap-1">
                <Shield className="w-3 h-3" /> {def.plan_count} plans
              </span>
            )}
            {def.has_schedule && (
              <span className="flex items-center gap-1">
                <CalendarClock className="w-3 h-3 text-primary" /> Scheduled
              </span>
            )}
            {def.has_trigger && (
              <span className="flex items-center gap-1">
                <Radio className="w-3 h-3 text-purple-500" /> Trigger
              </span>
            )}
          </div>

          <ChevronDown className={cn('w-4 h-4 text-text-secondary transition-transform', expanded && 'rotate-180')} />
        </div>
      </div>

      {/* Expanded Content */}
      {expanded && (
        <div className="border-t border-surface-border">
          <div className="p-4 space-y-4">
            {/* Metadata Grid */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <MetaItem label="Start Step" value={def.start_step} mono />
              <MetaItem label="Created" value={formatDate(def.created_at)} />
              <MetaItem label="Steps" value={`${def.step_count} total`} />
              <MetaItem label="Plans" value={`${def.plan_count} total`} />
              <MetaItem label="Schedule" value={def.has_schedule ? 'Yes' : 'No'} />
              <MetaItem label="Trigger" value={def.has_trigger ? 'Yes' : 'No'} />
              <MetaItem label="Terminals" value={def.terminals.map(t => t.name).join(', ')} />
              <MetaItem label="Enabled" value={def.enabled ? 'Active' : 'Disabled'} />
            </div>

            {/* Step Flow */}
            <div>
              <h4 className="text-xs uppercase tracking-wider text-text-secondary font-medium mb-2">Step Flow</h4>
              <StepFlowDiagram def={def} />
            </div>

            {/* Action Buttons */}
            <div className="flex items-center justify-between pt-2 border-t border-surface-border">
              <button
                onClick={() => navigate(`/workflows?workflow=${def.name}`)}
                className="text-xs text-primary hover:text-primary-hover transition-colors"
              >
                View Runs &rarr;
              </button>
              <div className="flex items-center gap-2">
                <button
                  onClick={handleToggle}
                  disabled={toggling}
                  className={cn(
                  'text-xs px-3 py-1.5 rounded-md transition-colors border',
                  def.enabled
                    ? 'text-error border-error/20 hover:bg-error/10'
                    : 'text-success border-success/20 hover:bg-success/10',
                  toggling && 'opacity-50 cursor-not-allowed'
                )}>
                  {toggling ? '...' : def.enabled ? 'Disable' : 'Enable'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}

// =============================================================================
// Sub-components
// =============================================================================

function MetaItem({ label, value, sub, mono }: { label: string; value: string; sub?: string; mono?: boolean }) {
  return (
    <div className="bg-background rounded p-2 border border-surface-border">
      <p className="text-[10px] uppercase tracking-wider text-text-secondary">{label}</p>
      <p className={cn('text-xs font-medium truncate', mono && 'font-mono')}>{value}</p>
      {sub && <p className="text-[9px] text-text-secondary">{sub}</p>}
    </div>
  );
}

function StepFlowDiagram({ def }: { def: DefinitionListEntry }) {
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {/* Start step */}
      <div className="rounded px-2 py-1 border border-primary/30 bg-primary/5 text-xs font-mono text-primary font-medium">
        {def.start_step}
      </div>
      {def.steps.map((step) => (
        <div key={step.name} className="flex items-center gap-1.5">
          <ChevronRight className="w-3 h-3 text-surface-border shrink-0" />
          <div className="rounded px-2 py-1 border border-blue-400/30 bg-blue-400/5 text-xs font-mono">
            {step.name}
          </div>
        </div>
      ))}
      {/* Terminals */}
      {def.terminals.map((t) => (
        <div key={t.name} className="flex items-center gap-1.5">
          <ChevronRight className="w-3 h-3 text-surface-border shrink-0" />
          <div className="rounded px-2 py-1 border border-success/30 bg-success/5 text-xs text-success font-medium">
            {t.name}
          </div>
        </div>
      ))}
    </div>
  );
}

// =============================================================================
// Create Definition Modal
// =============================================================================

function CreateDefinitionModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [yaml, setYaml] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = useCallback(async () => {
    if (!yaml.trim()) return;
    setSubmitting(true);
    setError(null);
    try {
      await workflowApi.createDefinition(yaml);
      onCreated();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }, [yaml, onClose, onCreated]);

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-surface rounded-lg border border-surface-border p-6 w-[600px] max-h-[80vh] flex flex-col gap-4" onClick={e => e.stopPropagation()}>
        <h3 className="text-lg font-semibold">Register Workflow Definition</h3>
        <p className="text-sm text-text-secondary">Paste your workflow definition YAML below.</p>
        {error && (
          <div className="text-sm text-error bg-error/10 rounded-md px-3 py-2">{error}</div>
        )}
        <textarea
          value={yaml}
          onChange={e => setYaml(e.target.value)}
          placeholder="name: my-workflow\nversion: 1\nsteps:\n  - name: step1\n    action: ...\n"
          rows={16}
          className="flex-1 bg-background border border-surface-border rounded-md px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary resize-none"
        />
        <div className="flex justify-end gap-2">
          <Button variant="ghost" onClick={onClose}>Cancel</Button>
          <Button variant="primary" onClick={handleSubmit} disabled={submitting || !yaml.trim()}>
            {submitting ? 'Registering...' : 'Register'}
          </Button>
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// Main Page
// =============================================================================

export function WorkflowDefinitionsPage() {
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const { definitions, refetch } = useWorkflowDefinitions();

  const filtered = definitions.filter((def) => {
    if (!searchQuery) return true;
    const q = searchQuery.toLowerCase();
    return def.name.toLowerCase().includes(q);
  });

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            onClick={() => navigate('/workflows')}
            className="text-text-secondary hover:text-text-primary transition-colors"
          >
            <ArrowLeft className="w-5 h-5" />
          </button>
          <div>
            <h2 className="text-3xl font-bold tracking-tight">Workflow Definitions</h2>
            <p className="text-sm text-text-secondary mt-1">
              {definitions.length} registered workflows
            </p>
          </div>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="bg-primary hover:bg-primary-hover text-background font-medium px-4 py-2 rounded-md transition-colors text-sm"
        >
          Register Workflow
        </button>
      </div>

      {/* Search */}
      <div className="relative max-w-md">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-text-secondary" />
        <input
          type="text"
          placeholder="Search definitions..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-9 pr-3 py-2 text-sm bg-surface border border-surface-border rounded-md focus:outline-none focus:ring-1 focus:ring-primary text-text-primary placeholder:text-text-secondary/60"
        />
      </div>

      {/* Definition Cards */}
      <div className="space-y-3">
        {filtered.map((def) => (
          <DefinitionCard key={`${def.name}:${def.version}`} def={def} onToggle={refetch} />
        ))}
        {filtered.length === 0 && (
          <div className="text-center py-12 text-text-secondary">
            <FileCode className="w-10 h-10 mx-auto mb-3 opacity-40" />
            <p className="text-sm">No definitions match your search.</p>
          </div>
        )}
      </div>

      {/* Create Definition Modal */}
      {showCreate && <CreateDefinitionModal onClose={() => setShowCreate(false)} onCreated={refetch} />}
    </div>
  );
}
