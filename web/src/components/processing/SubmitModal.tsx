import { useState } from "react";
import { X, Upload, FileCode, AlertCircle, Loader2, CheckCircle } from "lucide-react";
import { cn } from "../../lib/utils";
import { api } from "../../lib/api";

// =============================================================================
// YAML Templates
// =============================================================================

const TEMPLATES: { name: string; description: string; yaml: string }[] = [
    {
        name: 'Stream Filter → Stream',
        description: 'Filter events from a source stream and write matches to an output stream',
        yaml: `kind: Processing
name: my-filter-pipeline
parallelism: 4
batch_size: 128

source:
  name: input-source
  stream: my-input-stream
  namespace: default
  partition: all

operators:
  - type: filter
    name: my-filter
    config:
      - key: condition
        value: "json:level=ERROR"

sink:
  name: output-sink
  kind: stream
  target: filtered-output
  namespace: default

checkpointing:
  interval_ms: 30000`,
    },
    {
        name: 'Aggregate → KV Store',
        description: 'Group by key, aggregate with tumbling window, materialize to KV',
        yaml: `kind: Processing
name: my-aggregation
parallelism: 2
batch_size: 64

source:
  name: events-source
  stream: events
  namespace: default
  partition: all

operators:
  - type: keyby
    name: group-by-key
    config:
      - key: field
        value: category

  - type: aggregate
    name: counter
    config:
      - key: function
        value: count
      - key: window
        value: tumbling_time
      - key: window_size_ms
        value: "60000"

sink:
  name: state-sink
  kind: kv
  target: aggregated-counts
  namespace: default
  key_prefix: "agg"
  separator: ":"
  write_mode: upsert

checkpointing:
  interval_ms: 15000`,
    },
    {
        name: 'WASM Transform → Queue',
        description: 'Process records through a WASM module and enqueue results',
        yaml: `kind: Processing
name: wasm-transform
parallelism: 8
batch_size: 32

source:
  name: data-source
  stream: raw-data
  namespace: default
  partition: all

operators:
  - type: wasm
    name: my-transform
    module: /actions/transform.wasm
    config:
      - key: param
        value: "custom_value"

sink:
  name: queue-sink
  kind: queue
  target: processed-jobs
  namespace: default
  priority: 2

checkpointing:
  interval_ms: 10000`,
    },
    {
        name: 'FlatMap → Filter → Stream',
        description: 'Explode array fields, filter results, write to output stream',
        yaml: `kind: Processing
name: flatmap-pipeline
parallelism: 4
batch_size: 128

source:
  name: orders-source
  stream: orders
  namespace: default
  partition: all

operators:
  - type: flatmap
    name: split-items
    config:
      - key: array_field
        value: line_items
      - key: preserve_parent
        value: "true"

  - type: map
    name: extract-fields
    config:
      - key: fields
        value: "item_id,quantity,price,order_id"

  - type: filter
    name: high-value
    config:
      - key: condition
        value: "json:price>100"

sink:
  name: high-value-items
  kind: stream
  target: high-value-orders
  namespace: default

checkpointing:
  interval_ms: 30000`,
    },
];

// =============================================================================
// Component
// =============================================================================

interface Props {
    onClose: () => void;
    onSubmitted?: () => void;
}

export function ProcessingSubmitModal({ onClose, onSubmitted }: Props) {
    const [yaml, setYaml] = useState('');
    const [error, setError] = useState<string | null>(null);
    const [selectedTemplate, setSelectedTemplate] = useState<number | null>(null);
    const [submitting, setSubmitting] = useState(false);
    const [submitResult, setSubmitResult] = useState<{ job_id: string } | null>(null);

    function handleTemplateSelect(index: number) {
        setSelectedTemplate(index);
        setYaml(TEMPLATES[index].yaml);
        setError(null);
    }

    async function handleSubmit() {
        if (!yaml.trim()) {
            setError('Please enter a YAML pipeline definition.');
            return;
        }
        if (!yaml.includes('kind: Processing')) {
            setError('YAML must include "kind: Processing" at the top level.');
            return;
        }
        if (!yaml.includes('source:')) {
            setError('YAML must define a "source:" section.');
            return;
        }
        if (!yaml.includes('sink:')) {
            setError('YAML must define a "sink:" section.');
            return;
        }

        setSubmitting(true);
        setError(null);
        try {
            const result = await api.submitProcessingJob(yaml);
            setSubmitResult(result);
            onSubmitted?.();
            // Auto-close after brief success display
            setTimeout(() => onClose(), 1500);
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to submit pipeline');
        } finally {
            setSubmitting(false);
        }
    }

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={onClose}>
            <div
                className="bg-surface border border-surface-border rounded-lg shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col"
                onClick={e => e.stopPropagation()}
            >
                {/* Header */}
                <div className="flex items-center justify-between px-6 py-4 border-b border-surface-border">
                    <div className="flex items-center gap-3">
                        <FileCode className="w-5 h-5 text-primary" />
                        <div>
                            <h2 className="text-lg font-semibold text-text-primary">Submit Processing Pipeline</h2>
                            <p className="text-xs text-text-secondary">Define your pipeline using YAML, or start from a template.</p>
                        </div>
                    </div>
                    <button onClick={onClose} className="p-1 hover:bg-surface-hover rounded-md transition-colors">
                        <X className="w-5 h-5 text-text-secondary" />
                    </button>
                </div>

                {/* Body */}
                <div className="flex flex-1 overflow-hidden">
                    {/* Templates Sidebar */}
                    <div className="w-64 border-r border-surface-border bg-background p-4 overflow-y-auto shrink-0">
                        <p className="text-xs text-text-secondary uppercase tracking-wider mb-3 font-medium">Templates</p>
                        <div className="space-y-2">
                            {TEMPLATES.map((t, i) => (
                                <button
                                    key={i}
                                    onClick={() => handleTemplateSelect(i)}
                                    className={cn(
                                        "w-full text-left p-3 rounded-md border transition-colors",
                                        selectedTemplate === i
                                            ? "border-primary/50 bg-primary/5 text-text-primary"
                                            : "border-transparent hover:bg-surface-hover text-text-secondary hover:text-text-primary"
                                    )}
                                >
                                    <p className="text-sm font-medium">{t.name}</p>
                                    <p className="text-xs text-text-secondary mt-0.5">{t.description}</p>
                                </button>
                            ))}
                        </div>
                    </div>

                    {/* YAML Editor */}
                    <div className="flex-1 flex flex-col p-4 overflow-hidden">
                        <div className="flex items-center justify-between mb-2">
                            <p className="text-xs text-text-secondary uppercase tracking-wider font-medium">Pipeline YAML</p>
                            <span className="text-[10px] text-text-secondary font-mono">{yaml.split('\n').length} lines</span>
                        </div>
                        <textarea
                            value={yaml}
                            onChange={(e) => { setYaml(e.target.value); setError(null); }}
                            placeholder={`kind: Processing\nname: my-pipeline\nparallelism: 4\n\nsource:\n  name: input\n  stream: my-stream\n  partition: all\n\noperators:\n  - type: filter\n    name: my-filter\n    config:\n      - key: condition\n        value: "not_empty"\n\nsink:\n  name: output\n  kind: stream\n  target: output-stream`}
                            className={cn(
                                "flex-1 font-mono text-sm leading-relaxed p-4 rounded-md border bg-background text-text-primary resize-none focus:outline-none focus:ring-1 focus:ring-primary",
                                "placeholder:text-text-secondary/40",
                                error ? "border-error/50" : "border-surface-border"
                            )}
                            spellCheck={false}
                        />
                        {error && (
                            <div className="flex items-center gap-2 mt-2 text-xs text-error">
                                <AlertCircle className="w-3.5 h-3.5 shrink-0" />
                                {error}
                            </div>
                        )}
                    </div>
                </div>

                {/* Footer */}
                <div className="flex items-center justify-between px-6 py-4 border-t border-surface-border">
                    <div className="flex items-center gap-2 text-xs text-text-secondary">
                        {submitResult ? (
                            <>
                                <CheckCircle className="w-3.5 h-3.5 text-success" />
                                <span className="text-success">Pipeline submitted: <span className="font-mono">{submitResult.job_id}</span></span>
                            </>
                        ) : (
                            <>
                                <FileCode className="w-3.5 h-3.5" />
                                <span>Supported operators: filter, map, flatmap, keyby, aggregate, passthrough, wasm</span>
                            </>
                        )}
                    </div>
                    <div className="flex items-center gap-3">
                        <button
                            onClick={onClose}
                            className="px-4 py-2 text-sm text-text-secondary hover:text-text-primary transition-colors"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={handleSubmit}
                            disabled={submitting || !!submitResult}
                            className="flex items-center gap-2 bg-primary hover:bg-primary/90 text-background font-medium px-4 py-2 rounded-md transition-colors text-sm disabled:opacity-50"
                        >
                            {submitting ? (
                                <><Loader2 className="w-4 h-4 animate-spin" /> Submitting…</>
                            ) : submitResult ? (
                                <><CheckCircle className="w-4 h-4" /> Submitted</>
                            ) : (
                                <><Upload className="w-4 h-4" /> Submit</>
                            )}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
