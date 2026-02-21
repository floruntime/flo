import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AppLayout } from "./layouts/AppLayout";
import { NamespaceProvider } from "./lib/NamespaceContext";
import { ClusterOverview } from "./pages/ClusterOverview";
import { StreamsList } from "./pages/StreamsList";
import { ActionsList } from "./pages/ActionsList";
import { ActionDetailPage } from "./pages/ActionDetail";
import { WorkersListPage } from "./pages/WorkersList";
import { StreamDetail } from "./pages/StreamDetail.tsx";
import { KeyDetail } from "./pages/KeyDetail";
import { QueuesList } from "./pages/QueuesList";
import { QueueDetail } from "./pages/QueueDetail";
import { ProcessingList } from "./pages/ProcessingList";
import { ProcessingDetail } from "./pages/ProcessingDetail";
import { WorkflowsListPage } from "./pages/WorkflowsListPage";
import { WorkflowDetailPage } from "./pages/WorkflowDetailPage";
import { WorkflowDefinitionsPage } from "./pages/WorkflowDefinitionsPage";
import { WorkflowLayout } from "./layouts/WorkflowLayout";
import { TimeSeriesList } from "./pages/TimeSeriesList";
import { TimeSeriesDetail } from "./pages/TimeSeriesDetail";

function App() {
  return (
    <BrowserRouter>
      <NamespaceProvider>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<ClusterOverview />} />
          <Route path="/streams" element={<StreamsList />} />
          <Route path="/streams/:streamId" element={<StreamDetail />} />

          {/* KV Routes — namespace controlled by header selector */}
          <Route path="/kv" element={<KeyDetail />} />
          <Route path="/kv/:namespace" element={<KeyDetail />} />
          <Route path="/kv/:namespace/:key" element={<KeyDetail />} />

          <Route path="/queues" element={<QueuesList />} />
          <Route path="/queues/:queueName" element={<QueueDetail />} />
          <Route path="/timeseries" element={<TimeSeriesList />} />
          <Route path="/timeseries/:measurement" element={<TimeSeriesDetail />} />
          <Route path="/actions" element={<ActionsList />} />
          <Route path="/actions/:actionName" element={<ActionDetailPage />} />
          <Route path="/workers" element={<WorkersListPage />} />
          <Route path="/processing" element={<ProcessingList />} />
          <Route path="/processing/:jobId" element={<ProcessingDetail />} />

          {/* Workflow Routes – WorkflowLayout keeps the sidebar mounted across pages */}
          <Route element={<WorkflowLayout />}>
            <Route path="/workflows" element={<WorkflowsListPage />} />
            <Route path="/workflows/runs/:runId" element={<WorkflowDetailPage />} />
            <Route path="/workflows/definitions" element={<WorkflowDefinitionsPage />} />
          </Route>

          <Route path="/settings" element={<div className="p-4">Settings Page (Coming Soon)</div>} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
      </NamespaceProvider>
    </BrowserRouter>
  );
}

export default App;
