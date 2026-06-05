import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { NamespaceProvider } from '@/lib/namespace'
import { Shell } from '@/layout/Shell'
import { Overview } from '@/screens/Overview'
import { Streams } from '@/screens/streams/Streams'
import { KV } from '@/screens/kv/KV'
import { Queues } from '@/screens/queues/Queues'
import { TimeSeries } from '@/screens/timeseries/TimeSeries'
import { Actions } from '@/screens/compute/Actions'
import { Workers } from '@/screens/compute/Workers'
import { Processing } from '@/screens/processing/Processing'
import { Workflows } from '@/screens/workflows/Workflows'
import { Playground } from '@/playground/Playground'

function Console() {
  return (
    <NamespaceProvider>
      <Shell>
        <Routes>
          <Route index element={<Overview />} />
          <Route path="streams" element={<Streams />} />
          <Route path="kv" element={<KV />} />
          <Route path="queues" element={<Queues />} />
          <Route path="timeseries" element={<TimeSeries />} />
          <Route path="actions" element={<Actions />} />
          <Route path="workers" element={<Workers />} />
          <Route path="processing" element={<Processing />} />
          <Route path="workflows" element={<Workflows />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Shell>
    </NamespaceProvider>
  )
}

export function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/playground/*" element={<Playground />} />
        <Route path="/*" element={<Console />} />
      </Routes>
    </BrowserRouter>
  )
}
