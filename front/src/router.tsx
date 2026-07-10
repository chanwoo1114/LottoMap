import { createBrowserRouter } from 'react-router-dom';
import { AppShell } from '@/components/layout/AppShell';
import { MapScreen } from '@/features/map/MapScreen';
import { ResultsPage } from '@/features/results/ResultsPage';
import { GeneratePage } from '@/features/generate/GeneratePage';
import { MyNumbersPage } from '@/features/savedNumbers/MyNumbersPage';

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { path: '/', element: <MapScreen /> },
      { path: '/results', element: <ResultsPage /> },
      { path: '/generate', element: <GeneratePage /> },
      { path: '/my-numbers', element: <MyNumbersPage /> },
    ],
  },
]);