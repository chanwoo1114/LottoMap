import { RouterProvider } from 'react-router-dom';
import { router } from './router';
import { AuthProvider } from '@/features/auth/AuthContext';
import { FavoritesProvider } from '@/features/store/FavoritesContext';

export function App() {
  return (
    <AuthProvider>
      <FavoritesProvider>
        <RouterProvider router={router} />
      </FavoritesProvider>
    </AuthProvider>
  );
}