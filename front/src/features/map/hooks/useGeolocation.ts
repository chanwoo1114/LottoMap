import { useEffect, useState } from 'react';

export interface Coords {
  lat: number;
  lng: number;
}

const DEFAULT_COORDS: Coords = { lat: 37.565694, lng: 126.977139 };

export function useGeolocation() {
  const [location, setLocation] = useState<Coords>(DEFAULT_COORDS);

  useEffect(() => {
    if (!navigator.geolocation) return;

    const watchId = navigator.geolocation.watchPosition(
      (position) => {
        setLocation({
          lat: position.coords.latitude,
          lng: position.coords.longitude,
        });
      },
      (error) => console.error(error),
      { enableHighAccuracy: true, maximumAge: 1000, timeout: 10000 },
    );

    return () => navigator.geolocation.clearWatch(watchId);
  }, []);

  return location;
}