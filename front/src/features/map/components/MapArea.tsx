import type { RefObject } from 'react'
import { BiCurrentLocation } from 'react-icons/bi'
import { IconButton } from '@/components/ui/IconButton'

interface MapAreaProps {
  containerRef: RefObject<HTMLDivElement | null>;
  tooFar: boolean;
  onMyLocation: () => void;
}

export function MapArea({ containerRef, tooFar, onMyLocation }: MapAreaProps) {
  return (
    <div className="relative flex-1 overflow-hidden">
      <div ref={containerRef} className="h-full w-full" />

      {tooFar && (
        <div className="pointer-events-none absolute inset-x-0 top-4 z-10 flex justify-center">
          <div className="rounded-full bg-gray-900/80 px-4 py-2 text-sm font-medium text-white shadow-md">
            지도를 확대하면 판매점이 표시됩니다
          </div>
        </div>
      )}

      <IconButton onClick={onMyLocation} aria-label="내 위치로 이동" className="absolute bottom-20 right-4 z-10 md:bottom-6">
        <BiCurrentLocation className="h-5 w-5" />
      </IconButton>
    </div>
  )
}