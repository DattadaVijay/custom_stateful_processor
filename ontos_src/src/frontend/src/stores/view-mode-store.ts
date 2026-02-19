import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type ViewMode = 'consumer' | 'management';
export type DomainBrowserStyle = 'pills' | 'graph';
export type TilesPerRow = 1 | 2 | 3 | 4;

interface ViewModeState {
  viewMode: ViewMode;
  domainBrowserStyle: DomainBrowserStyle;
  tilesPerRow: TilesPerRow;
  setViewMode: (mode: ViewMode) => void;
  setDomainBrowserStyle: (style: DomainBrowserStyle) => void;
  setTilesPerRow: (count: TilesPerRow) => void;
  toggleViewMode: () => void;
}

export const useViewModeStore = create<ViewModeState>()(
  persist(
    (set, get) => ({
      viewMode: 'consumer', // Default to consumer/marketplace view
      domainBrowserStyle: 'pills', // Default to pills view
      tilesPerRow: 4, // Default to 4 tiles per row (current behavior)
      
      setViewMode: (mode: ViewMode) => {
        set({ viewMode: mode });
      },
      
      setDomainBrowserStyle: (style: DomainBrowserStyle) => {
        set({ domainBrowserStyle: style });
      },
      
      setTilesPerRow: (count: TilesPerRow) => {
        set({ tilesPerRow: count });
      },
      
      toggleViewMode: () => {
        const current = get().viewMode;
        set({ viewMode: current === 'consumer' ? 'management' : 'consumer' });
      },
    }),
    {
      name: 'view-mode-storage', // localStorage key
    }
  )
);

