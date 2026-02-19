import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

interface GlossaryPreferencesState {
  // Source filtering - stores hidden sources
  hiddenSources: string[];
  
  // Grouping
  groupBySource: boolean;
  
  // Show properties toggle
  showProperties: boolean;
  
  // Group properties by domain
  groupByDomain: boolean;
  
  // UI state
  isFilterExpanded: boolean;
  
  // Actions
  toggleSource: (source: string) => void;
  selectAllSources: () => void;
  selectNoneSources: (allSources: string[]) => void;
  setGroupBySource: (enabled: boolean) => void;
  setShowProperties: (enabled: boolean) => void;
  setGroupByDomain: (enabled: boolean) => void;
  isSourceVisible: (source: string) => boolean;
  setFilterExpanded: (expanded: boolean) => void;
}

export const useGlossaryPreferencesStore = create<GlossaryPreferencesState>()(
  persist(
    (set, get) => ({
      hiddenSources: [],
      groupBySource: false,
      showProperties: false,
      groupByDomain: false,
      isFilterExpanded: true,

      toggleSource: (source: string) => {
        set((state) => {
          const isCurrentlyHidden = state.hiddenSources.includes(source);
          if (isCurrentlyHidden) {
            // Remove from hidden (show it)
            return {
              hiddenSources: state.hiddenSources.filter((s) => s !== source),
            };
          } else {
            // Add to hidden
            return {
              hiddenSources: [...state.hiddenSources, source],
            };
          }
        });
      },

      selectAllSources: () => {
        // Clear all hidden sources - shows all
        set({ hiddenSources: [] });
      },

      selectNoneSources: (allSources: string[]) => {
        // Hide all sources
        set({ hiddenSources: [...allSources] });
      },

      setGroupBySource: (enabled: boolean) => {
        set({ groupBySource: enabled });
      },

      setShowProperties: (enabled: boolean) => {
        set({ showProperties: enabled });
      },

      setGroupByDomain: (enabled: boolean) => {
        set({ groupByDomain: enabled });
      },

      isSourceVisible: (source: string) => {
        return !get().hiddenSources.includes(source);
      },

      setFilterExpanded: (expanded: boolean) => {
        set({ isFilterExpanded: expanded });
      },
    }),
    {
      name: 'glossary-preferences-storage',
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        hiddenSources: state.hiddenSources,
        groupBySource: state.groupBySource,
        showProperties: state.showProperties,
        groupByDomain: state.groupByDomain,
        isFilterExpanded: state.isFilterExpanded,
      }),
    }
  )
);

// Export actions separately for easier usage
export const useGlossaryPreferencesActions = () =>
  useGlossaryPreferencesStore((state) => ({
    toggleSource: state.toggleSource,
    selectAllSources: state.selectAllSources,
    selectNoneSources: state.selectNoneSources,
    setGroupBySource: state.setGroupBySource,
    setShowProperties: state.setShowProperties,
    setGroupByDomain: state.setGroupByDomain,
  }));

