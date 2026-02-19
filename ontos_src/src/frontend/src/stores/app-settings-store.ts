import { create } from 'zustand';

export type TagDisplayFormat = 'short' | 'long';

interface AppSettingsState {
  tagDisplayFormat: TagDisplayFormat;
  isLoading: boolean;
  isLoaded: boolean;
  setTagDisplayFormat: (format: TagDisplayFormat) => void;
  fetchSettings: () => Promise<void>;
}

export const useAppSettingsStore = create<AppSettingsState>()((set, get) => ({
  tagDisplayFormat: 'short', // Default
  isLoading: false,
  isLoaded: false,

  setTagDisplayFormat: (format: TagDisplayFormat) => {
    set({ tagDisplayFormat: format });
  },

  fetchSettings: async () => {
    // Only fetch once
    if (get().isLoaded || get().isLoading) return;
    
    set({ isLoading: true });
    try {
      const response = await fetch('/api/settings');
      if (response.ok) {
        const data = await response.json();
        if (data.tag_display_format) {
          set({ tagDisplayFormat: data.tag_display_format as TagDisplayFormat });
        }
      }
    } catch (err) {
      console.error('Error fetching app settings:', err);
    } finally {
      set({ isLoading: false, isLoaded: true });
    }
  },
}));

