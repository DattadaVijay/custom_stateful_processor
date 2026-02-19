import { useEffect, useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Search, FileText, Database, Book, Shield, Loader2 } from 'lucide-react';
import { features } from '@/config/features';

type IndexSearchResult = {
  id: string;
  type: string;
  title: string;
  description: string;
  link: string;
  feature_id?: string;
  tags?: string[];
};

interface IndexSearchProps {
  initialQuery?: string;
}

export default function IndexSearch({ initialQuery = '' }: IndexSearchProps) {
  const navigate = useNavigate();
  const location = useLocation();
  const { t } = useTranslation(['search', 'common']);

  const [query, setQuery] = useState(initialQuery);
  const [results, setResults] = useState<IndexSearchResult[]>([]);
  const [loading, setLoading] = useState(false);

  // Update URL with query param (simple - only manages its own params)
  const updateUrl = (q: string) => {
    const params = new URLSearchParams();
    if (q) {
      params.set('query', q);
    }
    const queryString = params.toString();
    const newUrl = queryString ? `/search/index?${queryString}` : '/search/index';
    navigate(newUrl, { replace: true });
  };

  // Load initial state from URL
  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const urlQuery = params.get('query');
    if (urlQuery && urlQuery !== initialQuery) {
      setQuery(urlQuery);
    }
  }, [location.search, initialQuery]);

  // Perform search
  useEffect(() => {
    const run = async () => {
      const q = query.trim();
      if (!q) {
        setResults([]);
        updateUrl('');
        return;
      }
      setLoading(true);
      try {
        const resp = await fetch(`/api/search?search_term=${encodeURIComponent(q)}`);
        const data = resp.ok ? await resp.json() : [];
        setResults(Array.isArray(data) ? data : []);
        updateUrl(q);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    };
    const t = setTimeout(run, 300);
    return () => clearTimeout(t);
  }, [query]);

  // Get icon for result based on feature_id or type fallback
  const getIcon = (result: IndexSearchResult) => {
    // Prefer explicit feature-based icon mapping to keep UI consistent with navigation
    if (result.feature_id) {
      const feature = features.find((f) => f.id === result.feature_id);
      if (feature) {
        const Icon = feature.icon;
        return <Icon className="h-4 w-4 flex-shrink-0" />;
      }
    }

    // Fallbacks based on type
    switch (result.type) {
      case 'data-product':
        return <Database className="h-4 w-4 flex-shrink-0" />;
      case 'data-contract':
        return <FileText className="h-4 w-4 flex-shrink-0" />;
      case 'glossary-term':
        return <Book className="h-4 w-4 flex-shrink-0" />;
      case 'persona':
        return <Shield className="h-4 w-4 flex-shrink-0" />;
      default:
        return <Search className="h-4 w-4 flex-shrink-0" />;
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{t('search:index.title')}</CardTitle>
        <CardDescription className="text-xs">{t('search:index.description')}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="relative">
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={t('common:placeholders.searchDataProductsTermsContracts')}
            className="h-9 text-sm"
          />
        </div>
        <div className="space-y-2 text-sm">
          {loading ? (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              {t('common:labels.loading')}
            </div>
          ) : results.length === 0 ? (
            <div className="text-xs text-muted-foreground">{t('search:noResults')}</div>
          ) : (
            results.map(r => (
              <a 
                key={r.id} 
                href={r.link} 
                className="flex items-center gap-3 p-2 rounded hover:bg-accent"
              >
                {getIcon(r)}
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-medium">{r.title}</div>
                  <div className="text-xs text-muted-foreground truncate">{r.description}</div>
                </div>
              </a>
            ))
          )}
        </div>
      </CardContent>
    </Card>
  );
}
