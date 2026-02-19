import { useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Search as SearchIcon } from 'lucide-react';
import useBreadcrumbStore from '@/stores/breadcrumb-store';
import IndexSearch from '@/components/search/index-search';
import KGSearch from '@/components/search/kg-search';
import ConceptsSearch from '@/components/search/concepts-search';
import PropertiesSearch from '@/components/search/properties-search';
import LLMSearch from '@/components/search/llm-search';

// Map URL slugs to tab values
const SLUG_TO_TAB: Record<string, 'llm' | 'index' | 'concepts' | 'properties' | 'kg'> = {
  'llm': 'llm',
  'index': 'index',
  'concepts': 'concepts',
  'properties': 'properties',
  'kg': 'kg',
};

// Default tab when visiting /search
const DEFAULT_TAB = 'llm';

export default function SearchView() {
  const { t } = useTranslation(['search', 'common']);
  const location = useLocation();
  const navigate = useNavigate();
  const setStaticSegments = useBreadcrumbStore((state) => state.setStaticSegments);
  const setDynamicTitle = useBreadcrumbStore((state) => state.setDynamicTitle);

  // Extract current tab from URL path
  const pathParts = location.pathname.split('/').filter(Boolean);
  const currentSlug = pathParts.length > 1 ? pathParts[1] : '';
  const currentTab = SLUG_TO_TAB[currentSlug] || DEFAULT_TAB;

  useEffect(() => {
    setStaticSegments([]);
    setDynamicTitle(t('title'));
    return () => {
      setStaticSegments([]);
      setDynamicTitle(null);
    };
  }, [setStaticSegments, setDynamicTitle]);

  // Redirect /search to /search/llm (default tab)
  useEffect(() => {
    if (location.pathname === '/search' || location.pathname === '/search/') {
      navigate('/search/llm', { replace: true });
    }
  }, [location.pathname, navigate]);

  // Handle tab change - navigate to new slug, preserving only query params for that tab
  const handleModeChange = (newMode: 'llm' | 'index' | 'concepts' | 'properties' | 'kg') => {
    navigate(`/search/${newMode}`);
  };

  // Extract query params for each tab (they only matter for their respective components)
  // Each tab uses simple param names since they're isolated by their URL path
  const params = new URLSearchParams(location.search);

  // Index Search params (used at /search/index)
  const indexQuery = params.get('query') || '';

  // KG Search params (used at /search/kg)
  const kgPrefix = params.get('prefix') || '';
  const kgPath = params.get('path')?.split('|').filter(Boolean) || [];
  const kgSparql = params.get('sparql') || 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10';
  const kgDirection = params.get('direction') as 'all' | 'incoming' | 'outgoing' || 'all';
  const kgConceptsOnly = params.get('concepts_only') === 'true';

  // Concepts Search params (used at /search/concepts)
  const conceptsQuery = params.get('query') || '';
  const conceptsIri = params.get('iri');

  // Properties Search params (used at /search/properties)
  const propertiesQuery = params.get('query') || '';
  const propertiesIri = params.get('iri');

  // Create initial concept for concepts search
  const initialConcept = conceptsIri ? {
    value: conceptsIri,
    label: conceptsIri.split('/').pop() || conceptsIri.split('#').pop() || conceptsIri,
    type: 'class' as const
  } : null;

  const initialProperty = propertiesIri ? {
    value: propertiesIri,
    label: propertiesIri.split('/').pop() || propertiesIri.split('#').pop() || propertiesIri,
    type: 'property' as const
  } : null;

  return (
    <div className="py-4 space-y-4">
      <h1 className="text-3xl font-bold mb-4 flex items-center gap-2">
        <SearchIcon className="w-8 h-8" />
        {t('title')}
      </h1>
      <Tabs value={currentTab} onValueChange={(v) => handleModeChange(v as 'llm' | 'index' | 'concepts' | 'properties' | 'kg')}>
        <TabsList>
          <TabsTrigger value="llm">{t('tabs.askOntos')}</TabsTrigger>
          <TabsTrigger value="index">{t('tabs.indexSearch')}</TabsTrigger>
          <TabsTrigger value="kg">{t('tabs.knowledgeGraph')}</TabsTrigger>
          <TabsTrigger value="concepts">{t('tabs.concepts')}</TabsTrigger>
          <TabsTrigger value="properties">{t('tabs.properties')}</TabsTrigger>
        </TabsList>

        <TabsContent value="llm">
          <LLMSearch />
        </TabsContent>

        <TabsContent value="index">
          <IndexSearch initialQuery={indexQuery} />
        </TabsContent>

        <TabsContent value="kg">
          <KGSearch
            initialPrefix={kgPrefix}
            initialPath={kgPath}
            initialSparql={kgSparql}
            initialDirectionFilter={kgDirection}
            initialShowConceptsOnly={kgConceptsOnly}
          />
        </TabsContent>

        <TabsContent value="concepts">
          <ConceptsSearch
            initialQuery={conceptsQuery}
            initialSelectedConcept={initialConcept}
          />
        </TabsContent>

        <TabsContent value="properties">
          <PropertiesSearch
            initialQuery={propertiesQuery}
            initialSelectedProperty={initialProperty}
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}
