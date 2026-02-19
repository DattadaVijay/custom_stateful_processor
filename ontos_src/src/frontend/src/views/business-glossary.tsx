import { useState, useEffect, useCallback, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  FolderTree,
  Layers,
  Network,
  Plus,
  ChevronDown,
  Upload,
  HelpCircle,
  Loader2,
} from 'lucide-react';
import type {
  OntologyConcept,
  KnowledgeCollection,
  GroupedConcepts,
  TaxonomyStats,
} from '@/types/ontology';
import useBreadcrumbStore from '@/stores/breadcrumb-store';
import { useGlossaryPreferencesStore } from '@/stores/glossary-preferences-store';
import { usePermissions } from '@/stores/permissions-store';
import { FeatureAccessLevel } from '@/types/feature-access-levels';
import { useToast } from '@/hooks/use-toast';

// Tab components (from components/knowledge/)
import {
  CollectionsTab,
  ConceptsTab,
  GraphTab,
  CollectionEditorDialog,
  ConceptEditorDialog,
} from '@/components/knowledge';

type TabValue = 'collections' | 'concepts' | 'graph';

export default function BusinessGlossaryView() {
  const { t } = useTranslation(['semantic-models', 'common']);
  const [searchParams, setSearchParams] = useSearchParams();
  const { toast } = useToast();
  const { hasPermission } = usePermissions();
  
  // Permissions
  const canWrite = hasPermission('semantic-models', FeatureAccessLevel.READ_WRITE);
  
  // Tab state from URL
  const activeTab = (searchParams.get('tab') as TabValue) || 'concepts';
  
  // Data state
  const [isLoading, setIsLoading] = useState(true);
  const [collections, setCollections] = useState<KnowledgeCollection[]>([]);
  const [groupedConcepts, setGroupedConcepts] = useState<GroupedConcepts>({});
  const [groupedProperties, setGroupedProperties] = useState<Record<string, OntologyConcept[]>>({});
  const [selectedConcept, setSelectedConcept] = useState<OntologyConcept | null>(null);
  const [selectedCollection, setSelectedCollection] = useState<KnowledgeCollection | null>(null);
  const [stats, setStats] = useState<TaxonomyStats | null>(null);
  const [hiddenRoots, setHiddenRoots] = useState<Set<string>>(new Set());
  
  // Dialog state
  const [collectionEditorOpen, setCollectionEditorOpen] = useState(false);
  const [editingCollection, setEditingCollection] = useState<KnowledgeCollection | null>(null);
  const [conceptEditorOpen, setConceptEditorOpen] = useState(false);
  const [editingConcept, setEditingConcept] = useState<OntologyConcept | null>(null);
  
  // Glossary preferences from persistent store
  const glossaryPrefs = useGlossaryPreferencesStore();
  const { 
    hiddenSources, 
    groupBySource, 
    showProperties,
    groupByDomain,
    isFilterExpanded,
    toggleSource, 
    selectAllSources, 
    selectNoneSources, 
    setGroupBySource,
    setShowProperties,
    setGroupByDomain,
    setFilterExpanded
  } = glossaryPrefs;
  
  // Extract unique source contexts from concepts and properties
  const availableSources = useMemo(() => {
    const allConcepts = Object.values(groupedConcepts).flat();
    const allProperties = Object.values(groupedProperties).flat();
    const sources = new Set<string>();
    allConcepts.forEach((concept) => {
      if (concept.source_context) {
        sources.add(concept.source_context);
      }
    });
    allProperties.forEach((prop) => {
      if (prop.source_context) {
        sources.add(prop.source_context);
      }
    });
    return Array.from(sources).sort();
  }, [groupedConcepts, groupedProperties]);

  // Filter concepts (and optionally properties) based on hidden sources
  const filteredConcepts = useMemo(() => {
    const allConcepts = Object.values(groupedConcepts).flat();
    const allProperties = showProperties ? Object.values(groupedProperties).flat() : [];
    
    // Deduplicate by IRI: properties may exist in both groupedConcepts and groupedProperties
    const seenIris = new Set<string>();
    const combined: OntologyConcept[] = [];
    for (const item of [...allConcepts, ...allProperties]) {
      if (!seenIris.has(item.iri)) {
        seenIris.add(item.iri);
        combined.push(item);
      }
    }
    
    if (hiddenSources.length === 0) {
      return combined;
    }
    return combined.filter(
      (item) => !item.source_context || !hiddenSources.includes(item.source_context)
    );
  }, [groupedConcepts, groupedProperties, hiddenSources, showProperties]);
  
  // Breadcrumbs
  const setStaticSegments = useBreadcrumbStore((state) => state.setStaticSegments);
  
  useEffect(() => {
    setStaticSegments([
      { label: t('semantic-models:title'), path: '/semantic-models' },
    ]);
  }, [setStaticSegments, t]);
  
  // Fetch data
  const fetchData = useCallback(async () => {
    setIsLoading(true);
    try {
      const [collectionsRes, conceptsRes, statsRes] = await Promise.all([
        fetch('/api/knowledge/collections?hierarchical=true'),
        fetch('/api/semantic-models/concepts-grouped'),
        fetch('/api/semantic-models/stats'),
      ]);
      
      if (collectionsRes.ok) {
        const data = await collectionsRes.json();
        setCollections(data.collections || []);
      }
      
      if (conceptsRes.ok) {
        const data = await conceptsRes.json();
        setGroupedConcepts(data.grouped_concepts || {});
      }
      
      if (statsRes.ok) {
        const data = await statsRes.json();
        setStats(data.stats);
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
    } finally {
      setIsLoading(false);
    }
  }, []);
  
  // Fetch properties when showProperties toggle is enabled
  const fetchProperties = useCallback(async () => {
    try {
      const response = await fetch('/api/semantic-models/properties-grouped');
      if (!response.ok) throw new Error('Failed to fetch properties');
      const data = await response.json();
      
      // Convert to OntologyConcept-compatible format
      const propsGrouped: Record<string, OntologyConcept[]> = {};
      for (const [source, props] of Object.entries(data.grouped_properties || {})) {
        propsGrouped[source] = (props as any[]).map((p: any) => ({
          ...p,
          properties: [],
          synonyms: [],
          examples: [],
        } as OntologyConcept));
      }
      setGroupedProperties(propsGrouped);
    } catch (err) {
      console.error('Failed to fetch properties:', err);
    }
  }, []);

  // Effect to fetch/clear properties when toggle changes
  useEffect(() => {
    if (showProperties) {
      fetchProperties();
    } else {
      setGroupedProperties({});
    }
  }, [showProperties, fetchProperties]);
  
  useEffect(() => {
    fetchData();
  }, [fetchData]);
  
  // Handle concept from URL
  useEffect(() => {
    const conceptIri = searchParams.get('concept');
    if (conceptIri && filteredConcepts.length > 0) {
      const decoded = decodeURIComponent(conceptIri);
      const found = filteredConcepts.find(c => c.iri === decoded);
      if (found) {
        setSelectedConcept(found);
      }
    }
  }, [searchParams, filteredConcepts]);
  
  // Tab change handler
  const handleTabChange = (value: string) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.set('tab', value);
    setSearchParams(newParams, { replace: true });
  };
  
  // Handler for toggling root visibility in the knowledge graph
  const handleToggleRoot = useCallback((rootIri: string) => {
    setHiddenRoots(prev => {
      const newSet = new Set(prev);
      if (newSet.has(rootIri)) {
        newSet.delete(rootIri);
      } else {
        newSet.add(rootIri);
      }
      return newSet;
    });
  }, []);
  
  // Collection handlers
  const handleCreateCollection = () => {
    setEditingCollection(null);
    setCollectionEditorOpen(true);
  };
  
  const handleEditCollection = (collection: KnowledgeCollection) => {
    setEditingCollection(collection);
    setCollectionEditorOpen(true);
  };
  
  const handleSaveCollection = async (data: any, isNew: boolean) => {
    try {
      const url = isNew
        ? '/api/knowledge/collections'
        : `/api/knowledge/collections/${encodeURIComponent(editingCollection!.iri)}`;
      const method = isNew ? 'POST' : 'PATCH';
      
      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to save collection');
      }
      
      toast({
        title: t('common:toast.success'),
        description: isNew
          ? t('semantic-models:messages.collectionCreated')
          : t('semantic-models:messages.collectionUpdated'),
      });
      
      setCollectionEditorOpen(false);
      await fetchData();
    } catch (error: any) {
      toast({
        title: t('common:toast.error'),
        description: error.message,
        variant: 'destructive',
      });
      throw error;
    }
  };
  
  const handleDeleteCollection = async (collection: KnowledgeCollection) => {
    if (!confirm(t('semantic-models:messages.confirmDeleteCollection', { name: collection.label }))) {
      return;
    }
    
    try {
      const response = await fetch(
        `/api/knowledge/collections/${encodeURIComponent(collection.iri)}`,
        { method: 'DELETE' }
      );
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to delete collection');
      }
      
      toast({
        title: t('common:toast.success'),
        description: t('semantic-models:messages.collectionDeleted'),
      });
      
      await fetchData();
    } catch (error: any) {
      toast({
        title: t('common:toast.error'),
        description: error.message,
        variant: 'destructive',
      });
    }
  };
  
  // Concept handlers
  const handleCreateConcept = () => {
    setEditingConcept(null);
    setConceptEditorOpen(true);
  };
  
  const handleEditConcept = (concept: OntologyConcept) => {
    setEditingConcept(concept);
    setConceptEditorOpen(true);
  };
  
  const handleSelectConcept = (concept: OntologyConcept) => {
    setSelectedConcept(concept);
    const newParams = new URLSearchParams(searchParams);
    newParams.set('concept', encodeURIComponent(concept.iri));
    newParams.set('tab', 'concepts');
    setSearchParams(newParams, { replace: true });
  };
  
  const handleSaveConcept = async (data: any, isNew: boolean) => {
    try {
      const url = isNew
        ? '/api/knowledge/concepts'
        : `/api/knowledge/concepts/${encodeURIComponent(editingConcept!.iri)}`;
      const method = isNew ? 'POST' : 'PATCH';
      
      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to save concept');
      }
      
      toast({
        title: t('common:toast.success'),
        description: isNew
          ? t('semantic-models:messages.conceptCreated')
          : t('semantic-models:messages.conceptUpdated'),
      });
      
      setConceptEditorOpen(false);
      await fetchData();
    } catch (error: any) {
      toast({
        title: t('common:toast.error'),
        description: error.message,
        variant: 'destructive',
      });
      throw error;
    }
  };
  
  const handleDeleteConcept = async (concept: OntologyConcept) => {
    try {
      const response = await fetch(
        `/api/knowledge/concepts/${encodeURIComponent(concept.iri)}`,
        { method: 'DELETE' }
      );
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to delete concept');
      }
      
      toast({
        title: t('common:toast.success'),
        description: t('semantic-models:messages.conceptDeleted'),
      });
      
      if (selectedConcept?.iri === concept.iri) {
        setSelectedConcept(null);
      }
      
      await fetchData();
    } catch (error: any) {
      toast({
        title: t('common:toast.error'),
        description: error.message,
        variant: 'destructive',
      });
    }
  };
  
  // Export handler
  const handleExportCollection = async (collection: KnowledgeCollection, format: 'turtle' | 'rdfxml') => {
    try {
      const response = await fetch(
        `/api/knowledge/collections/${encodeURIComponent(collection.iri)}/export?format=${format}`
      );
      
      if (!response.ok) {
        throw new Error('Export failed');
      }
      
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${collection.label || 'collection'}.${format === 'turtle' ? 'ttl' : 'rdf'}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (error: any) {
      toast({
        title: t('common:toast.error'),
        description: error.message,
        variant: 'destructive',
      });
    }
  };
  
  // Get editable collections for concept creation
  const editableCollections = collections.filter(c => c.is_editable);
  
  // Stats for display
  const totalConcepts = stats?.total_concepts ?? Object.values(groupedConcepts).flat().length;
  const totalProperties = stats?.total_properties ?? Object.values(groupedProperties).flat().length;
  
  return (
    <div className="flex flex-col py-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Network className="h-8 w-8 text-primary" />
          <div>
            <h1 className="text-2xl font-bold">{t('semantic-models:title')}</h1>
            <p className="text-sm text-muted-foreground">
              {collections.length} {t('semantic-models:collections.title').toLowerCase()} / {totalConcepts} {t('common:terms.concepts')}
              {showProperties && ` / ${totalProperties} ${t('common:terms.properties')}`}
            </p>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          {canWrite && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button>
                  <Plus className="h-4 w-4 mr-2" />
                  {t('common:actions.create')}
                  <ChevronDown className="h-4 w-4 ml-2" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={handleCreateConcept}>
                  <Layers className="h-4 w-4 mr-2" />
                  {t('semantic-models:actions.createConcept')}
                </DropdownMenuItem>
                <DropdownMenuItem onClick={handleCreateCollection}>
                  <FolderTree className="h-4 w-4 mr-2" />
                  {t('semantic-models:actions.createCollection')}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
          
          <Button variant="outline" size="icon" title={t('common:actions.import')}>
            <Upload className="h-4 w-4" />
          </Button>
          
          <Button variant="ghost" size="icon" title={t('common:actions.help')}>
            <HelpCircle className="h-4 w-4" />
          </Button>
        </div>
      </div>
      
      {/* Loading state */}
      {isLoading ? (
        <div className="flex-1 flex items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        /* Tabs */
        <Tabs value={activeTab} onValueChange={handleTabChange} className="flex-1 flex flex-col">
          <TabsList className="w-fit">
            <TabsTrigger value="concepts" className="gap-2">
              <Layers className="h-4 w-4" />
              {t('semantic-models:tabs.concepts')}
              <Badge variant="secondary" className="ml-1 h-5 text-xs">
                {totalConcepts}
              </Badge>
            </TabsTrigger>
            <TabsTrigger value="collections" className="gap-2">
              <FolderTree className="h-4 w-4" />
              {t('semantic-models:tabs.collections')}
              <Badge variant="secondary" className="ml-1 h-5 text-xs">
                {collections.length}
              </Badge>
            </TabsTrigger>
            <TabsTrigger value="graph" className="gap-2">
              <Network className="h-4 w-4" />
              {t('semantic-models:tabs.graph')}
            </TabsTrigger>
          </TabsList>
          
          <TabsContent value="concepts" className="flex-1 mt-4">
            <ConceptsTab
              collections={collections}
              groupedConcepts={groupedConcepts}
              filteredConcepts={filteredConcepts}
              selectedConcept={selectedConcept}
              onSelectConcept={handleSelectConcept}
              onCreateConcept={handleCreateConcept}
              onEditConcept={handleEditConcept}
              onDeleteConcept={handleDeleteConcept}
              onRefresh={fetchData}
              canEdit={canWrite}
              // Filter props
              availableSources={availableSources}
              hiddenSources={hiddenSources}
              groupBySource={groupBySource}
              showProperties={showProperties}
              groupByDomain={groupByDomain}
              isFilterExpanded={isFilterExpanded}
              onToggleSource={toggleSource}
              onSelectAllSources={selectAllSources}
              onSelectNoneSources={selectNoneSources}
              onSetGroupBySource={setGroupBySource}
              onSetShowProperties={setShowProperties}
              onSetGroupByDomain={setGroupByDomain}
              onSetFilterExpanded={setFilterExpanded}
            />
          </TabsContent>
          
          <TabsContent value="collections" className="flex-1 mt-4">
            <CollectionsTab
              collections={collections}
              selectedCollection={selectedCollection}
              onSelectCollection={setSelectedCollection}
              onCreateCollection={handleCreateCollection}
              onEditCollection={handleEditCollection}
              onDeleteCollection={handleDeleteCollection}
              onExportCollection={handleExportCollection}
              canEdit={canWrite}
            />
          </TabsContent>
          
          <TabsContent value="graph" className="flex-1 mt-4">
            <GraphTab
              concepts={filteredConcepts}
              hiddenRoots={hiddenRoots}
              onToggleRoot={handleToggleRoot}
              onNodeClick={handleSelectConcept}
              showRootBadges={!groupBySource}
              availableSources={availableSources}
              hiddenSources={hiddenSources}
              onToggleSource={toggleSource}
              onSelectAllSources={selectAllSources}
              onSelectNoneSources={selectNoneSources}
            />
          </TabsContent>
        </Tabs>
      )}
      
      {/* Collection Editor Dialog */}
      <CollectionEditorDialog
        open={collectionEditorOpen}
        onOpenChange={setCollectionEditorOpen}
        collection={editingCollection}
        collections={collections}
        onSave={handleSaveCollection}
      />
      
      {/* Concept Editor Dialog */}
      <ConceptEditorDialog
        open={conceptEditorOpen}
        onOpenChange={setConceptEditorOpen}
        concept={editingConcept}
        collection={selectedCollection || editableCollections[0]}
        collections={editableCollections}
        onSave={handleSaveConcept}
      />
    </div>
  );
}
