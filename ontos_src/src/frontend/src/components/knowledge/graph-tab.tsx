import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible';
import {
  ChevronRight,
  ChevronDown,
  Filter,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import type { OntologyConcept } from '@/types/ontology';
import { KnowledgeGraph } from '@/components/semantic-models/knowledge-graph';

interface GraphTabProps {
  concepts: OntologyConcept[];
  hiddenRoots: Set<string>;
  onToggleRoot: (rootIri: string) => void;
  onNodeClick: (concept: OntologyConcept) => void;
  showRootBadges?: boolean;
  // Source filtering props
  availableSources: string[];
  hiddenSources: string[];
  onToggleSource: (source: string) => void;
  onSelectAllSources: () => void;
  onSelectNoneSources: (sources: string[]) => void;
}

export const GraphTab: React.FC<GraphTabProps> = ({
  concepts,
  hiddenRoots,
  onToggleRoot,
  onNodeClick,
  showRootBadges = true,
  availableSources,
  hiddenSources,
  onToggleSource,
  onSelectAllSources,
  onSelectNoneSources,
}) => {
  const { t } = useTranslation(['semantic-models', 'common']);
  const [isFilterExpanded, setIsFilterExpanded] = useState(true);

  // Count concepts per source
  const getSourceCount = (source: string) => {
    return concepts.filter((c) => c.source_context === source).length;
  };

  return (
    <div className="h-[800px] flex flex-col">
      {/* Filter Panel */}
      {availableSources.length > 0 && (
        <Collapsible
          open={isFilterExpanded}
          onOpenChange={setIsFilterExpanded}
          className="border rounded-lg mb-4 bg-background"
        >
          <div className="px-4 py-2 flex items-center justify-between">
            <CollapsibleTrigger asChild>
              <button className="flex items-center gap-2 text-sm font-medium hover:text-primary transition-colors">
                {isFilterExpanded ? (
                  <ChevronDown className="h-4 w-4" />
                ) : (
                  <ChevronRight className="h-4 w-4" />
                )}
                <Filter className="h-4 w-4" />
                {t('semantic-models:filters.bySource')}
                {hiddenSources.length > 0 && (
                  <Badge variant="secondary" className="h-5 text-[10px] px-1.5">
                    {availableSources.filter(s => !hiddenSources.includes(s)).length}/{availableSources.length}
                  </Badge>
                )}
              </button>
            </CollapsibleTrigger>
            <div className="flex gap-1">
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-xs px-2"
                onClick={onSelectAllSources}
              >
                {t('semantic-models:filters.all')}
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-xs px-2"
                onClick={() => onSelectNoneSources(availableSources)}
              >
                {t('semantic-models:filters.none')}
              </Button>
            </div>
          </div>
          <CollapsibleContent>
            <div className="px-4 pb-3">
              <div className="flex flex-wrap gap-2">
                {availableSources.map((source) => {
                  const isVisible = !hiddenSources.includes(source);
                  const conceptCount = getSourceCount(source);
                  return (
                    <label
                      key={source}
                      className={cn(
                        "flex items-center gap-1.5 px-2 py-1 rounded-md text-xs cursor-pointer transition-colors",
                        "border hover:bg-accent",
                        isVisible ? "bg-accent/50 border-primary/30" : "opacity-60"
                      )}
                    >
                      <Checkbox
                        checked={isVisible}
                        onCheckedChange={() => onToggleSource(source)}
                        className="h-3.5 w-3.5"
                      />
                      <span>{source}</span>
                      <Badge variant="secondary" className="h-4 text-[10px] px-1">
                        {conceptCount}
                      </Badge>
                    </label>
                  );
                })}
              </div>
            </div>
          </CollapsibleContent>
        </Collapsible>
      )}

      {/* Graph */}
      <div className="flex-1 min-h-0">
        <KnowledgeGraph
          concepts={concepts}
          hiddenRoots={hiddenRoots}
          onToggleRoot={onToggleRoot}
          onNodeClick={onNodeClick}
          showRootBadges={showRootBadges}
        />
      </div>
    </div>
  );
};
