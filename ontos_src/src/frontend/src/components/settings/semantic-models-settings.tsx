import { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useApi } from '@/hooks/use-api';
import { useToast } from '@/hooks/use-toast';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { DataTable } from '@/components/ui/data-table';
import { ColumnDef, Column } from '@tanstack/react-table';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Switch } from '@/components/ui/switch';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Upload, ChevronDown, RefreshCw, Trash2, Loader2, Library, Eye, Copy, Check, Network } from 'lucide-react';
import type { SemanticModel } from '@/types/ontology';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog';
import OntologyLibraryDialog from '@/components/settings/ontology-library-dialog';

// System contexts that should be hidden from the UI (internal use only)
const SYSTEM_CONTEXTS = ['urn:meta:sources', 'urn:semantic-links'];

export default function SemanticModelsSettings() {
  const { t } = useTranslation('semantic-models');
  const { get, post, delete: deleteApi } = useApi();
  const { toast } = useToast();
  const [items, setItems] = useState<SemanticModel[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [uploadingId, setUploadingId] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [modelToDelete, setModelToDelete] = useState<SemanticModel | null>(null);
  const [libraryDialogOpen, setLibraryDialogOpen] = useState(false);
  const [viewDialogOpen, setViewDialogOpen] = useState(false);
  const [viewingModel, setViewingModel] = useState<SemanticModel | null>(null);
  const [viewContent, setViewContent] = useState<string>('');
  const [viewLoading, setViewLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isRefreshingGraph, setIsRefreshingGraph] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  
  // Filter out system contexts from display
  const filteredItems = useMemo(() => 
    items.filter(m => !SYSTEM_CONTEXTS.includes(m.name)),
    [items]
  );

  const fetchItems = async () => {
    setIsLoading(true);
    try {
      const res = await get<{ semantic_models: SemanticModel[] } | SemanticModel[]>('/api/semantic-models');
      const data = (res.data as any);
      const models: SemanticModel[] = Array.isArray(data) ? data : (data?.semantic_models || []);
      console.log('Semantic models loaded:', models.map(m => ({ name: m.name, created_by: m.created_by })));
      setItems(models || []);
    } catch (e: any) {
      toast({ title: 'Error', description: e.message || 'Failed to load models', variant: 'destructive' });
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => { fetchItems(); }, []);

  const onRefreshGraph = async () => {
    setIsRefreshingGraph(true);
    try {
      const res = await post<{ message: string }>('/api/semantic-models/refresh-graph', {});
      if (res.error) {
        toast({ title: 'Error', description: res.error, variant: 'destructive' });
      } else {
        toast({ title: 'Success', description: res.data?.message ?? 'Knowledge graph refreshed successfully' });
        await fetchItems();
      }
    } catch (e: any) {
      toast({ title: 'Error', description: e.message ?? 'Failed to refresh knowledge graph', variant: 'destructive' });
    } finally {
      setIsRefreshingGraph(false);
    }
  };

  const onUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    setUploadingId('uploading');
    try {
      const res = await post<{ model: SemanticModel; message: string }>('/api/semantic-models/upload', formData);
      
      if (res.error) {
        toast({ 
          title: 'Upload Failed', 
          description: res.error, 
          variant: 'destructive' 
        });
      } else {
        toast({ 
          title: 'Success', 
          description: res.data.message || 'Semantic model uploaded successfully' 
        });
        await fetchItems();
      }
    } catch (e: any) {
      toast({ 
        title: 'Upload Error', 
        description: e.message || 'Failed to upload file', 
        variant: 'destructive' 
      });
    } finally {
      setUploadingId(null);
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const onToggleEnabled = async (modelId: string, currentEnabled: boolean) => {
    setUploadingId(modelId);
    try {
      const response = await fetch(`/api/semantic-models/${modelId}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ enabled: !currentEnabled }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || 'Failed to update model');
      }

      toast({ 
        title: 'Success', 
        description: `Model ${!currentEnabled ? 'enabled' : 'disabled'} successfully` 
      });
      
      await fetchItems();
    } catch (e: any) {
      toast({ 
        title: 'Error', 
        description: e.message || 'Failed to update model', 
        variant: 'destructive' 
      });
    } finally {
      setUploadingId(null);
    }
  };

  const onDeleteClick = (model: SemanticModel) => {
    setModelToDelete(model);
    setDeleteDialogOpen(true);
  };

  const onViewClick = async (model: SemanticModel) => {
    setViewingModel(model);
    setViewDialogOpen(true);
    setViewLoading(true);
    setViewContent('');
    setCopied(false);
    
    try {
      const res = await get<{ content: string; format: string; name: string }>(
        `/api/semantic-models/${encodeURIComponent(model.id)}/content`
      );
      if (res.error) {
        toast({ title: 'Error', description: res.error, variant: 'destructive' });
        setViewContent('Failed to load content.');
      } else {
        setViewContent(res.data.content || '');
      }
    } catch (e: any) {
      toast({ title: 'Error', description: e.message || 'Failed to load content', variant: 'destructive' });
      setViewContent('Failed to load content.');
    } finally {
      setViewLoading(false);
    }
  };

  const onCopyContent = async () => {
    try {
      await navigator.clipboard.writeText(viewContent);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      toast({ title: 'Error', description: 'Failed to copy to clipboard', variant: 'destructive' });
    }
  };

  const onDeleteConfirm = async () => {
    if (!modelToDelete) return;

    try {
      const res = await deleteApi(`/api/semantic-models/${modelToDelete.id}`);
      
      if (res.error) {
        toast({ 
          title: 'Delete Failed', 
          description: res.error, 
          variant: 'destructive' 
        });
      } else {
        toast({ 
          title: 'Success', 
          description: 'Semantic model deleted successfully' 
        });
        await fetchItems();
      }
    } catch (e: any) {
      toast({ 
        title: 'Delete Error', 
        description: e.message || 'Failed to delete model', 
        variant: 'destructive' 
      });
    } finally {
      setDeleteDialogOpen(false);
      setModelToDelete(null);
    }
  };

  const columns = useMemo<ColumnDef<SemanticModel>[]>(() => [
    {
      accessorKey: 'name',
      header: ({ column }: { column: Column<SemanticModel, unknown> }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}>
          Name <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => <div className="font-medium">{row.getValue('name')}</div>,
    },
    { 
      id: 'source',
      header: 'Source', 
      cell: ({ row }) => {
        const model = row.original;
        const createdBy = model.created_by || '';
        
        // Determine source based on created_by
        let label = 'Unknown';
        let variant: 'default' | 'secondary' | 'outline' = 'secondary';
        
        if (createdBy === 'system@startup') {
          label = 'System';
          variant = 'outline';
        } else if (createdBy === 'system@file') {
          label = 'File';
          variant = 'secondary';
        } else if (createdBy === 'system@schema') {
          label = 'Schema';
          variant = 'secondary';
        } else if (createdBy.startsWith('system@')) {
          label = 'System';
          variant = 'outline';
        } else if (createdBy && createdBy !== '') {
          label = 'Upload';
          variant = 'default';
        }
        
        return <Badge variant={variant}>{label}</Badge>;
      }
    },
    { 
      accessorKey: 'format', 
      header: 'Format', 
      cell: ({ row }) => <Badge variant="secondary">{(row.getValue('format') as string)?.toUpperCase()}</Badge> 
    },
    { 
      accessorKey: 'size_bytes', 
      header: 'Size', 
      cell: ({ row }) => {
        const bytes = row.getValue('size_bytes') as number | undefined;
        if (!bytes) return <span>-</span>;
        const kb = (bytes / 1024).toFixed(1);
        return <span>{kb} KB</span>;
      }
    },
    {
      accessorKey: 'enabled',
      header: 'Enabled',
      cell: ({ row }) => {
        const model = row.original;
        const isToggling = uploadingId === model.id;
        const isFileBased = model.id?.startsWith('file-');
        const createdBy = model.created_by || '';
        const isSystemManaged = createdBy.startsWith('system@') && createdBy !== 'system@startup';
        
        // File-based and schema models can't be toggled (always enabled)
        if (isFileBased || isSystemManaged) {
          return (
            <div data-action-cell="true">
              <Badge variant="outline" className="text-xs">Always On</Badge>
            </div>
          );
        }
        
        return (
          <div data-action-cell="true">
            <Switch
              checked={row.getValue('enabled')}
              onCheckedChange={() => onToggleEnabled(model.id, row.getValue('enabled'))}
              disabled={isToggling}
            />
          </div>
        );
      },
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => {
        const model = row.original;
        const isFileBased = model.id?.startsWith('file-');
        const createdBy = model.created_by || '';
        const isSystemManaged = createdBy.startsWith('system@') && createdBy !== 'system@startup';
        const canDelete = !isFileBased && !isSystemManaged;
        
        return (
          <div data-action-cell="true" className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => onViewClick(model)}
              title="View content"
            >
              <Eye className="h-4 w-4" />
            </Button>
            {canDelete ? (
              <Button
                variant="ghost"
                size="icon"
                onClick={() => onDeleteClick(model)}
                title="Delete model"
              >
                <Trash2 className="h-4 w-4 text-destructive" />
              </Button>
            ) : (
              <span className="text-xs text-muted-foreground px-2">Read-only</span>
            )}
          </div>
        );
      },
    },
  ], [uploadingId]);

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle>Semantic Models (RDFS/SKOS)</CardTitle>
          <CardDescription>Upload and manage taxonomy files for tagging.</CardDescription>
        </div>
        <div className="flex items-center gap-2">
          <Input ref={fileInputRef} type="file" accept=".ttl,.rdf,.xml,.skos,.rdfs,.owl,.nt,.n3,.trig,.trix,.jsonld,.json" className="hidden" onChange={onUpload} />
          <Button 
            variant="default"
            onClick={() => setLibraryDialogOpen(true)}
          >
            <Library className="h-4 w-4 mr-2" />
            Industry Library
          </Button>
          <Button 
            variant="outline" 
            onClick={() => fileInputRef.current?.click()}
            disabled={uploadingId === 'uploading'}
          >
            {uploadingId === 'uploading' ? (
              <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Uploading...</>
            ) : (
              <><Upload className="h-4 w-4 mr-2" /> Upload</>
            )}
          </Button>
          <Button
            variant="outline"
            onClick={onRefreshGraph}
            disabled={isRefreshingGraph || uploadingId === 'uploading'}
            title={t('rebuildGraphTooltip')}
          >
            {isRefreshingGraph ? (
              <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> {t('rebuildingGraph')}</>
            ) : (
              <><Network className="h-4 w-4 mr-2" /> {t('rebuildGraph')}</>
            )}
          </Button>
          <Button variant="ghost" onClick={fetchItems} title="Refresh list">
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <DataTable columns={columns} data={filteredItems} searchColumn="name" isLoading={isLoading} />
      </CardContent>

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Semantic Model</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete <strong>{modelToDelete?.name}</strong>? 
              This action cannot be undone and will remove the model from the semantic graph.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={onDeleteConfirm} className="bg-destructive text-destructive-foreground hover:bg-destructive/90">
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Industry Ontology Library Dialog */}
      <OntologyLibraryDialog
        isOpen={libraryDialogOpen}
        onOpenChange={setLibraryDialogOpen}
        onImportSuccess={() => {
          // Refresh the list after successful import
          fetchItems();
        }}
      />

      {/* View Content Dialog */}
      <Dialog open={viewDialogOpen} onOpenChange={(open) => {
        setViewDialogOpen(open);
        if (!open) {
          setViewingModel(null);
          setViewContent('');
        }
      }}>
        <DialogContent className="max-w-4xl w-[90vw] h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Eye className="h-5 w-5" />
              {viewingModel?.name || 'View Ontology'}
            </DialogTitle>
            <DialogDescription>
              Raw content of the semantic model ({viewingModel?.format?.toUpperCase() || 'Unknown format'})
            </DialogDescription>
          </DialogHeader>
          <div className="flex-1 min-h-0">
            {viewLoading ? (
              <div className="flex items-center justify-center h-full text-muted-foreground">
                <Loader2 className="h-6 w-6 animate-spin mr-2" />
                Loading content...
              </div>
            ) : (
              <ScrollArea className="h-full rounded-md border bg-muted/30">
                <pre className="p-4 text-sm font-mono whitespace-pre-wrap break-words">
                  {viewContent || 'No content available.'}
                </pre>
              </ScrollArea>
            )}
          </div>
          <DialogFooter className="flex-shrink-0">
            <Button
              variant="outline"
              onClick={onCopyContent}
              disabled={viewLoading || !viewContent}
            >
              {copied ? (
                <><Check className="h-4 w-4 mr-2" /> Copied!</>
              ) : (
                <><Copy className="h-4 w-4 mr-2" /> Copy to Clipboard</>
              )}
            </Button>
            <Button variant="default" onClick={() => setViewDialogOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}


