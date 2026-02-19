import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ColumnDef } from '@tanstack/react-table';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { DataTable } from '@/components/ui/data-table';
import { ListViewSkeleton } from '@/components/common/list-view-skeleton';
import { RelativeDate } from '@/components/common/relative-date';
import { useToast } from '@/hooks/use-toast';
import useBreadcrumbStore from '@/stores/breadcrumb-store';
import { useProjectContext } from '@/stores/project-store';
import {
  Plus,
  Trash2,
  AlertCircle,
  Database,
  FileText,
  Users,
  Server,
  ChevronDown,
} from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import type {
  DatasetListItem,
  DatasetStatus,
} from '@/types/dataset';
import {
  DATASET_STATUS_LABELS,
  DATASET_STATUS_COLORS,
} from '@/types/dataset';
import DatasetFormDialog from '@/components/datasets/dataset-form-dialog';

export default function Datasets() {
  const { t } = useTranslation(['datasets', 'common']);
  const { toast } = useToast();
  const navigate = useNavigate();
  const { currentProject, hasProjectContext } = useProjectContext();
  const setStaticSegments = useBreadcrumbStore((state) => state.setStaticSegments);
  const setDynamicTitle = useBreadcrumbStore((state) => state.setDynamicTitle);

  // Data state
  const [datasets, setDatasets] = useState<DatasetListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Dialog state
  const [openCreateDialog, setOpenCreateDialog] = useState(false);

  // Filter state (status filter is server-side, search is handled by DataTable client-side)
  const [statusFilter, setStatusFilter] = useState<string>('all');

  // Fetch datasets
  const fetchDatasets = useCallback(async () => {
    try {
      setLoading(true);

      const params = new URLSearchParams();
      if (hasProjectContext && currentProject) {
        params.append('project_id', currentProject.id);
      }
      if (statusFilter && statusFilter !== 'all') {
        params.append('status', statusFilter);
      }

      const queryString = params.toString();
      const endpoint = `/api/datasets${queryString ? `?${queryString}` : ''}`;

      const response = await fetch(endpoint);
      if (!response.ok) throw new Error('Failed to fetch datasets');
      const data = await response.json();
      setDatasets(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch datasets');
    } finally {
      setLoading(false);
    }
  }, [hasProjectContext, currentProject, statusFilter]);

  useEffect(() => {
    fetchDatasets();
  }, [fetchDatasets]);

  useEffect(() => {
    // Set breadcrumbs
    setStaticSegments([]);
    setDynamicTitle(t('title'));

    return () => {
      setStaticSegments([]);
      setDynamicTitle(null);
    };
  }, [setStaticSegments, setDynamicTitle, t]);

  // Delete dataset
  const deleteDataset = async (id: string) => {
    if (!confirm(t('messages.deleteConfirm'))) return;

    try {
      const response = await fetch(`/api/datasets/${id}`, {
        method: 'DELETE',
      });
      if (!response.ok) throw new Error(t('messages.deleteError'));
      await fetchDatasets();
      toast({
        title: t('messages.success'),
        description: t('messages.deleteSuccess'),
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : t('messages.deleteError');
      toast({
        title: t('messages.error'),
        description: message,
        variant: 'destructive',
      });
    }
  };


  // Table columns
  const columns: ColumnDef<DatasetListItem>[] = [
    {
      accessorKey: 'name',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.name')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => (
        <div className="flex flex-col">
          <span
            className="font-medium text-primary hover:underline cursor-pointer"
            onClick={() => navigate(`/datasets/${row.original.id}`)}
          >
            {row.original.name}
          </span>
          {row.original.description && (
            <span className="text-xs text-muted-foreground line-clamp-1">
              {row.original.description}
            </span>
          )}
        </div>
      ),
    },
    {
      accessorKey: 'status',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.status')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        const status = row.original.status as DatasetStatus;
        return (
          <Badge
            variant="outline"
            className={DATASET_STATUS_COLORS[status] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'}
          >
            {t(`status.${status}`) || DATASET_STATUS_LABELS[status] || status}
          </Badge>
        );
      },
    },
    {
      accessorKey: 'instance_count',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.instances')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => (
        <div className="flex items-center gap-1">
          <Server className="h-3 w-3 text-muted-foreground" />
          <span className="text-sm">
            {row.original.instance_count || 0}
          </span>
        </div>
      ),
    },
    {
      accessorKey: 'contract_name',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.contract')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        if (!row.original.contract_id) {
          return <span className="text-muted-foreground text-sm">-</span>;
        }
        return (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <div
                  className="flex items-center gap-1 cursor-pointer hover:text-primary"
                  onClick={(e) => {
                    e.stopPropagation();
                    navigate(`/data-contracts/${row.original.contract_id}`);
                  }}
                >
                  <FileText className="h-3 w-3" />
                  <span className="text-sm truncate max-w-[150px]">
                    {row.original.contract_name || t('table.viewContract')}
                  </span>
                </div>
              </TooltipTrigger>
              <TooltipContent>
                <p>{t('table.viewContract')}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        );
      },
    },
    {
      accessorKey: 'owner_team_name',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.owner')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => {
        if (!row.original.owner_team_id) {
          return <span className="text-muted-foreground text-sm">-</span>;
        }
        return (
          <div className="flex items-center gap-1">
            <Users className="h-3 w-3 text-muted-foreground" />
            <span className="text-sm">{row.original.owner_team_name || t('table.owner')}</span>
          </div>
        );
      },
    },
    {
      accessorKey: 'subscriber_count',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.subscribers')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => (
        <span className="text-sm text-muted-foreground">
          {row.original.subscriber_count || 0}
        </span>
      ),
    },
    {
      accessorKey: 'version',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.version')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => (
        <span className="text-sm text-muted-foreground font-mono">
          {row.original.version || '-'}
        </span>
      ),
    },
    {
      accessorKey: 'updated_at',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}>
          {t('table.updated')}
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => (
        <RelativeDate date={row.original.updated_at} />
      ),
    },
    {
      id: 'actions',
      header: '',
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-8 w-8 text-destructive hover:text-destructive"
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteDataset(row.original.id);
                  }}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>{t('table.deleteDataset')}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      ),
    },
  ];

  return (
    <div className="py-6">
      <h1 className="text-3xl font-bold mb-6 flex items-center gap-2">
        <Database className="w-8 h-8" />
        {t('title')}
      </h1>

      {error && (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
          <AlertDescription className="whitespace-pre-wrap flex-1">{error}</AlertDescription>
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0 ml-2 hover:bg-destructive/20"
            onClick={() => setError(null)}
            title={t('common:tooltips.dismiss')}
          >
            <span className="sr-only">Dismiss</span>
            ×
          </Button>
        </Alert>
      )}

      {loading ? (
        <ListViewSkeleton columns={8} rows={5} toolbarButtons={2} />
      ) : (
        <DataTable
          columns={columns}
          data={datasets}
          searchColumn="name"
          storageKey="datasets-sort"
          toolbarActions={
            <>
              <Select value={statusFilter} onValueChange={setStatusFilter}>
                <SelectTrigger className="w-[130px] h-9">
                  <SelectValue placeholder={t('table.status')} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">{t('filters.allStatus')}</SelectItem>
                  <SelectItem value="draft">{t('filters.draft')}</SelectItem>
                  <SelectItem value="active">{t('filters.active')}</SelectItem>
                  <SelectItem value="deprecated">{t('filters.deprecated')}</SelectItem>
                  <SelectItem value="retired">{t('filters.retired')}</SelectItem>
                </SelectContent>
              </Select>
              <Button onClick={() => setOpenCreateDialog(true)} className="gap-2 h-9">
                <Plus className="h-4 w-4" />
                {t('newDataset')}
              </Button>
            </>
          }
          onRowClick={(row) => navigate(`/datasets/${row.original.id}`)}
        />
      )}

      {/* Create Dialog */}
      <DatasetFormDialog
        open={openCreateDialog}
        onOpenChange={setOpenCreateDialog}
        onSuccess={() => {
          fetchDatasets();
          setOpenCreateDialog(false);
        }}
      />
    </div>
  );
}
