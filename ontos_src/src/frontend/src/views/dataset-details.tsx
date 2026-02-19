import { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useToast } from '@/hooks/use-toast';
import useBreadcrumbStore from '@/stores/breadcrumb-store';
import {
  ArrowLeft,
  Pencil,
  Trash2,
  AlertCircle,
  Table2,
  FileText,
  Users,
  Bell,
  BellOff,
  ExternalLink,
  Loader2,
  KeyRound,
} from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import type {
  Dataset,
  DatasetStatus,
  DatasetSubscriptionResponse,
  DatasetSubscribersListResponse,
  DatasetInstance,
  DatasetInstanceListResponse,
  DatasetInstanceEnvironment,
} from '@/types/dataset';
import {
  DATASET_STATUS_LABELS,
  DATASET_STATUS_COLORS,
  DATASET_INSTANCE_STATUS_LABELS,
  DATASET_INSTANCE_STATUS_COLORS,
  DATASET_INSTANCE_ROLE_LABELS,
  DATASET_INSTANCE_ROLE_COLORS,
  DATASET_INSTANCE_ENVIRONMENT_LABELS,
  DATASET_INSTANCE_ENVIRONMENT_COLORS,
} from '@/types/dataset';
import type { DatasetInstanceRole } from '@/types/dataset';
import type { DatasetInstanceStatus } from '@/types/dataset';
import { RelativeDate } from '@/components/common/relative-date';
import DatasetFormDialog from '@/components/datasets/dataset-form-dialog';
import DatasetInstanceFormDialog from '@/components/datasets/dataset-instance-form-dialog';
import CreateContractFromDatasetDialog from '@/components/datasets/create-contract-from-dataset-dialog';
import DatasetLookupDialog from '@/components/data-contracts/dataset-lookup-dialog';
import EntityMetadataPanel from '@/components/metadata/entity-metadata-panel';
import { RatingPanel } from '@/components/ratings';
import AccessGrantsPanel from '@/components/access/access-grants-panel';
import TagChip from '@/components/ui/tag-chip';
import { CommentSidebar } from '@/components/comments';
import ConceptSelectDialog from '@/components/semantic/concept-select-dialog';
import LinkedConceptChips from '@/components/semantic/linked-concept-chips';
import type { EntitySemanticLink } from '@/types/semantic-link';
import { Label } from '@/components/ui/label';
import { Plus, Server, Database } from 'lucide-react';
import RequestDatasetActionDialog from '@/components/datasets/request-dataset-action-dialog';
import { usePermissions } from '@/stores/permissions-store';
import { FeatureAccessLevel } from '@/types/settings';

export default function DatasetDetails() {
  const { t } = useTranslation(['datasets', 'common']);
  const { datasetId } = useParams<{ datasetId: string }>();
  const navigate = useNavigate();
  const { toast } = useToast();
  const setStaticSegments = useBreadcrumbStore((state) => state.setStaticSegments);
  const setDynamicTitle = useBreadcrumbStore((state) => state.setDynamicTitle);

  // Data state
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Subscription state
  const [subscriptionStatus, setSubscriptionStatus] = useState<DatasetSubscriptionResponse | null>(null);
  const [subscribers, setSubscribers] = useState<DatasetSubscribersListResponse | null>(null);
  const [subscribing, setSubscribing] = useState(false);

  // Request dialog state
  const [isRequestDialogOpen, setIsRequestDialogOpen] = useState(false);
  
  // Permissions
  const { getPermissionLevel } = usePermissions();
  const canDirectStatusChange = (() => {
    const permLevel = getPermissionLevel('datasets');
    return permLevel === FeatureAccessLevel.READ_WRITE ||
           permLevel === FeatureAccessLevel.ADMIN ||
           permLevel === FeatureAccessLevel.FULL;
  })();

  // Dialog state
  const [openEditDialog, setOpenEditDialog] = useState(false);
  const [isCommentSidebarOpen, setIsCommentSidebarOpen] = useState(false);
  const [conceptDialogOpen, setConceptDialogOpen] = useState(false);
  const [openInstanceDialog, setOpenInstanceDialog] = useState(false);
  const [editingInstance, setEditingInstance] = useState<DatasetInstance | null>(null);
  const [isCreateContractDialogOpen, setIsCreateContractDialogOpen] = useState(false);
  const [isUCLookupOpen, setIsUCLookupOpen] = useState(false);
  const [isCreatingFromUC, setIsCreatingFromUC] = useState(false);

  // Semantic links state
  const [semanticLinks, setSemanticLinks] = useState<EntitySemanticLink[]>([]);

  // Instances state
  const [instances, setInstances] = useState<DatasetInstance[]>([]);
  const [instancesLoading, setInstancesLoading] = useState(false);

  // Fetch dataset
  const fetchDataset = useCallback(async () => {
    if (!datasetId) return;

    try {
      setLoading(true);
      const response = await fetch(`/api/datasets/${datasetId}`);
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Dataset not found');
        }
        throw new Error('Failed to fetch dataset');
      }
      const data = await response.json();
      setDataset(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch dataset');
    } finally {
      setLoading(false);
    }
  }, [datasetId]);

  // Fetch subscription status
  const fetchSubscriptionStatus = useCallback(async () => {
    if (!datasetId) return;

    try {
      const response = await fetch(`/api/datasets/${datasetId}/subscription`);
      if (response.ok) {
        const data = await response.json();
        setSubscriptionStatus(data);
      }
    } catch (err) {
      console.warn('Failed to fetch subscription status:', err);
    }
  }, [datasetId]);

  // Fetch subscribers
  const fetchSubscribers = useCallback(async () => {
    if (!datasetId) return;

    try {
      const response = await fetch(`/api/datasets/${datasetId}/subscribers`);
      if (response.ok) {
        const data = await response.json();
        setSubscribers(data);
      }
    } catch (err) {
      console.warn('Failed to fetch subscribers:', err);
    }
  }, [datasetId]);

  // Fetch semantic links
  const fetchSemanticLinks = useCallback(async () => {
    if (!datasetId) return;

    try {
      const response = await fetch(`/api/semantic-links/entity/dataset/${datasetId}`);
      if (response.ok) {
        const data = await response.json();
        setSemanticLinks(Array.isArray(data) ? data : []);
      }
    } catch (err) {
      console.warn('Failed to fetch semantic links:', err);
      setSemanticLinks([]);
    }
  }, [datasetId]);

  // Fetch instances
  const fetchInstances = useCallback(async () => {
    if (!datasetId) return;

    try {
      setInstancesLoading(true);
      const response = await fetch(`/api/datasets/${datasetId}/instances`);
      if (response.ok) {
        const data: DatasetInstanceListResponse = await response.json();
        setInstances(data.instances || []);
      }
    } catch (err) {
      console.warn('Failed to fetch instances:', err);
      setInstances([]);
    } finally {
      setInstancesLoading(false);
    }
  }, [datasetId]);

  useEffect(() => {
    fetchDataset();
    fetchSubscriptionStatus();
    fetchSubscribers();
    fetchSemanticLinks();
    fetchInstances();
  }, [fetchDataset, fetchSubscriptionStatus, fetchSubscribers, fetchSemanticLinks, fetchInstances]);

  useEffect(() => {
    // Set breadcrumbs
    setStaticSegments([{ label: t('title'), path: '/datasets' }]);
    setDynamicTitle(dataset?.name || t('details.loading'));

    return () => {
      setStaticSegments([]);
      setDynamicTitle(null);
    };
  }, [setStaticSegments, setDynamicTitle, dataset?.name, t]);

  // Toggle subscription
  const toggleSubscription = async () => {
    if (!datasetId) return;

    setSubscribing(true);
    try {
      const isSubscribed = subscriptionStatus?.subscribed;
      const method = isSubscribed ? 'DELETE' : 'POST';
      const response = await fetch(`/api/datasets/${datasetId}/subscribe`, {
        method,
      });

      if (!response.ok) throw new Error(t('details.subscription.error'));

      const data = await response.json();
      setSubscriptionStatus(data);
      fetchSubscribers();

      toast({
        title: isSubscribed ? t('details.subscription.unsubscribed') : t('details.subscription.subscribed'),
        description: isSubscribed
          ? t('details.subscription.unsubscribeMessage')
          : t('details.subscription.subscribeMessage'),
      });
    } catch (err) {
      toast({
        title: t('messages.error'),
        description: t('details.subscription.error'),
        variant: 'destructive',
      });
    } finally {
      setSubscribing(false);
    }
  };

  // Delete dataset
  const handleDelete = async () => {
    if (!datasetId || !dataset) return;
    if (!confirm(t('details.deleteConfirm', { name: dataset.name }))) return;

    try {
      const response = await fetch(`/api/datasets/${datasetId}`, {
        method: 'DELETE',
      });
      if (!response.ok) throw new Error(t('details.deleteError'));

      toast({
        title: t('messages.success'),
        description: t('details.deleteSuccess'),
      });
      navigate('/datasets');
    } catch (err) {
      toast({
        title: t('messages.error'),
        description: t('details.deleteError'),
        variant: 'destructive',
      });
    }
  };

  // Add semantic link
  const addSemanticLink = async (iri: string) => {
    if (!datasetId) return;
    try {
      const response = await fetch('/api/semantic-links/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          entity_id: datasetId,
          entity_type: 'dataset',
          iri,
        }),
      });
      if (!response.ok) throw new Error(t('details.conceptLinkError'));
      await fetchSemanticLinks();
      setConceptDialogOpen(false);
      toast({ title: t('details.subscription.subscribed'), description: t('details.conceptLinked') });
    } catch (err) {
      toast({
        title: t('messages.error'),
        description: err instanceof Error ? err.message : t('details.conceptLinkError'),
        variant: 'destructive',
      });
    }
  };

  // Remove semantic link
  const removeSemanticLink = async (linkId: string) => {
    try {
      const response = await fetch(`/api/semantic-links/${linkId}`, { method: 'DELETE' });
      if (!response.ok) throw new Error(t('details.conceptUnlinkError'));
      await fetchSemanticLinks();
      toast({ title: t('details.subscription.unsubscribed'), description: t('details.conceptUnlinked') });
    } catch (err) {
      toast({
        title: t('messages.error'),
        description: err instanceof Error ? err.message : t('details.conceptUnlinkError'),
        variant: 'destructive',
      });
    }
  };

  // Delete instance
  const handleDeleteInstance = async (instanceId: string) => {
    if (!datasetId) return;
    if (!confirm(t('details.instances.removeConfirm'))) return;

    try {
      const response = await fetch(`/api/datasets/${datasetId}/instances/${instanceId}`, {
        method: 'DELETE',
      });
      if (!response.ok) throw new Error(t('details.instances.removeError'));

      toast({
        title: t('messages.success'),
        description: t('details.instances.removeSuccess'),
      });
      fetchInstances();
    } catch (err) {
      toast({
        title: t('messages.error'),
        description: t('details.instances.removeError'),
        variant: 'destructive',
      });
    }
  };

  // Edit instance
  const handleEditInstance = (instance: DatasetInstance) => {
    setEditingInstance(instance);
    setOpenInstanceDialog(true);
  };

  // Add new instance
  const handleAddInstance = () => {
    setEditingInstance(null);
    setOpenInstanceDialog(true);
  };

  // Create instance from Unity Catalog table
  const handleCreateFromUC = async (table: { full_name: string }) => {
    if (!datasetId) return;
    
    setIsCreatingFromUC(true);
    try {
      // Extract display name from full path (last part)
      const displayName = table.full_name.split('.').pop() || table.full_name;
      
      // Create the instance with the UC table path
      const response = await fetch(`/api/datasets/${datasetId}/instances`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          physical_path: table.full_name,
          display_name: displayName,
          role: 'main',
          environment: 'production',
          status: 'active',
          server_type: 'unity_catalog',
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || t('details.instances.addError'));
      }

      toast({
        title: t('messages.success'),
        description: t('details.instances.addSuccess'),
      });
      
      setIsUCLookupOpen(false);
      fetchInstances();
    } catch (err) {
      toast({
        title: t('messages.error'),
        description: err instanceof Error ? err.message : t('details.instances.addError'),
        variant: 'destructive',
      });
    } finally {
      setIsCreatingFromUC(false);
    }
  };

  if (loading) {
    return (
      <div className="py-6 space-y-6">
        {/* Header skeleton */}
        <div className="flex items-center justify-between">
          <Skeleton className="h-9 w-32" />
          <div className="flex items-center gap-2">
            <Skeleton className="h-9 w-9" />
            <Skeleton className="h-9 w-9" />
            <Skeleton className="h-9 w-24" />
            <Skeleton className="h-9 w-24" />
          </div>
        </div>

        {/* Core Metadata Card skeleton */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-3">
              <Skeleton className="h-7 w-7 rounded" />
              <Skeleton className="h-7 w-64" />
            </div>
            <Skeleton className="h-4 w-96 mt-2" />
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid md:grid-cols-3 gap-x-6 gap-y-2">
              {[...Array(6)].map((_, i) => (
                <div key={i} className="flex items-center gap-2">
                  <Skeleton className="h-4 w-16" />
                  <Skeleton className="h-4 w-24" />
                </div>
              ))}
            </div>
            <div className="pt-2 border-t">
              <div className="flex gap-3">
                <div className="flex-1">
                  <Skeleton className="h-3 w-12 mb-1.5" />
                  <div className="flex gap-1">
                    <Skeleton className="h-5 w-16" />
                    <Skeleton className="h-5 w-20" />
                  </div>
                </div>
                <div className="flex-1">
                  <Skeleton className="h-3 w-24 mb-1.5" />
                  <Skeleton className="h-5 w-32" />
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Overview Card skeleton */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <Skeleton className="h-5 w-5" />
              <Skeleton className="h-5 w-32" />
            </div>
            <Skeleton className="h-4 w-48 mt-1" />
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1">
                <Skeleton className="h-4 w-16" />
                <Skeleton className="h-4 w-12" />
              </div>
              <div className="space-y-1">
                <Skeleton className="h-4 w-24" />
                <Skeleton className="h-4 w-16" />
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Contract Card skeleton */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <Skeleton className="h-5 w-5" />
              <Skeleton className="h-5 w-28" />
            </div>
            <Skeleton className="h-4 w-56 mt-1" />
          </CardHeader>
          <CardContent>
            <Skeleton className="h-16 w-full rounded-lg" />
          </CardContent>
        </Card>

        {/* Instances Card skeleton */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-2">
                  <Skeleton className="h-5 w-5" />
                  <Skeleton className="h-5 w-36" />
                </div>
                <Skeleton className="h-4 w-48 mt-1" />
              </div>
              <div className="flex gap-2">
                <Skeleton className="h-8 w-36" />
                <Skeleton className="h-8 w-28" />
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (error || !dataset) {
    return (
      <div className="py-6 space-y-6">
        <Button variant="outline" size="sm" onClick={() => navigate('/datasets')}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          {t('details.backToList')}
        </Button>
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error || t('details.notFound')}</AlertDescription>
        </Alert>
      </div>
    );
  }

  const status = dataset.status as DatasetStatus;

  return (
    <div className="py-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <Button variant="outline" size="sm" onClick={() => navigate('/datasets')}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          {t('details.backToList')}
        </Button>
        <div className="flex items-center gap-2">
          {/* Request Action Button - first after Back to List */}
          <Button variant="outline" size="sm" onClick={() => setIsRequestDialogOpen(true)}>
            <KeyRound className="mr-2 h-4 w-4" />
            {t('request.button', 'Request...')}
          </Button>
          <CommentSidebar
            entityType="dataset"
            entityId={datasetId!}
            isOpen={isCommentSidebarOpen}
            onToggle={() => setIsCommentSidebarOpen(!isCommentSidebarOpen)}
            className="h-8"
          />
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon"
                  onClick={toggleSubscription}
                  disabled={subscribing}
                >
                  {subscriptionStatus?.subscribed ? (
                    <BellOff className="h-4 w-4" />
                  ) : (
                    <Bell className="h-4 w-4" />
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>{subscriptionStatus?.subscribed ? t('details.unsubscribe') : t('details.subscribe')}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <Button variant="outline" onClick={() => setOpenEditDialog(true)}>
            <Pencil className="h-4 w-4 mr-2" />
            {t('details.edit')}
          </Button>
          <Button variant="destructive" onClick={handleDelete}>
            <Trash2 className="h-4 w-4 mr-2" />
            {t('details.delete')}
          </Button>
        </div>
      </div>

      {/* Core Metadata Card */}
      <Card>
        <CardHeader>
          <CardTitle className="text-2xl font-bold flex items-center">
            <Table2 className="mr-3 h-7 w-7 text-primary" />
            {dataset.name}
          </CardTitle>
          <CardDescription className="pt-1">
            {dataset.description || t('details.noDescription')}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid md:grid-cols-3 gap-x-6 gap-y-2">
            <div className="flex items-center gap-2">
              <Label className="text-xs text-muted-foreground min-w-[4rem]">{t('details.coreMetadata.status')}:</Label>
              <div className="flex items-center gap-1.5">
                <Badge
                  variant="outline"
                  className={DATASET_STATUS_COLORS[status] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'}
                >
                  {t(`status.${status}`) || DATASET_STATUS_LABELS[status] || status}
                </Badge>
                {dataset.published && (
                  <Badge variant="default" className="bg-green-600 text-xs">{t('details.published')}</Badge>
                )}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-xs text-muted-foreground min-w-[4rem]">{t('details.coreMetadata.version')}:</Label>
              <Badge variant="outline" className="text-xs">{dataset.version || t('common:states.notAvailable')}</Badge>
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-xs text-muted-foreground min-w-[4rem]">{t('details.coreMetadata.owner')}:</Label>
              {dataset.owner_team_id && dataset.owner_team_name ? (
                <span
                  className="text-xs cursor-pointer text-primary hover:underline truncate"
                  onClick={() => navigate(`/teams/${dataset.owner_team_id}`)}
                  title={`Team ID: ${dataset.owner_team_id}`}
                >
                  {dataset.owner_team_name}
                </span>
              ) : (
                <span className="text-xs text-muted-foreground">{t('common:states.notAssigned')}</span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-xs text-muted-foreground min-w-[4rem]">{t('details.coreMetadata.project')}:</Label>
              {dataset.project_id && dataset.project_name ? (
                <span
                  className="text-xs cursor-pointer text-primary hover:underline truncate"
                  onClick={() => navigate(`/projects/${dataset.project_id}`)}
                  title={`Project ID: ${dataset.project_id}`}
                >
                  {dataset.project_name}
                </span>
              ) : (
                <span className="text-xs text-muted-foreground">{t('common:states.notAssigned')}</span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-xs text-muted-foreground min-w-[4rem]">{t('details.coreMetadata.created')}:</Label>
              <span className="text-xs text-muted-foreground truncate">
                <RelativeDate date={dataset.created_at} />
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-xs text-muted-foreground min-w-[4rem]">{t('details.coreMetadata.updated')}:</Label>
              <span className="text-xs text-muted-foreground truncate">
                <RelativeDate date={dataset.updated_at} />
              </span>
            </div>
          </div>

          <div className="pt-2 border-t">
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="flex-1 min-w-0">
                <Label className="text-xs text-muted-foreground mb-1.5 block">{t('details.coreMetadata.tags')}:</Label>
                <div className="flex flex-wrap gap-1">
                  {dataset.tags && dataset.tags.length > 0 ? (
                    dataset.tags.map((tag, idx) => (
                      <TagChip key={idx} tag={tag} size="sm" />
                    ))
                  ) : (
                    <span className="text-xs text-muted-foreground">{t('details.coreMetadata.noTags')}</span>
                  )}
                </div>
              </div>
              <div className="flex-1 min-w-0">
                <Label className="text-xs text-muted-foreground mb-1.5 block">{t('details.coreMetadata.linkedConcepts')}:</Label>
                <LinkedConceptChips
                  links={semanticLinks}
                  onRemove={(id) => removeSemanticLink(id)}
                  trailing={<Button size="sm" variant="outline" onClick={() => setConceptDialogOpen(true)} className="h-6 text-xs">{t('details.coreMetadata.addConcept')}</Button>}
                />
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Contract Link */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5" />
            {t('details.contract.title')}
          </CardTitle>
          <CardDescription>
            {t('details.contract.subtitle')}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {dataset.contract_id ? (
            <div className="flex items-center justify-between p-3 border rounded-lg">
              <div>
                <p className="font-medium">{dataset.contract_name || t('details.contract.linkedContract')}</p>
                <p className="text-sm text-muted-foreground">
                  {t('details.contract.description')}
                </p>
              </div>
              <Button variant="outline" asChild>
                <Link to={`/data-contracts/${dataset.contract_id}`}>
                  <ExternalLink className="h-4 w-4 mr-2" />
                  {t('details.contract.viewContract')}
                </Link>
              </Button>
            </div>
          ) : (
            <div className="text-center py-8 text-muted-foreground">
              <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
              <p>{t('details.contract.noContract')}</p>
              <p className="text-sm mb-4">
                {t('details.contract.noContractHint')}
              </p>
              <Button 
                variant="outline" 
                onClick={() => setIsCreateContractDialogOpen(true)}
              >
                <Plus className="h-4 w-4 mr-2" />
                Create Contract
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Physical Instances */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <Server className="h-5 w-5" />
                {t('details.instances.title')}
                {instances.length > 0 && (
                  <Badge variant="secondary">{instances.length}</Badge>
                )}
              </CardTitle>
              <CardDescription>
                {t('details.instances.subtitle')}
              </CardDescription>
            </div>
            <div className="flex gap-2">
              <Button 
                variant="outline" 
                size="sm" 
                onClick={() => setIsUCLookupOpen(true)}
                disabled={isCreatingFromUC}
              >
                {isCreatingFromUC ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Database className="h-4 w-4 mr-2" />
                )}
                {t('details.instances.inferFromUC', 'Infer from Unity Catalog')}
              </Button>
              <Button variant="outline" size="sm" onClick={handleAddInstance}>
                <Plus className="h-4 w-4 mr-2" />
                {t('details.instances.addInstance')}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {instancesLoading ? (
            <div className="space-y-2">
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
            </div>
          ) : instances.length > 0 ? (
            <div className="space-y-4">
              {/* Group instances by role */}
              {(['main', 'dimension', 'lookup', 'reference', 'staging'] as DatasetInstanceRole[]).map((role) => {
                const roleInstances = instances.filter((i) => (i.role || 'main') === role);
                if (roleInstances.length === 0) return null;
                
                return (
                  <div key={role} className="space-y-2">
                    <div className="flex items-center gap-2">
                      <Badge
                        variant="outline"
                        className={DATASET_INSTANCE_ROLE_COLORS[role] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'}
                      >
                        {t(`instanceRole.${role}`) || DATASET_INSTANCE_ROLE_LABELS[role] || role}
                      </Badge>
                      <span className="text-sm text-muted-foreground">
                        ({roleInstances.length})
                      </span>
                    </div>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>{t('details.instances.table.name')}</TableHead>
                          <TableHead>{t('details.instances.table.environment')}</TableHead>
                          <TableHead>{t('details.instances.table.physicalPath')}</TableHead>
                          <TableHead>{t('details.instances.table.contract')}</TableHead>
                          <TableHead>{t('details.instances.table.tags')}</TableHead>
                          <TableHead>{t('details.instances.table.status')}</TableHead>
                          <TableHead className="text-right">{t('details.instances.table.actions')}</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {roleInstances.map((instance) => {
                          const instStatus = instance.status as DatasetInstanceStatus;
                          const instEnv = instance.environment as DatasetInstanceEnvironment | undefined;
                          return (
                            <TableRow key={instance.id}>
                              <TableCell>
                                <div className="flex flex-col">
                                  <span className="font-medium">
                                    {instance.display_name || instance.physical_path.split('.').pop()}
                                  </span>
                                  {instance.server_type && (
                                    <span className="text-xs text-muted-foreground capitalize">
                                      {instance.server_type}
                                    </span>
                                  )}
                                </div>
                              </TableCell>
                              <TableCell>
                                {instEnv ? (
                                  <Badge
                                    variant="outline"
                                    className={DATASET_INSTANCE_ENVIRONMENT_COLORS[instEnv] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'}
                                  >
                                    {DATASET_INSTANCE_ENVIRONMENT_LABELS[instEnv] || instEnv}
                                  </Badge>
                                ) : instance.server_environment ? (
                                  <Badge variant="outline" className="capitalize">
                                    {instance.server_environment}
                                  </Badge>
                                ) : (
                                  <span className="text-muted-foreground">-</span>
                                )}
                              </TableCell>
                              <TableCell>
                                <code className="text-sm bg-muted px-2 py-1 rounded">
                                  {instance.physical_path}
                                </code>
                              </TableCell>
                              <TableCell>
                                {instance.contract_name ? (
                                  <TooltipProvider>
                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <Link
                                          to={`/data-contracts/${instance.contract_id}`}
                                          className="text-sm hover:underline text-blue-600 dark:text-blue-400"
                                        >
                                          v{instance.contract_version || '-'}
                                        </Link>
                                      </TooltipTrigger>
                                      <TooltipContent>
                                        <p>{instance.contract_name}</p>
                                      </TooltipContent>
                                    </Tooltip>
                                  </TooltipProvider>
                                ) : (
                                  <span className="text-muted-foreground">-</span>
                                )}
                              </TableCell>
                              <TableCell>
                                {instance.tags && instance.tags.length > 0 ? (
                                  <div className="flex flex-wrap gap-1">
                                    {instance.tags.slice(0, 3).map((tag, idx) => (
                                      <TagChip key={idx} tag={tag} size="sm" />
                                    ))}
                                    {instance.tags.length > 3 && (
                                      <span className="text-xs text-muted-foreground">
                                        +{instance.tags.length - 3}
                                      </span>
                                    )}
                                  </div>
                                ) : (
                                  <span className="text-muted-foreground">-</span>
                                )}
                              </TableCell>
                              <TableCell>
                                <Badge
                                  variant="outline"
                                  className={DATASET_INSTANCE_STATUS_COLORS[instStatus] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'}
                                >
                                  {t(`instanceStatus.${instStatus}`) || DATASET_INSTANCE_STATUS_LABELS[instStatus] || instStatus}
                                </Badge>
                              </TableCell>
                              <TableCell className="text-right">
                                <div className="flex justify-end gap-1">
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    onClick={() => handleEditInstance(instance)}
                                  >
                                    <Pencil className="h-4 w-4" />
                                  </Button>
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    onClick={() => handleDeleteInstance(instance.id)}
                                  >
                                    <Trash2 className="h-4 w-4 text-destructive" />
                                  </Button>
                                </div>
                              </TableCell>
                            </TableRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="text-center py-8 text-muted-foreground">
              <Server className="h-8 w-8 mx-auto mb-2 opacity-50" />
              <p>{t('details.instances.noInstances')}</p>
              <p className="text-sm">
                {t('details.instances.noInstancesHint')}
              </p>
              <Button variant="outline" size="sm" className="mt-4" onClick={handleAddInstance}>
                <Plus className="h-4 w-4 mr-2" />
                {t('details.instances.addFirstInstance')}
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Subscribers */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Bell className="h-5 w-5" />
            {t('details.subscribers.title')}
            {subscribers && (
              <Badge variant="secondary">{subscribers.subscriber_count}</Badge>
            )}
          </CardTitle>
          <CardDescription>
            {t('details.subscribers.subtitle')}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {subscribers && subscribers.subscribers.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t('details.subscribers.table.email')}</TableHead>
                  <TableHead>{t('details.subscribers.table.subscribed')}</TableHead>
                  <TableHead>{t('details.subscribers.table.reason')}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {subscribers.subscribers.map((sub, idx) => (
                  <TableRow key={idx}>
                    <TableCell>{sub.email}</TableCell>
                    <TableCell>
                      <RelativeDate date={sub.subscribed_at} />
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {sub.reason || '-'}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="text-center py-8 text-muted-foreground">
              <Users className="h-8 w-8 mx-auto mb-2 opacity-50" />
              <p>{t('details.subscribers.noSubscribers')}</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Access Grants Panel */}
      {datasetId && (
        <AccessGrantsPanel
          entityType="dataset"
          entityId={datasetId}
          canManage={canDirectStatusChange}
          showPendingRequests={canDirectStatusChange}
        />
      )}

      {/* Metadata Panel - Rich texts, links, documents */}
      {datasetId && (
        <EntityMetadataPanel entityId={datasetId} entityType="dataset" />
      )}

      {/* Ratings Panel */}
      {datasetId && (
        <RatingPanel
          entityType="dataset"
          entityId={datasetId}
          title={t('details.ratings.title', 'Ratings & Reviews')}
          showDistribution
          allowSubmit
        />
      )}

      {/* Edit Dialog */}
      <DatasetFormDialog
        open={openEditDialog}
        onOpenChange={setOpenEditDialog}
        dataset={dataset}
        onSuccess={() => {
          fetchDataset();
          setOpenEditDialog(false);
        }}
      />

      {/* Concept Select Dialog */}
      <ConceptSelectDialog
        isOpen={conceptDialogOpen}
        onOpenChange={setConceptDialogOpen}
        onSelect={addSemanticLink}
      />

      {/* Instance Form Dialog */}
      {datasetId && (
        <DatasetInstanceFormDialog
          open={openInstanceDialog}
          onOpenChange={(open) => {
            setOpenInstanceDialog(open);
            if (!open) setEditingInstance(null);
          }}
          datasetId={datasetId}
          instance={editingInstance}
          onSuccess={() => {
            fetchInstances();
            setOpenInstanceDialog(false);
            setEditingInstance(null);
          }}
        />
      )}

      {/* Unity Catalog Lookup Dialog */}
      <DatasetLookupDialog
        isOpen={isUCLookupOpen}
        onOpenChange={setIsUCLookupOpen}
        onSelect={handleCreateFromUC}
      />

      {/* Create Contract from Dataset Dialog */}
      {dataset && (
        <CreateContractFromDatasetDialog
          isOpen={isCreateContractDialogOpen}
          onOpenChange={setIsCreateContractDialogOpen}
          dataset={{
            id: dataset.id,
            name: dataset.name,
            description: dataset.description,
            status: dataset.status,
            version: dataset.version,
            published: dataset.published,
            contract_id: dataset.contract_id,
            contract_name: dataset.contract_name,
            owner_team_id: dataset.owner_team_id,
            owner_team_name: dataset.owner_team_name,
            project_id: dataset.project_id,
            project_name: dataset.project_name,
            subscriber_count: dataset.subscriber_count,
            instance_count: dataset.instance_count,
          }}
          onSuccess={(contractId) => {
            // Refresh dataset to get updated contract link
            fetchDataset();
            setIsCreateContractDialogOpen(false);
            // Navigate to the new contract
            navigate(`/data-contracts/${contractId}`);
          }}
        />
      )}

      {/* Request Action Dialog */}
      {datasetId && dataset && (
        <RequestDatasetActionDialog
          isOpen={isRequestDialogOpen}
          onOpenChange={setIsRequestDialogOpen}
          datasetId={datasetId}
          datasetName={dataset.name}
          datasetStatus={dataset.status}
          datasetPublished={dataset.published}
          canDirectStatusChange={canDirectStatusChange}
          onSuccess={() => {
            fetchDataset();
            setIsRequestDialogOpen(false);
          }}
        />
      )}
    </div>
  );
}

