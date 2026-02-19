import { useState, useEffect } from 'react';
import { useForm } from 'react-hook-form';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useToast } from '@/hooks/use-toast';
import { Loader2, Info } from 'lucide-react';
import type {
  DatasetInstance,
  DatasetInstanceCreate,
  DatasetInstanceUpdate,
  DatasetInstanceStatus,
  DatasetInstanceRole,
  DatasetInstanceEnvironment,
} from '@/types/dataset';
import {
  DATASET_INSTANCE_ROLE_LABELS,
  DATASET_INSTANCE_ENVIRONMENT_LABELS,
} from '@/types/dataset';
import { UnifiedAssetType } from '@/types/assets';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

interface ContractOption {
  id: string;
  name: string;
  version: string;
  status: string;
}

interface ServerOption {
  id: string;
  server: string;
  type: string;
  environment: string;
  description?: string;
}

interface DatasetInstanceFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  datasetId: string;
  instance?: DatasetInstance | null; // If provided, we're in edit mode
  onSuccess: () => void;
}

interface FormData {
  contract_id: string;
  contract_server_id: string;
  physical_path: string;
  asset_type: UnifiedAssetType | '';
  role: DatasetInstanceRole;
  display_name: string;
  environment: DatasetInstanceEnvironment | '';
  status: DatasetInstanceStatus;
  notes: string;
}

export default function DatasetInstanceFormDialog({
  open,
  onOpenChange,
  datasetId,
  instance,
  onSuccess,
}: DatasetInstanceFormDialogProps) {
  const { t } = useTranslation('datasets');
  const { toast } = useToast();
  const [submitting, setSubmitting] = useState(false);
  const [contracts, setContracts] = useState<ContractOption[]>([]);
  const [servers, setServers] = useState<ServerOption[]>([]);
  const [loadingServers, setLoadingServers] = useState(false);

  const isEditMode = !!instance;

  const {
    register,
    handleSubmit,
    reset,
    watch,
    setValue,
    formState: { errors },
  } = useForm<FormData>({
    defaultValues: {
      contract_id: '',
      contract_server_id: '',
      physical_path: '',
      asset_type: '',
      role: 'main',
      display_name: '',
      environment: '',
      status: 'active',
      notes: '',
    },
  });

  const selectedContractId = watch('contract_id');

  // Load contracts when dialog opens
  useEffect(() => {
    if (open) {
      fetch('/api/data-contracts')
        .then((res) => res.json())
        .then((data) => {
          // Guard against null/undefined response
          if (Array.isArray(data)) {
            setContracts(
              data.map((c: any) => ({
                id: c.id,
                name: c.name,
                version: c.version,
                status: c.status,
              }))
            );
          } else {
            setContracts([]);
          }
        })
        .catch((err) => {
          console.error('Failed to fetch contracts:', err);
          setContracts([]);
        });
    }
  }, [open]);

  // Load servers when contract changes
  useEffect(() => {
    if (selectedContractId) {
      setLoadingServers(true);
      // Fetch the contract details to get its servers
      fetch(`/api/data-contracts/${selectedContractId}`)
        .then((res) => res.json())
        .then((data) => {
          const contractServers = (data.servers || []).map((s: any) => ({
            id: s.id,
            server: s.server || s.id,
            type: s.type,
            environment: s.environment || 'unknown',
            description: s.description,
          }));
          setServers(contractServers);
          
          // If editing and the server is in the list, keep it; otherwise reset
          if (instance && instance.contract_id === selectedContractId) {
            // Keep the current server selection
          } else {
            setValue('contract_server_id', '');
          }
        })
        .catch(console.error)
        .finally(() => setLoadingServers(false));
    } else {
      setServers([]);
      setValue('contract_server_id', '');
    }
  }, [selectedContractId, setValue, instance]);

  // Reset form when dialog opens/closes or instance changes
  useEffect(() => {
    if (open) {
      if (instance) {
        reset({
          contract_id: instance.contract_id || '',
          contract_server_id: instance.contract_server_id || '',
          physical_path: instance.physical_path,
          asset_type: (instance.asset_type || '') as UnifiedAssetType | '',
          role: (instance.role || 'main') as DatasetInstanceRole,
          display_name: instance.display_name || '',
          environment: (instance.environment || '') as DatasetInstanceEnvironment | '',
          status: instance.status as DatasetInstanceStatus,
          notes: instance.notes || '',
        });
      } else {
        reset({
          contract_id: '',
          contract_server_id: '',
          physical_path: '',
          asset_type: '',
          role: 'main',
          display_name: '',
          environment: '',
          status: 'active',
          notes: '',
        });
      }
    }
  }, [open, instance, reset]);

  const onSubmit = async (formData: FormData) => {
    setSubmitting(true);

    try {
      const payload: DatasetInstanceCreate | DatasetInstanceUpdate = {
        contract_id: formData.contract_id || undefined,
        contract_server_id: formData.contract_server_id || undefined,
        physical_path: formData.physical_path,
        asset_type: formData.asset_type || undefined,
        role: formData.role,
        display_name: formData.display_name || undefined,
        environment: formData.environment || undefined,
        status: formData.status,
        notes: formData.notes || undefined,
      };

      const url = isEditMode
        ? `/api/datasets/${datasetId}/instances/${instance!.id}`
        : `/api/datasets/${datasetId}/instances`;

      const response = await fetch(url, {
        method: isEditMode ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to save instance');
      }

      toast({
        title: 'Success',
        description: isEditMode
          ? 'Instance updated successfully'
          : 'Instance created successfully',
      });

      onSuccess();
    } catch (err) {
      toast({
        title: 'Error',
        description: err instanceof Error ? err.message : 'Failed to save instance',
        variant: 'destructive',
      });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>
            {isEditMode ? 'Edit Instance' : 'Add Physical Instance'}
          </DialogTitle>
          <DialogDescription>
            {isEditMode
              ? 'Update the physical instance details.'
              : 'Add a new physical implementation of this dataset.'}
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          {/* Contract Selection */}
          <div className="space-y-2">
            <Label htmlFor="contract_id">Contract Version</Label>
            <Select
              value={watch('contract_id')}
              onValueChange={(value) => setValue('contract_id', value)}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a contract version" />
              </SelectTrigger>
              <SelectContent>
                {contracts.map((contract) => (
                  <SelectItem key={contract.id} value={contract.id}>
                    {contract.name} (v{contract.version}) - {contract.status}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              The contract version this instance implements
            </p>
          </div>

          {/* Server Selection */}
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Label htmlFor="contract_server_id">Server</Label>
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                  </TooltipTrigger>
                  <TooltipContent className="max-w-xs">
                    <p>
                      Servers are defined in the contract. They specify the system type
                      (Databricks, Snowflake, etc.) and environment (dev, prod, etc.)
                    </p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </div>
            <Select
              value={watch('contract_server_id')}
              onValueChange={(value) => setValue('contract_server_id', value)}
              disabled={!selectedContractId || loadingServers}
            >
              <SelectTrigger>
                <SelectValue
                  placeholder={
                    loadingServers
                      ? 'Loading servers...'
                      : !selectedContractId
                      ? 'Select a contract first'
                      : servers.length === 0
                      ? 'No servers defined in contract'
                      : 'Select a server'
                  }
                />
              </SelectTrigger>
              <SelectContent>
                {servers.map((server) => (
                  <SelectItem key={server.id} value={server.id}>
                    <span className="capitalize">{server.type}</span> -{' '}
                    <span className="capitalize">{server.environment}</span>
                    {server.server && ` (${server.server})`}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              The target system and environment from the contract
            </p>
          </div>

          {/* Physical Path */}
          <div className="space-y-2">
            <Label htmlFor="physical_path">Physical Path *</Label>
            <Input
              id="physical_path"
              placeholder="e.g., catalog.schema.table or db.schema.table"
              {...register('physical_path', {
                required: 'Physical path is required',
              })}
            />
            {errors.physical_path && (
              <p className="text-sm text-destructive">{errors.physical_path.message}</p>
            )}
            <p className="text-xs text-muted-foreground">
              The full path to the object in the target system. Format depends on the system
              type (e.g., catalog.schema.table for Unity Catalog).
            </p>
          </div>

          {/* Asset Type */}
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Label htmlFor="asset_type">Asset Type</Label>
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                  </TooltipTrigger>
                  <TooltipContent className="max-w-xs">
                    <p>
                      The unified asset type identifies the kind of asset across platforms.
                      This enables platform-agnostic governance policies and search.
                    </p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </div>
            <Select
              value={watch('asset_type') || 'unspecified'}
              onValueChange={(value) => setValue('asset_type', value === 'unspecified' ? '' : value as UnifiedAssetType)}
            >
              <SelectTrigger>
                <SelectValue placeholder={t('instanceForm.selectAssetType', 'Select asset type')} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="unspecified">{t('assetTypes.unspecified')}</SelectItem>
                {/* Unity Catalog Assets */}
                <SelectItem value={UnifiedAssetType.UC_TABLE}>{t('assetTypes.uc_table')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_VIEW}>{t('assetTypes.uc_view')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_MATERIALIZED_VIEW}>{t('assetTypes.uc_materialized_view')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_STREAMING_TABLE}>{t('assetTypes.uc_streaming_table')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_FUNCTION}>{t('assetTypes.uc_function')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_MODEL}>{t('assetTypes.uc_model')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_VOLUME}>{t('assetTypes.uc_volume')}</SelectItem>
                <SelectItem value={UnifiedAssetType.UC_METRIC}>{t('assetTypes.uc_metric')}</SelectItem>
                {/* Snowflake Assets */}
                <SelectItem value={UnifiedAssetType.SNOWFLAKE_TABLE}>{t('assetTypes.snowflake_table')}</SelectItem>
                <SelectItem value={UnifiedAssetType.SNOWFLAKE_VIEW}>{t('assetTypes.snowflake_view')}</SelectItem>
                {/* Kafka Assets */}
                <SelectItem value={UnifiedAssetType.KAFKA_TOPIC}>{t('assetTypes.kafka_topic')}</SelectItem>
                {/* PowerBI Assets */}
                <SelectItem value={UnifiedAssetType.POWERBI_DATASET}>{t('assetTypes.powerbi_dataset')}</SelectItem>
                <SelectItem value={UnifiedAssetType.POWERBI_DASHBOARD}>{t('assetTypes.powerbi_dashboard')}</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              {t('instanceForm.assetTypeHint', 'Platform-agnostic asset type for unified governance')}
            </p>
          </div>

          {/* Role and Environment Row */}
          <div className="grid grid-cols-2 gap-4">
            {/* Role */}
            <div className="space-y-2">
              <Label htmlFor="role">Role *</Label>
              <Select
                value={watch('role')}
                onValueChange={(value) => setValue('role', value as DatasetInstanceRole)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {Object.entries(DATASET_INSTANCE_ROLE_LABELS).map(([value, label]) => (
                    <SelectItem key={value} value={value}>
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Purpose of this table in the dataset
              </p>
            </div>

            {/* Environment */}
            <div className="space-y-2">
              <Label htmlFor="environment">Environment</Label>
              <Select
                value={watch('environment')}
                onValueChange={(value) => setValue('environment', value as DatasetInstanceEnvironment)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select environment" />
                </SelectTrigger>
                <SelectContent>
                  {Object.entries(DATASET_INSTANCE_ENVIRONMENT_LABELS).map(([value, label]) => (
                    <SelectItem key={value} value={value}>
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Deployment stage (dev, prod, etc.)
              </p>
            </div>
          </div>

          {/* Display Name */}
          <div className="space-y-2">
            <Label htmlFor="display_name">Display Name</Label>
            <Input
              id="display_name"
              placeholder="e.g., Customer Addresses, Countries Lookup"
              {...register('display_name')}
            />
            <p className="text-xs text-muted-foreground">
              Human-readable name for this table within the dataset
            </p>
          </div>

          {/* Status */}
          <div className="space-y-2">
            <Label htmlFor="status">Status</Label>
            <Select
              value={watch('status')}
              onValueChange={(value) => setValue('status', value as DatasetInstanceStatus)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="active">Active</SelectItem>
                <SelectItem value="deprecated">Deprecated</SelectItem>
                <SelectItem value="retired">Retired</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Notes */}
          <div className="space-y-2">
            <Label htmlFor="notes">Notes</Label>
            <Textarea
              id="notes"
              placeholder="Optional notes about this instance..."
              {...register('notes')}
              rows={3}
            />
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={submitting}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={submitting}>
              {submitting && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              {isEditMode ? 'Update' : 'Create'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}


