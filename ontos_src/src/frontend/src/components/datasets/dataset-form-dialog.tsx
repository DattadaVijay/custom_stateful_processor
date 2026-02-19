import { useState, useEffect } from 'react';
import { useForm } from 'react-hook-form';
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
import { useProjectContext } from '@/stores/project-store';
import { Loader2, Info } from 'lucide-react';
import type {
  Dataset,
  DatasetCreate,
  DatasetUpdate,
  DatasetStatus,
} from '@/types/dataset';

interface DatasetFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dataset?: Dataset; // If provided, we're in edit mode
  onSuccess: () => void;
}

interface FormData {
  name: string;
  description: string;
  contract_id: string;
  owner_team_id: string;
  project_id: string;
  status: DatasetStatus;
  version: string;
  published: boolean;
}

export default function DatasetFormDialog({
  open,
  onOpenChange,
  dataset,
  onSuccess,
}: DatasetFormDialogProps) {
  const { toast } = useToast();
  const { currentProject } = useProjectContext();
  const [submitting, setSubmitting] = useState(false);
  const [contracts, setContracts] = useState<{ id: string; name: string }[]>([]);
  const [teams, setTeams] = useState<{ id: string; name: string }[]>([]);
  const [projects, setProjects] = useState<{ id: string; name: string }[]>([]);

  const isEditMode = !!dataset;

  const {
    register,
    handleSubmit,
    reset,
    watch,
    setValue,
    formState: { errors },
  } = useForm<FormData>({
    defaultValues: {
      name: '',
      description: '',
      contract_id: '',
      owner_team_id: '',
      project_id: '',
      status: 'draft',
      version: '',
      published: false,
    },
  });

  // Load reference data
  useEffect(() => {
    if (open) {
      // Fetch contracts
      fetch('/api/data-contracts')
        .then((res) => res.json())
        .then((data) => setContracts(data.map((c: any) => ({ id: c.id, name: c.name }))))
        .catch(console.error);

      // Fetch teams
      fetch('/api/teams')
        .then((res) => res.json())
        .then((data) => setTeams(data.map((t: any) => ({ id: t.id, name: t.name }))))
        .catch(console.error);

      // Fetch projects
      fetch('/api/projects')
        .then((res) => res.json())
        .then((data) => setProjects(data.map((p: any) => ({ id: p.id, name: p.name }))))
        .catch(console.error);
    }
  }, [open]);

  // Reset form when dialog opens/closes or dataset changes
  useEffect(() => {
    if (open) {
      if (dataset) {
        reset({
          name: dataset.name,
          description: dataset.description || '',
          contract_id: dataset.contract_id || '',
          owner_team_id: dataset.owner_team_id || '',
          project_id: dataset.project_id || '',
          status: dataset.status as DatasetStatus,
          version: dataset.version || '',
          published: dataset.published || false,
        });
      } else {
        // For new datasets, default to current project if one is active
        reset({
          name: '',
          description: '',
          contract_id: '',
          owner_team_id: '',
          project_id: currentProject?.id || '',
          status: 'draft',
          version: '',
          published: false,
        });
      }
    }
  }, [open, dataset, reset, currentProject]);

  const onSubmit = async (data: FormData) => {
    setSubmitting(true);

    try {
      const payload: DatasetCreate | DatasetUpdate = {
        name: data.name,
        description: data.description || undefined,
        contract_id: data.contract_id || undefined,
        owner_team_id: data.owner_team_id || undefined,
        project_id: data.project_id || undefined,
        status: data.status,
        version: data.version || undefined,
        published: data.published,
      };

      const url = isEditMode ? `/api/datasets/${dataset.id}` : '/api/datasets';
      const method = isEditMode ? 'PUT' : 'POST';

      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || 'Failed to save dataset');
      }

      toast({
        title: 'Success',
        description: isEditMode ? 'Dataset updated successfully' : 'Dataset created successfully',
      });

      onSuccess();
    } catch (err) {
      toast({
        title: 'Error',
        description: err instanceof Error ? err.message : 'Failed to save dataset',
        variant: 'destructive',
      });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {isEditMode ? 'Edit Dataset' : 'Create New Dataset'}
          </DialogTitle>
          <DialogDescription>
            {isEditMode
              ? 'Update the dataset configuration'
              : 'Create a logical grouping for related data assets'}
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
          {/* Basic Information */}
          <div className="space-y-4">
            <h3 className="font-medium text-sm text-muted-foreground uppercase tracking-wide">
              Basic Information
            </h3>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="name">Name *</Label>
                <Input
                  id="name"
                  placeholder="e.g., Customer Master Data"
                  {...register('name', { required: 'Name is required' })}
                />
                {errors.name && (
                  <p className="text-sm text-destructive">{errors.name.message}</p>
                )}
              </div>

              <div className="space-y-2">
                <Label htmlFor="status">Status</Label>
                <Select
                  value={watch('status')}
                  onValueChange={(v) => setValue('status', v as DatasetStatus)}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="draft">Draft</SelectItem>
                    <SelectItem value="in_review">In Review</SelectItem>
                    <SelectItem value="active">Active</SelectItem>
                    <SelectItem value="deprecated">Deprecated</SelectItem>
                    <SelectItem value="retired">Retired</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="description">Description</Label>
              <Textarea
                id="description"
                placeholder="Describe this dataset and its purpose..."
                rows={3}
                {...register('description')}
              />
            </div>
          </div>

          {/* Contract & Ownership */}
          <div className="space-y-4">
            <h3 className="font-medium text-sm text-muted-foreground uppercase tracking-wide">
              Contract & Ownership
            </h3>

            <div className="space-y-2">
              <Label htmlFor="contract_id">Data Contract</Label>
              <Select
                value={watch('contract_id') || 'none'}
                onValueChange={(v) => setValue('contract_id', v === 'none' ? '' : v)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select a contract" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">No contract</SelectItem>
                  {contracts.map((contract) => (
                    <SelectItem key={contract.id} value={contract.id}>
                      {contract.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Optional: Link to a data contract for governance
              </p>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="owner_team_id">Owner Team</Label>
                <Select
                  value={watch('owner_team_id') || 'none'}
                  onValueChange={(v) => setValue('owner_team_id', v === 'none' ? '' : v)}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select a team" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">No owner team</SelectItem>
                    {teams.map((team) => (
                      <SelectItem key={team.id} value={team.id}>
                        {team.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label htmlFor="project_id">Project</Label>
                <Select
                  value={watch('project_id') || 'none'}
                  onValueChange={(v) => setValue('project_id', v === 'none' ? '' : v)}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select a project" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">No project</SelectItem>
                    {projects.map((project) => (
                      <SelectItem key={project.id} value={project.id}>
                        {project.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {/* Version */}
          <div className="space-y-4">
            <h3 className="font-medium text-sm text-muted-foreground uppercase tracking-wide">
              Version
            </h3>

            <div className="space-y-2">
              <Label htmlFor="version">Version</Label>
              <Input
                id="version"
                placeholder="e.g., 1.0.0"
                {...register('version')}
              />
            </div>
          </div>

          {/* Info about instances */}
          {!isEditMode && (
            <div className="rounded-lg bg-muted/50 p-4 flex gap-3">
              <Info className="h-5 w-5 text-muted-foreground shrink-0 mt-0.5" />
              <div className="text-sm text-muted-foreground">
                <p className="font-medium text-foreground mb-1">What&apos;s next?</p>
                <p>
                  After creating this dataset, you can add physical instances (tables, views)
                  from the dataset details page. Each instance can be linked to different
                  environments and contract versions.
                </p>
              </div>
            </div>
          )}

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
              {isEditMode ? 'Save Changes' : 'Create Dataset'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
