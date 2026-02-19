import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useToast } from '@/hooks/use-toast';
import { useApi } from '@/hooks/use-api';
import { useNotificationsStore } from '@/stores/notifications-store';
import { Loader2, AlertCircle, FileText, Eye, Rocket, RefreshCw, XCircle } from 'lucide-react';
import AccessRequestFields from '@/components/access/access-request-fields';

type RequestType = 'access' | 'review' | 'publish' | 'status_change';

interface RequestDatasetActionDialogProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  datasetId: string;
  datasetName?: string;
  datasetStatus?: string;
  datasetPublished?: boolean;
  onSuccess?: () => void;
  /** If true, status changes are applied directly without approval workflow */
  canDirectStatusChange?: boolean;
}

// Dataset lifecycle transitions (simpler than ODPS/ODCS)
const ALLOWED_TRANSITIONS: Record<string, { target: string; label: string }[]> = {
  'draft': [
    { target: 'active', label: 'Activate' },
    { target: 'deprecated', label: 'Deprecate' },
  ],
  'in_review': [
    { target: 'draft', label: 'Cancel Review' },
    { target: 'active', label: 'Approve & Activate' },
  ],
  'active': [
    { target: 'deprecated', label: 'Deprecate' },
  ],
  'deprecated': [
    { target: 'retired', label: 'Retire' },
    { target: 'active', label: 'Reactivate' },
  ],
  'retired': [], // Terminal state
};

function getAllowedTransitions(status: string): { target: string; label: string }[] {
  return ALLOWED_TRANSITIONS[status.toLowerCase()] || [];
}

export default function RequestDatasetActionDialog({
  isOpen,
  onOpenChange,
  datasetId,
  datasetName,
  datasetStatus,
  datasetPublished,
  onSuccess,
  canDirectStatusChange = false
}: RequestDatasetActionDialogProps) {
  const { post } = useApi();
  const { toast } = useToast();
  const refreshNotifications = useNotificationsStore((state) => state.refreshNotifications);
  
  const [requestType, setRequestType] = useState<RequestType>('status_change');
  const [message, setMessage] = useState('');
  const [justification, setJustification] = useState('');
  const [targetStatus, setTargetStatus] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedDuration, setSelectedDuration] = useState<number>(30);

  // Reset form when dialog opens/closes
  useEffect(() => {
    if (isOpen) {
      setRequestType('status_change');
      setMessage('');
      setJustification('');
      setTargetStatus('');
      setError(null);
      setSelectedDuration(30);
    }
  }, [isOpen]);

  const getRequestTypeConfig = (type: RequestType) => {
    switch (type) {
      case 'access':
        return {
          icon: <Eye className="h-5 w-5" />,
          title: 'Request Access to Dataset',
          description: 'Request permission to view and use this dataset.',
          enabled: true,
          endpoint: '/api/access-grants/request',
        };
      case 'review':
        return {
          icon: <FileText className="h-5 w-5" />,
          title: 'Request Data Steward Review',
          description: 'Submit this dataset for review by a data steward.',
          enabled: datasetStatus?.toLowerCase() === 'draft',
          endpoint: `/api/datasets/${datasetId}/request-review`,
        };
      case 'publish':
        const isPublished = datasetPublished === true;
        return {
          icon: isPublished ? <XCircle className="h-5 w-5" /> : <Rocket className="h-5 w-5" />,
          title: isPublished ? 'Unpublish from Marketplace' : 'Publish to Marketplace',
          description: isPublished 
            ? 'Remove this dataset from the organization-wide marketplace.'
            : 'Publish this dataset to the organization-wide marketplace.',
          enabled: ['active', 'approved', 'certified'].includes(datasetStatus?.toLowerCase() || ''),
          endpoint: isPublished 
            ? `/api/datasets/${datasetId}/unpublish`
            : `/api/datasets/${datasetId}/publish`,
        };
      case 'status_change':
        const allowedTransitions = datasetStatus ? getAllowedTransitions(datasetStatus) : [];
        return {
          icon: <RefreshCw className="h-5 w-5" />,
          title: canDirectStatusChange ? 'Change Status' : 'Request Status Change',
          description: canDirectStatusChange 
            ? 'Directly change the lifecycle status of this dataset.'
            : 'Request approval to change the lifecycle status of this dataset.',
          enabled: allowedTransitions.length > 0,
          endpoint: canDirectStatusChange 
            ? `/api/datasets/${datasetId}/change-status`
            : `/api/datasets/${datasetId}/request-status-change`,
        };
    }
  };

  const validateForm = (): boolean => {
    setError(null);
    
    if (requestType === 'access') {
      if (!message.trim()) {
        setError('Please provide a reason for requesting access');
        return false;
      }
      if (message.trim().length < 10) {
        setError('Please provide a more detailed reason (at least 10 characters)');
        return false;
      }
    }
    
    if (requestType === 'status_change') {
      if (!targetStatus) {
        setError('Please select a target status');
        return false;
      }
      // Justification is only required for approval requests, not direct changes
      if (!canDirectStatusChange) {
        if (!justification.trim()) {
          setError('Please provide a justification for the status change');
          return false;
        }
        if (justification.trim().length < 20) {
          setError('Please provide a more detailed justification (at least 20 characters)');
          return false;
        }
      }
    }
    
    return true;
  };

  const handleSubmit = async () => {
    if (!validateForm()) {
      return;
    }

    const config = getRequestTypeConfig(requestType);
    if (!config.enabled) {
      setError(`Cannot request ${requestType} for a dataset with status '${datasetStatus}'`);
      return;
    }

    setError(null);
    setSubmitting(true);

    try {
      let payload: any;
      
      if (requestType === 'access') {
        payload = {
          entity_type: 'dataset',
          entity_id: datasetId,
          reason: message.trim(),
          requested_permission_level: 'READ',
          requested_duration_days: selectedDuration,
        };
      } else if (requestType === 'review') {
        payload = {
          message: message.trim() || undefined,
        };
      } else if (requestType === 'publish') {
        // Publish/unpublish don't need a payload
        payload = {};
      } else if (requestType === 'status_change') {
        if (canDirectStatusChange) {
          payload = {
            new_status: targetStatus,
          };
        } else {
          payload = {
            target_status: targetStatus,
            justification: justification.trim(),
            current_status: datasetStatus,
          };
        }
      }

      const response = await post(config.endpoint, payload);

      if (response.error) {
        throw new Error(response.error);
      }

      // Different success messages based on request type
      if (requestType === 'publish') {
        const isPublished = datasetPublished === true;
        toast({
          title: isPublished ? 'Unpublished' : 'Published',
          description: isPublished 
            ? 'Dataset removed from marketplace.'
            : 'Dataset published to marketplace.'
        });
      } else if (requestType === 'status_change' && canDirectStatusChange) {
        toast({
          title: 'Status Changed',
          description: `Dataset status changed from "${datasetStatus}" to "${targetStatus}".`
        });
      } else {
        toast({
          title: 'Request Submitted',
          description: `Your ${requestType} request has been submitted and you will be notified of the decision.`
        });
      }

      // Refresh notifications
      refreshNotifications();

      // Call success callback
      if (onSuccess) {
        onSuccess();
      }

      // Reset form and close dialog
      setMessage('');
      setJustification('');
      setTargetStatus('');
      onOpenChange(false);

    } catch (e: any) {
      setError(e.message || 'Failed to submit request');
      toast({
        title: 'Error',
        description: e.message || 'Failed to submit request',
        variant: 'destructive'
      });
    } finally {
      setSubmitting(false);
    }
  };

  const allowedTransitions = datasetStatus ? getAllowedTransitions(datasetStatus) : [];
  const config = getRequestTypeConfig(requestType);

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            {config.icon}
            {config.title}
          </DialogTitle>
          <DialogDescription>
            {datasetName && <span className="font-medium">{datasetName}</span>}
            {datasetStatus && <span className="text-muted-foreground ml-2">({datasetStatus})</span>}
            {datasetPublished && <span className="text-green-600 ml-2">[Published]</span>}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* Request Type Selector */}
          <div className="space-y-2">
            <Label htmlFor="request-type">Request Type</Label>
            <Select value={requestType} onValueChange={(value) => setRequestType(value as RequestType)}>
              <SelectTrigger id="request-type">
                <SelectValue placeholder="Select request type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="status_change">
                  <div className="flex items-center gap-2">
                    <RefreshCw className="h-4 w-4" />
                    {canDirectStatusChange ? 'Change Status' : 'Request Status Change'}
                  </div>
                </SelectItem>
                <SelectItem value="review" disabled={!getRequestTypeConfig('review').enabled}>
                  <div className="flex items-center gap-2">
                    <FileText className="h-4 w-4" />
                    Request Steward Review
                  </div>
                </SelectItem>
                <SelectItem value="publish" disabled={!getRequestTypeConfig('publish').enabled}>
                  <div className="flex items-center gap-2">
                    {datasetPublished ? <XCircle className="h-4 w-4" /> : <Rocket className="h-4 w-4" />}
                    {datasetPublished ? 'Unpublish from Marketplace' : 'Publish to Marketplace'}
                  </div>
                </SelectItem>
                <SelectItem value="access">
                  <div className="flex items-center gap-2">
                    <Eye className="h-4 w-4" />
                    Request Access
                  </div>
                </SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">{config.description}</p>
          </div>

          {/* Status Change Fields */}
          {requestType === 'status_change' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="target-status">Target Status *</Label>
                <Select value={targetStatus} onValueChange={setTargetStatus} disabled={submitting}>
                  <SelectTrigger id="target-status">
                    <SelectValue placeholder="Select target status" />
                  </SelectTrigger>
                  <SelectContent>
                    {allowedTransitions.map((transition) => (
                      <SelectItem key={transition.target} value={transition.target}>
                        {transition.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {allowedTransitions.length === 0 && (
                  <p className="text-xs text-muted-foreground">
                    No status transitions available from current status.
                  </p>
                )}
              </div>

              {/* Justification - only for approval requests */}
              {!canDirectStatusChange && (
                <div className="space-y-2">
                  <Label htmlFor="status-justification">Justification *</Label>
                  <Textarea
                    id="status-justification"
                    value={justification}
                    onChange={(e) => setJustification(e.target.value)}
                    placeholder="Explain why this status change is needed and any relevant context..."
                    className="min-h-[100px] resize-none"
                    disabled={submitting}
                  />
                  <div className="text-xs text-muted-foreground">
                    Minimum 20 characters required. This will be reviewed by an admin.
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Access Request Fields - using shared component */}
          {requestType === 'access' && (
            <AccessRequestFields
              entityType="dataset"
              message={message}
              onMessageChange={setMessage}
              selectedDuration={selectedDuration}
              onDurationChange={setSelectedDuration}
              disabled={submitting}
            />
          )}

          {/* Review Request Message */}
          {requestType === 'review' && (
            <div className="space-y-2">
              <Label htmlFor="review-message">Message (optional)</Label>
              <Textarea
                id="review-message"
                value={message}
                onChange={(e) => setMessage(e.target.value)}
                placeholder="Add any notes for the reviewer..."
                className="min-h-[80px] resize-none"
                disabled={submitting}
              />
            </div>
          )}

          {/* Publish confirmation */}
          {requestType === 'publish' && (
            <div className="p-3 bg-muted/50 rounded-lg border">
              <p className="text-sm text-muted-foreground">
                {datasetPublished 
                  ? 'This will remove the dataset from the marketplace. Existing subscribers will still have access.'
                  : 'This will make the dataset visible in the organization-wide marketplace for discovery.'}
              </p>
            </div>
          )}

          {/* Error Display */}
          {error && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={submitting || !config.enabled}>
            {submitting ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                {requestType === 'status_change' && canDirectStatusChange ? 'Changing Status...' : 
                 requestType === 'publish' ? (datasetPublished ? 'Unpublishing...' : 'Publishing...') :
                 'Sending Request...'}
              </>
            ) : (
              requestType === 'status_change' && canDirectStatusChange ? 'Change Status' : 
              requestType === 'publish' ? (datasetPublished ? 'Unpublish' : 'Publish') :
              'Send Request'
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

