import { useState } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { useToast } from '@/hooks/use-toast';
import { Loader2, Bell } from 'lucide-react';

interface SubscribeDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  productId: string;
  productName: string;
  onSuccess?: () => void;
  /** If true, subscribes to a dataset instead of a data product */
  isDataset?: boolean;
}

export default function SubscribeDialog({
  open,
  onOpenChange,
  productId,
  productName,
  onSuccess,
  isDataset = false,
}: SubscribeDialogProps) {
  const { toast } = useToast();
  const [reason, setReason] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  const entityType = isDataset ? 'Dataset' : 'Data Product';
  const apiEndpoint = isDataset 
    ? `/api/datasets/${productId}/subscribe`
    : `/api/data-products/${productId}/subscribe`;

  const handleSubmit = async () => {
    setIsSubmitting(true);
    try {
      const response = await fetch(apiEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: reason.trim() || undefined }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `Failed to subscribe (${response.status})`);
      }

      toast({
        title: 'Subscribed!',
        description: `You are now subscribed to "${productName}". You'll receive notifications about updates and changes.`,
      });

      setReason('');
      onOpenChange(false);
      onSuccess?.();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to subscribe';
      toast({
        title: 'Subscription Failed',
        description: message,
        variant: 'destructive',
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleClose = () => {
    if (!isSubmitting) {
      setReason('');
      onOpenChange(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Bell className="h-5 w-5 text-primary" />
            Subscribe to {entityType}
          </DialogTitle>
          <DialogDescription>
            Subscribe to <span className="font-medium">{productName}</span> to receive notifications about updates, changes, and compliance status.
          </DialogDescription>
        </DialogHeader>

        <div className="py-4">
          <Label htmlFor="reason" className="text-sm font-medium">
            Why are you subscribing? <span className="text-muted-foreground">(optional)</span>
          </Label>
          <Textarea
            id="reason"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="E.g., Need this data for quarterly reporting, building a dashboard, etc."
            className="mt-2 min-h-[100px]"
            disabled={isSubmitting}
          />
          <p className="text-xs text-muted-foreground mt-2">
            This helps data owners understand how their products are being used.
          </p>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClose} disabled={isSubmitting}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={isSubmitting}>
            {isSubmitting ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Subscribing...
              </>
            ) : (
              <>
                <Bell className="mr-2 h-4 w-4" />
                Subscribe
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

