/**
 * @deprecated This component has been replaced by RequestAccessWithDurationDialog.
 * This file re-exports the new component for backward compatibility.
 */
import RequestAccessWithDurationDialog, {
  RequestAccessWithDurationDialogProps,
} from './request-access-with-duration-dialog';

export type RequestAccessDialogProps = RequestAccessWithDurationDialogProps;

// Re-export with the old name for backward compatibility
export default RequestAccessWithDurationDialog;

// Also export the new component directly
export { RequestAccessWithDurationDialog };
