import { useParams } from 'react-router-dom';
import WorkflowDesigner from '@/components/workflows/workflow-designer';

export default function WorkflowDesignerView() {
  const { workflowId } = useParams();
  
  return (
    <WorkflowDesigner workflowId={workflowId} />
  );
}

