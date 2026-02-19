import { Link } from 'react-router-dom';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Database, FileText, Users, Bell, Server } from 'lucide-react';
import { RelativeDate } from '@/components/common/relative-date';
import type {
  DatasetListItem,
  DatasetStatus,
} from '@/types/dataset';
import {
  DATASET_STATUS_LABELS,
  DATASET_STATUS_COLORS,
} from '@/types/dataset';

interface DatasetCardProps {
  dataset: DatasetListItem;
}

export default function DatasetCard({ dataset }: DatasetCardProps) {
  const status = dataset.status as DatasetStatus;

  return (
    <Link to={`/datasets/${dataset.id}`}>
      <Card className="h-full hover:shadow-md transition-shadow cursor-pointer">
        <CardHeader className="pb-2">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-2">
              <Database className="h-5 w-5 text-muted-foreground" />
              <CardTitle className="text-lg">{dataset.name}</CardTitle>
            </div>
            <div className="flex gap-1">
              <Badge
                variant="outline"
                className={DATASET_STATUS_COLORS[status] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'}
              >
                {DATASET_STATUS_LABELS[status] || status}
              </Badge>
            </div>
          </div>
          {dataset.description && (
            <CardDescription className="text-sm line-clamp-2">
              {dataset.description}
            </CardDescription>
          )}
        </CardHeader>
        <CardContent className="space-y-3">
          {/* Instances and Published */}
          <div className="flex items-center gap-2">
            {dataset.instance_count !== undefined && dataset.instance_count > 0 && (
              <Badge variant="secondary" className="flex items-center gap-1">
                <Server className="h-3 w-3" />
                {dataset.instance_count} instance{dataset.instance_count !== 1 ? 's' : ''}
              </Badge>
            )}
            {dataset.published && (
              <Badge variant="secondary">Published</Badge>
            )}
            {dataset.version && (
              <Badge variant="outline" className="font-mono">v{dataset.version}</Badge>
            )}
          </div>

          {/* Contract */}
          {dataset.contract_id && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <FileText className="h-4 w-4" />
              <span className="truncate">{dataset.contract_name || 'Linked contract'}</span>
            </div>
          )}

          {/* Footer */}
          <div className="flex items-center justify-between text-sm text-muted-foreground pt-2 border-t">
            <div className="flex items-center gap-1">
              {dataset.owner_team_name && (
                <>
                  <Users className="h-3 w-3" />
                  <span className="truncate max-w-[100px]">{dataset.owner_team_name}</span>
                </>
              )}
            </div>
            <div className="flex items-center gap-3">
              {dataset.subscriber_count !== undefined && dataset.subscriber_count > 0 && (
                <div className="flex items-center gap-1">
                  <Bell className="h-3 w-3" />
                  <span>{dataset.subscriber_count}</span>
                </div>
              )}
              <RelativeDate date={dataset.updated_at} />
            </div>
          </div>
        </CardContent>
      </Card>
    </Link>
  );
}
