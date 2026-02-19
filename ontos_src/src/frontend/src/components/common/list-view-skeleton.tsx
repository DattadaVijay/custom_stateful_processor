import { Skeleton } from '@/components/ui/skeleton';

interface ListViewSkeletonProps {
  /** Number of columns to show in the table header/rows */
  columns?: number;
  /** Number of rows to show */
  rows?: number;
  /** Whether to show the toolbar skeleton */
  showToolbar?: boolean;
  /** Whether to show the pagination skeleton */
  showPagination?: boolean;
  /** Number of action buttons in the toolbar */
  toolbarButtons?: number;
}

/**
 * Reusable skeleton loading state for list views with DataTable.
 * Provides a consistent loading experience across all list views.
 */
export function ListViewSkeleton({
  columns = 6,
  rows = 5,
  showToolbar = true,
  showPagination = true,
  toolbarButtons = 2,
}: ListViewSkeletonProps) {
  return (
    <div className="space-y-4">
      {/* Toolbar skeleton */}
      {showToolbar && (
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Skeleton className="h-9 w-64" />
            <Skeleton className="h-9 w-24" />
          </div>
          <div className="flex items-center gap-2">
            {[...Array(toolbarButtons)].map((_, i) => (
              <Skeleton key={i} className="h-9 w-32" />
            ))}
          </div>
        </div>
      )}

      {/* Table skeleton */}
      <div className="border rounded-lg">
        {/* Header row */}
        <div className="border-b p-3 bg-muted/30">
          <div className="flex gap-4 items-center">
            <Skeleton className="h-4 w-4" />
            {[...Array(columns)].map((_, i) => (
              <Skeleton 
                key={i} 
                className="h-4" 
                style={{ width: `${Math.random() * 40 + 60}px` }} 
              />
            ))}
          </div>
        </div>
        {/* Data rows */}
        {[...Array(rows)].map((_, rowIndex) => (
          <div key={rowIndex} className="border-b p-3 last:border-b-0">
            <div className="flex gap-4 items-center">
              <Skeleton className="h-4 w-4" />
              {[...Array(columns)].map((_, colIndex) => (
                <Skeleton 
                  key={colIndex} 
                  className="h-4" 
                  style={{ width: `${Math.random() * 60 + 40}px` }} 
                />
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Pagination skeleton */}
      {showPagination && (
        <div className="flex items-center justify-between">
          <Skeleton className="h-4 w-32" />
          <div className="flex items-center gap-2">
            <Skeleton className="h-8 w-24" />
            <Skeleton className="h-8 w-20" />
            <Skeleton className="h-8 w-8" />
            <Skeleton className="h-8 w-8" />
            <Skeleton className="h-8 w-8" />
            <Skeleton className="h-8 w-8" />
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * Skeleton for detail view headers with back button and action buttons.
 */
export function DetailHeaderSkeleton({ actionButtons = 3 }: { actionButtons?: number }) {
  return (
    <div className="flex items-center justify-between">
      <Skeleton className="h-9 w-32" />
      <div className="flex items-center gap-2">
        {[...Array(actionButtons)].map((_, i) => (
          <Skeleton key={i} className="h-9 w-24" />
        ))}
      </div>
    </div>
  );
}

/**
 * Skeleton for a card with title and content.
 */
export function CardSkeleton({ 
  titleWidth = 'w-48',
  descriptionWidth = 'w-64',
  contentRows = 3,
}: { 
  titleWidth?: string;
  descriptionWidth?: string;
  contentRows?: number;
}) {
  return (
    <div className="border rounded-lg p-6 space-y-4">
      <div>
        <Skeleton className={`h-6 ${titleWidth} mb-2`} />
        <Skeleton className={`h-4 ${descriptionWidth}`} />
      </div>
      <div className="space-y-3">
        {[...Array(contentRows)].map((_, i) => (
          <Skeleton key={i} className="h-4 w-full" />
        ))}
      </div>
    </div>
  );
}

/**
 * Skeleton for metadata grid (used in detail views).
 */
export function MetadataGridSkeleton({ items = 6 }: { items?: number }) {
  return (
    <div className="grid md:grid-cols-3 gap-x-6 gap-y-3">
      {[...Array(items)].map((_, i) => (
        <div key={i} className="flex items-center gap-2">
          <Skeleton className="h-4 w-16" />
          <Skeleton className="h-4 w-24" />
        </div>
      ))}
    </div>
  );
}

/**
 * Comprehensive skeleton for detail views.
 * Shows header with back button, main card with metadata, and optional additional cards.
 */
export function DetailViewSkeleton({ 
  cards = 3,
  actionButtons = 3,
}: { 
  cards?: number;
  actionButtons?: number;
}) {
  return (
    <div className="py-6 space-y-6">
      {/* Header skeleton */}
      <div className="flex items-center justify-between">
        <Skeleton className="h-9 w-32" />
        <div className="flex items-center gap-2">
          {[...Array(actionButtons)].map((_, i) => (
            <Skeleton key={i} className="h-9 w-24" />
          ))}
        </div>
      </div>

      {/* Core Metadata Card skeleton */}
      <div className="border rounded-lg">
        <div className="p-6 border-b">
          <div className="flex items-center gap-3">
            <Skeleton className="h-7 w-7 rounded" />
            <Skeleton className="h-7 w-64" />
          </div>
          <Skeleton className="h-4 w-96 mt-2" />
        </div>
        <div className="p-6 space-y-3">
          <div className="grid md:grid-cols-3 gap-x-6 gap-y-2">
            {[...Array(6)].map((_, i) => (
              <div key={i} className="flex items-center gap-2">
                <Skeleton className="h-4 w-16" />
                <Skeleton className="h-4 w-24" />
              </div>
            ))}
          </div>
          <div className="pt-3 border-t">
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
        </div>
      </div>

      {/* Additional cards skeleton */}
      {[...Array(cards - 1)].map((_, cardIndex) => (
        <div key={cardIndex} className="border rounded-lg">
          <div className="p-6 border-b">
            <div className="flex items-center gap-2">
              <Skeleton className="h-5 w-5" />
              <Skeleton className="h-5 w-32" />
            </div>
            <Skeleton className="h-4 w-56 mt-1" />
          </div>
          <div className="p-6">
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

