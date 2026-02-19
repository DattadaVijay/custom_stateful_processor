import { useEffect, useState } from 'react';
import { Star } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useApi } from '@/hooks/use-api';
import type { RatingAggregation } from '@/types/comments';

export interface RatingBadgeProps {
  /** Entity type (data_product, dataset, etc.) */
  entityType: string;
  /** Entity ID */
  entityId: string;
  /** Size variant */
  size?: 'sm' | 'md';
  /** Additional class names */
  className?: string;
  /** Click handler (optional) */
  onClick?: (e: React.MouseEvent) => void;
}

/**
 * RatingBadge - Compact inline rating display for cards
 * 
 * Fetches and displays rating data for an entity in a minimal badge format.
 * Shows nothing if no ratings exist.
 */
export function RatingBadge({
  entityType,
  entityId,
  size = 'sm',
  className,
  onClick,
}: RatingBadgeProps) {
  const { get } = useApi();
  const [rating, setRating] = useState<RatingAggregation | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    if (!entityType || !entityId) return;

    const fetchRating = async () => {
      try {
        const response = await get<RatingAggregation>(
          `/api/entities/${entityType}/${entityId}/ratings`
        );
        if (!response.error && response.data) {
          setRating(response.data);
        }
      } catch (error) {
        // Silent fail - ratings are not critical
      } finally {
        setLoaded(true);
      }
    };

    fetchRating();
  }, [entityType, entityId, get]);

  // Don't show anything until loaded or if no ratings
  if (!loaded || !rating || rating.total_ratings === 0) {
    return null;
  }

  const sizeClasses = size === 'sm' 
    ? 'h-3 w-3 text-[10px]' 
    : 'h-3.5 w-3.5 text-xs';

  return (
    <div
      className={cn(
        'inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full bg-amber-50 dark:bg-amber-950/30',
        onClick && 'cursor-pointer hover:bg-amber-100 dark:hover:bg-amber-900/40 transition-colors',
        className
      )}
      onClick={onClick}
      title={`${rating.average_rating.toFixed(1)} out of 5 stars (${rating.total_ratings} ratings)`}
    >
      <Star className={cn(sizeClasses, 'fill-amber-400 text-amber-400')} />
      <span className={cn(
        sizeClasses,
        'font-medium text-amber-700 dark:text-amber-300 tabular-nums'
      )}>
        {rating.average_rating.toFixed(1)}
      </span>
      <span className={cn(
        sizeClasses,
        'text-amber-600/70 dark:text-amber-400/70 ml-1.5'
      )}>
        ({rating.total_ratings})
      </span>
    </div>
  );
}

export default RatingBadge;

