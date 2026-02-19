import { Star } from 'lucide-react';
import { cn } from '@/lib/utils';

export interface StarRatingProps {
  /** Rating value (0-5, supports decimals for display) */
  value: number;
  /** Maximum stars to display */
  max?: number;
  /** Size variant */
  size?: 'sm' | 'md' | 'lg';
  /** Show numeric value alongside stars */
  showValue?: boolean;
  /** Additional class names */
  className?: string;
}

const sizeClasses = {
  sm: 'h-3 w-3',
  md: 'h-4 w-4',
  lg: 'h-5 w-5',
};

const textSizeClasses = {
  sm: 'text-xs',
  md: 'text-sm',
  lg: 'text-base',
};

/**
 * StarRating - Display component for showing star ratings
 * 
 * Supports partial stars via CSS clip-path for displaying averages.
 */
export function StarRating({
  value,
  max = 5,
  size = 'md',
  showValue = false,
  className,
}: StarRatingProps) {
  const clampedValue = Math.max(0, Math.min(value, max));
  const fullStars = Math.floor(clampedValue);
  const partialFill = clampedValue - fullStars;
  const emptyStars = max - Math.ceil(clampedValue);

  return (
    <div className={cn('inline-flex items-center gap-0.5', className)}>
      {/* Full stars */}
      {Array.from({ length: fullStars }).map((_, i) => (
        <Star
          key={`full-${i}`}
          className={cn(sizeClasses[size], 'fill-amber-400 text-amber-400')}
        />
      ))}

      {/* Partial star */}
      {partialFill > 0 && (
        <div className="relative">
          {/* Empty star background */}
          <Star className={cn(sizeClasses[size], 'text-muted-foreground/30')} />
          {/* Filled overlay with clip */}
          <div
            className="absolute inset-0 overflow-hidden"
            style={{ width: `${partialFill * 100}%` }}
          >
            <Star className={cn(sizeClasses[size], 'fill-amber-400 text-amber-400')} />
          </div>
        </div>
      )}

      {/* Empty stars */}
      {Array.from({ length: emptyStars }).map((_, i) => (
        <Star
          key={`empty-${i}`}
          className={cn(sizeClasses[size], 'text-muted-foreground/30')}
        />
      ))}

      {/* Numeric value */}
      {showValue && (
        <span className={cn('ml-1 text-muted-foreground', textSizeClasses[size])}>
          {clampedValue.toFixed(1)}
        </span>
      )}
    </div>
  );
}

export default StarRating;

