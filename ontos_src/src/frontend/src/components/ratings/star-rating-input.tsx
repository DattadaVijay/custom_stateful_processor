import { useState } from 'react';
import { Star } from 'lucide-react';
import { cn } from '@/lib/utils';

export interface StarRatingInputProps {
  /** Current rating value */
  value?: number;
  /** Callback when rating changes */
  onChange: (rating: number) => void;
  /** Maximum stars */
  max?: number;
  /** Size variant */
  size?: 'sm' | 'md' | 'lg';
  /** Disabled state */
  disabled?: boolean;
  /** Additional class names */
  className?: string;
}

const sizeClasses = {
  sm: 'h-5 w-5',
  md: 'h-6 w-6',
  lg: 'h-8 w-8',
};

/**
 * StarRatingInput - Interactive star rating selector
 * 
 * Allows users to click on stars to select a rating.
 * Shows hover preview before selection.
 */
export function StarRatingInput({
  value = 0,
  onChange,
  max = 5,
  size = 'md',
  disabled = false,
  className,
}: StarRatingInputProps) {
  const [hoverValue, setHoverValue] = useState<number | null>(null);

  const displayValue = hoverValue ?? value;

  const handleClick = (rating: number) => {
    if (!disabled) {
      onChange(rating);
    }
  };

  const handleMouseEnter = (rating: number) => {
    if (!disabled) {
      setHoverValue(rating);
    }
  };

  const handleMouseLeave = () => {
    setHoverValue(null);
  };

  return (
    <div
      className={cn(
        'inline-flex items-center gap-1',
        disabled && 'opacity-50 cursor-not-allowed',
        className
      )}
      onMouseLeave={handleMouseLeave}
    >
      {Array.from({ length: max }).map((_, index) => {
        const starValue = index + 1;
        const isFilled = starValue <= displayValue;
        const isHovering = hoverValue !== null;

        return (
          <button
            key={starValue}
            type="button"
            disabled={disabled}
            onClick={() => handleClick(starValue)}
            onMouseEnter={() => handleMouseEnter(starValue)}
            className={cn(
              'transition-all duration-150 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-1 rounded-sm',
              !disabled && 'cursor-pointer hover:scale-110',
              isHovering && 'transform'
            )}
            aria-label={`Rate ${starValue} out of ${max} stars`}
          >
            <Star
              className={cn(
                sizeClasses[size],
                'transition-colors duration-150',
                isFilled
                  ? 'fill-amber-400 text-amber-400'
                  : 'text-muted-foreground/40 hover:text-amber-300'
              )}
            />
          </button>
        );
      })}
    </div>
  );
}

export default StarRatingInput;

