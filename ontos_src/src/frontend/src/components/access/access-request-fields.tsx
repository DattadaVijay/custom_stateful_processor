import { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

export interface AccessRequestFieldsProps {
  entityType: 'data_product' | 'dataset' | 'data_contract';
  message: string;
  onMessageChange: (value: string) => void;
  selectedDuration: number;
  onDurationChange: (value: number) => void;
  disabled?: boolean;
}

/**
 * Shared component for access request form fields.
 * Includes reason textarea and duration selector with localization.
 */
export default function AccessRequestFields({
  entityType,
  message,
  onMessageChange,
  selectedDuration,
  onDurationChange,
  disabled = false,
}: AccessRequestFieldsProps) {
  const { t } = useTranslation('access-grants');
  
  const [durationOptions, setDurationOptions] = useState<number[]>([30, 90, 180, 365]);
  const [loadingConfig, setLoadingConfig] = useState(false);

  // Fetch duration config from API
  useEffect(() => {
    setLoadingConfig(true);
    fetch(`/api/access-grants/config/${entityType}/options`)
      .then(res => res.json())
      .then(data => {
        if (data.duration_options && data.duration_options.length > 0) {
          setDurationOptions(data.duration_options);
          // Only update selected duration if current isn't in options
          if (!data.duration_options.includes(selectedDuration)) {
            onDurationChange(data.default_duration || data.duration_options[0]);
          }
        }
      })
      .catch(() => {
        // Use defaults on error
        setDurationOptions([30, 90, 180, 365]);
      })
      .finally(() => setLoadingConfig(false));
  }, [entityType]);

  // Format duration for display
  const formatDuration = (days: number): string => {
    if (days < 30) {
      return t('request.duration.days', { count: days });
    } else if (days < 365) {
      const months = Math.round(days / 30);
      return t('request.duration.months', { count: months });
    } else {
      const years = Math.round(days / 365);
      return t('request.duration.years', { count: years });
    }
  };

  const entityTypeLabel = t(`entityTypes.${entityType}`);

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Label htmlFor="access-message">
          {t('request.reason.label')} *
        </Label>
        <Textarea
          id="access-message"
          value={message}
          onChange={(e) => onMessageChange(e.target.value)}
          placeholder={t('request.reason.placeholder', { entityType: entityTypeLabel })}
          className="min-h-[100px] resize-none"
          disabled={disabled}
        />
      </div>
      
      <div className="space-y-2">
        <Label htmlFor="access-duration">
          {t('request.duration.label')}
        </Label>
        <Select 
          value={selectedDuration.toString()} 
          onValueChange={(v) => onDurationChange(parseInt(v, 10))}
          disabled={disabled || loadingConfig}
        >
          <SelectTrigger id="access-duration">
            <SelectValue placeholder={t('request.duration.placeholder')} />
          </SelectTrigger>
          <SelectContent>
            {durationOptions.map((days) => (
              <SelectItem key={days} value={days.toString()}>
                {formatDuration(days)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <p className="text-xs text-muted-foreground">
          {t('request.duration.helpText')}
        </p>
      </div>
    </div>
  );
}

