/**
 * Workflow label utilities.
 * 
 * Provides consistent, localized labels for workflow trigger types,
 * entity types, and step types throughout the application.
 */

import { TFunction } from 'i18next';
import {
  Shield,
  UserCheck,
  Bell,
  Tag,
  Code,
  CheckCircle,
  XCircle,
  ClipboardCheck,
  Truck,
  GitBranch,
  FileSearch,
  Globe,
  type LucideIcon,
} from 'lucide-react';
import { TriggerType, EntityType, StepType } from '@/types/process-workflow';

/**
 * Icons for each step type.
 */
export const STEP_ICONS: Record<StepType, LucideIcon> = {
  validation: Shield,
  approval: UserCheck,
  notification: Bell,
  assign_tag: Tag,
  remove_tag: Tag,
  conditional: GitBranch,
  script: Code,
  pass: CheckCircle,
  fail: XCircle,
  policy_check: ClipboardCheck,
  delivery: Truck,
  create_asset_review: FileSearch,
  webhook: Globe,
};

/**
 * Colors for each step type (Tailwind color names without prefix).
 */
export const STEP_COLORS: Record<StepType, string> = {
  validation: 'blue',
  approval: 'amber',
  notification: 'green',
  assign_tag: 'violet',
  remove_tag: 'rose',
  conditional: 'slate',
  script: 'cyan',
  pass: 'emerald',
  fail: 'red',
  policy_check: 'orange',
  delivery: 'indigo',
  create_asset_review: 'teal',
  webhook: 'orange',
};

/**
 * Get the icon for a step type with fallback.
 */
export function getStepIcon(type: StepType | string): LucideIcon {
  return STEP_ICONS[type as StepType] || Code;
}

/**
 * Get the color for a step type with fallback.
 */
export function getStepColor(type: StepType | string): string {
  return STEP_COLORS[type as StepType] || 'slate';
}

/**
 * Get a human-readable label for a trigger type.
 */
export function getTriggerTypeLabel(type: TriggerType, t: TFunction): string {
  return t(`common:workflows.triggerTypes.${type}`, { defaultValue: formatFallback(type) });
}

/**
 * Get a human-readable label for an entity type.
 */
export function getEntityTypeLabel(type: EntityType, t: TFunction): string {
  return t(`common:workflows.entityTypes.${type}`, { defaultValue: formatFallback(type) });
}

/**
 * Get a human-readable label for a step type.
 */
export function getStepTypeLabel(type: StepType, t: TFunction): string {
  return t(`common:workflows.stepTypes.${type}`, { defaultValue: formatFallback(type) });
}

/**
 * Get a formatted trigger display string including entity types.
 * Example: "On Create (Table, View)"
 */
export function getTriggerDisplay(
  trigger: { type: TriggerType; entity_types: EntityType[] },
  t: TFunction
): string {
  const typeLabel = getTriggerTypeLabel(trigger.type, t);
  
  if (!trigger.entity_types || trigger.entity_types.length === 0) {
    return typeLabel;
  }
  
  const entityLabels = trigger.entity_types
    .map(et => getEntityTypeLabel(et, t))
    .join(', ');
  
  return `${typeLabel} (${entityLabels})`;
}

/**
 * Format a snake_case or kebab-case string as a fallback label.
 * Example: "on_request_review" -> "On Request Review"
 */
function formatFallback(value: string): string {
  return value
    .replace(/[_-]/g, ' ')
    .replace(/\b\w/g, char => char.toUpperCase());
}

/**
 * All trigger types for use in selectors/dropdowns.
 */
export const ALL_TRIGGER_TYPES: TriggerType[] = [
  'on_create',
  'on_update',
  'on_delete',
  'on_status_change',
  'scheduled',
  'manual',
  'before_create',
  'before_update',
  'on_request_review',
  'on_request_access',
  'on_request_publish',
  'on_request_status_change',
  'on_job_success',
  'on_job_failure',
  'on_subscribe',
  'on_unsubscribe',
  'on_expiring',
  'on_revoke',
];

/**
 * All entity types for use in selectors/dropdowns.
 */
export const ALL_ENTITY_TYPES: EntityType[] = [
  'catalog',
  'schema',
  'table',
  'view',
  'data_contract',
  'data_product',
  'dataset',
  'domain',
  'project',
  'access_grant',
  'role',
  'data_asset_review',
  'job',
  'subscription',
];

/**
 * All step types for use in selectors/dropdowns.
 */
export const ALL_STEP_TYPES: StepType[] = [
  'validation',
  'approval',
  'notification',
  'assign_tag',
  'remove_tag',
  'conditional',
  'script',
  'pass',
  'fail',
  'policy_check',
  'delivery',
  'create_asset_review',
  'webhook',
];

/**
 * Special recipient values that are not role UUIDs.
 */
export const SPECIAL_RECIPIENTS: Record<string, string> = {
  'requester': 'Requester',
  'owner': 'Owner',
  'domain_owners': 'Domain Owners',
  'project_owners': 'Project Owners',
  'data_stewards': 'Data Stewards',
  'admins': 'Administrators',
};

/**
 * Resolve an approver/recipient identifier to a display name.
 * Handles both UUIDs (resolved via roles map) and special values.
 */
export function resolveRecipientDisplay(
  value: string | undefined,
  rolesMap: Record<string, string>
): string {
  if (!value) return 'Not configured';
  
  // Check special values first
  if (value in SPECIAL_RECIPIENTS) {
    return SPECIAL_RECIPIENTS[value];
  }
  
  // Check if it's a role UUID
  if (value in rolesMap) {
    return rolesMap[value];
  }
  
  // Fallback: return raw value (might be email or legacy role name)
  return value;
}

