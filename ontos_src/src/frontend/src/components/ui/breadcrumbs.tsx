import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { ChevronRight, Home as HomeIcon, Store, LayoutDashboard } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import useBreadcrumbStore from '@/stores/breadcrumb-store';
import { useViewModeStore, ViewMode } from '@/stores/view-mode-store';
import { usePermissions } from '@/stores/permissions-store';
import { FeatureAccessLevel } from '@/types/settings';
import { Button } from '@/components/ui/button';

interface BreadcrumbsProps extends React.HTMLAttributes<HTMLElement> {}

export function Breadcrumbs({ className, ...props }: BreadcrumbsProps) {
  const { t } = useTranslation('home');
  const location = useLocation();
  const { staticSegments, dynamicTitle } = useBreadcrumbStore();
  const { viewMode, setViewMode } = useViewModeStore();
  const { permissions, isLoading: permissionsLoading } = usePermissions();

  // Check if we're on the home page
  const isHomePage = location.pathname === '/';

  // Determine if user has any access
  const hasAnyAccess = React.useMemo(() => {
    if (permissionsLoading || !permissions) return false;
    return Object.values(permissions).some(level => level !== FeatureAccessLevel.NONE);
  }, [permissions, permissionsLoading]);

  // Determine if user has management capabilities
  const hasManagementAccess = React.useMemo(() => {
    if (permissionsLoading || !permissions) return false;
    const managementFeatures = ['data-products', 'data-contracts', 'data-domains', 'semantic-models'];
    return managementFeatures.some(feature => {
      const level = permissions[feature];
      return level === FeatureAccessLevel.READ_WRITE || level === FeatureAccessLevel.ADMIN;
    });
  }, [permissions, permissionsLoading]);

  // Show toggle only on home page when user has both consumer and management access
  const showModeToggle = isHomePage && hasAnyAccess && hasManagementAccess;

  // Effective view mode for consumer-only users
  const effectiveViewMode: ViewMode = hasAnyAccess && !hasManagementAccess ? 'consumer' : viewMode;

  return (
    <div className={cn('flex items-center justify-between mb-4', className)}>
      <nav
        aria-label="breadcrumb"
        className="text-sm text-muted-foreground"
        {...props}
      >
        <ol className="list-none p-0 inline-flex items-center space-x-1">
        {/* Home Icon Link */}
        <li>
          <Link to="/" className="flex items-center hover:text-primary">
            <HomeIcon className="h-4 w-4 mr-1.5" />
          </Link>
        </li>

        {/* Static Segments */}
        {staticSegments.map((segment, index) => (
          <React.Fragment key={segment.path || index}>
            <li className="flex items-center">
              <ChevronRight className="h-4 w-4" />
            </li>
            <li className={cn(segment.path ? "hover:text-primary" : "font-medium text-foreground")}>
              {segment.path ? (
                <Link to={segment.path}>{segment.label}</Link>
              ) : (
                <span>{segment.label}</span>
              )}
            </li>
          </React.Fragment>
        ))}

        {/* Dynamic Title (Last Segment) - only if static segments exist AND dynamic title is present OR if no static segments but dynamic title is present */}
        {(staticSegments.length > 0 && dynamicTitle) || (staticSegments.length === 0 && dynamicTitle) ? (
          <>
            <li className="flex items-center">
              <ChevronRight className="h-4 w-4" />
            </li>
            <li className="font-medium text-foreground">
              <span>{dynamicTitle}</span>
            </li>
          </>
        ) : null}
      </ol>
    </nav>

      {/* Mode Toggle - only show on home page for users with both consumer and management access */}
      {showModeToggle && (
        <div className="inline-flex items-center gap-1 p-1 bg-muted rounded-lg">
          <Button
            variant={effectiveViewMode === 'consumer' ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setViewMode('consumer')}
            className={cn(
              "gap-2 h-8",
              effectiveViewMode === 'consumer' && "shadow-sm"
            )}
          >
            <Store className="h-4 w-4" />
            {t('marketplace.modeToggle.marketplace')}
          </Button>
          <Button
            variant={effectiveViewMode === 'management' ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setViewMode('management')}
            className={cn(
              "gap-2 h-8",
              effectiveViewMode === 'management' && "shadow-sm"
            )}
          >
            <LayoutDashboard className="h-4 w-4" />
            {t('marketplace.modeToggle.management')}
          </Button>
        </div>
      )}
    </div>
  );
} 