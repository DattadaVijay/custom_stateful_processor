import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Separator } from '@/components/ui/separator';
import { Settings as SettingsIcon, ShieldX, Loader2, Save } from 'lucide-react';
import RolesSettings from '@/components/settings/roles-settings';
import SemanticModelsSettings from '@/components/settings/semantic-models-settings';
import TagsSettings from '@/components/settings/tags-settings';
import JobsSettings from '@/components/settings/jobs-settings';
import SearchConfigEditor from '@/components/settings/search-config-editor';
import MCPTokensSettings from '@/components/settings/mcp-tokens-settings';
import GitSettings from '@/components/settings/git-settings';
import UICustomizationSettings from '@/components/settings/ui-customization-settings';
import { usePermissions } from '@/stores/permissions-store';
import { FeatureAccessLevel } from '@/types/settings';
import { useToast } from '@/hooks/use-toast';
import useBreadcrumbStore from '@/stores/breadcrumb-store';

interface AppSettings {
  id: string;
  name: string;
  value: any;
  enableBackgroundJobs: boolean;
  workspaceDeploymentPath: string;
  // Unity Catalog settings
  databricksCatalog: string;
  databricksSchema: string;
  databricksVolume: string;
  // Audit log
  appAuditLogDir: string;
  // LLM settings
  llmEnabled: boolean;
  llmEndpoint: string;
  llmSystemPrompt: string;
  llmDisclaimerText: string;
  // Legacy settings (kept for existing tabs)
  databricksHost: string;
  databricksToken: string;
  databricksWarehouseId: string;
  gitRepoUrl: string;
  gitBranch: string;
  gitToken: string;
  // Delivery mode settings
  deliveryModeDirect: boolean;
  deliveryModeIndirect: boolean;
  deliveryModeManual: boolean;
  deliveryDirectDryRun: boolean;
}

export default function Settings() {
  const { t } = useTranslation(['settings', 'common']);
  const { isLoading: permissionsLoading, hasPermission } = usePermissions();
  const { toast } = useToast();
  const { tab: urlTab } = useParams<{ tab?: string }>();
  const navigate = useNavigate();
  
  const setStaticSegments = useBreadcrumbStore((state) => state.setStaticSegments);
  const setDynamicTitle = useBreadcrumbStore((state) => state.setDynamicTitle);
  
  // Determine the active tab from URL param or default to 'general'
  // Note: Workflows tab moved to Compliance view
  const validTabs = ['general', 'git', 'delivery', 'jobs', 'roles', 'tags', 'semantic-models', 'search', 'mcp-tokens', 'ui-customization'];
  const activeTab = urlTab && validTabs.includes(urlTab) ? urlTab : 'general';
  
  // Tab display names for breadcrumbs
  const tabNames: Record<string, string> = {
    'general': t('settings:tabs.general', 'General'),
    'git': t('settings:tabs.git', 'Git'),
    'delivery': t('settings:tabs.delivery', 'Delivery'),
    'jobs': t('settings:tabs.jobs', 'Jobs'),
    'roles': t('settings:tabs.roles', 'Roles'),
    'tags': t('settings:tabs.tags', 'Tags'),
    'semantic-models': t('settings:tabs.semanticModels', 'Semantic Models'),
    'search': t('settings:tabs.search', 'Search'),
    'mcp-tokens': t('settings:tabs.mcpTokens', 'MCP Tokens'),
    'ui-customization': t('settings:tabs.uiCustomization', 'UI Customization'),
  };

  // Check if user has at least READ_ONLY access to settings
  const hasSettingsAccess = hasPermission('settings', FeatureAccessLevel.READ_ONLY);
  const hasWriteAccess = hasPermission('settings', FeatureAccessLevel.READ_WRITE);
  
  // General/databricks/git settings state
  const [settings, setSettings] = useState<AppSettings>({
    id: '',
    name: '',
    value: null,
    enableBackgroundJobs: false,
    workspaceDeploymentPath: '',
    // Unity Catalog settings
    databricksCatalog: '',
    databricksSchema: '',
    databricksVolume: '',
    // Audit log
    appAuditLogDir: '',
    // LLM settings
    llmEnabled: false,
    llmEndpoint: '',
    llmSystemPrompt: '',
    llmDisclaimerText: '',
    // Legacy settings
    databricksHost: '',
    databricksToken: '',
    databricksWarehouseId: '',
    gitRepoUrl: '',
    gitBranch: '',
    gitToken: '',
    // Delivery mode settings
    deliveryModeDirect: false,
    deliveryModeIndirect: false,
    deliveryModeManual: true,
    deliveryDirectDryRun: false,
  });
  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);

  // Set up breadcrumbs
  useEffect(() => {
    setStaticSegments([
      { label: t('settings:title', 'Settings'), path: '/settings' },
    ]);
    setDynamicTitle(tabNames[activeTab] || 'General');
    
    return () => {
      setStaticSegments([]);
      setDynamicTitle(null);
    };
  }, [activeTab, setStaticSegments, setDynamicTitle, t]);

  // Fetch current settings on mount
  useEffect(() => {
    const fetchSettings = async () => {
      setIsLoading(true);
      try {
        const response = await fetch('/api/settings');
        if (response.ok) {
          const data = await response.json();
          setSettings(prev => ({
            ...prev,
            workspaceDeploymentPath: data.workspace_deployment_path || '',
            databricksCatalog: data.databricks_catalog || '',
            databricksSchema: data.databricks_schema || '',
            databricksVolume: data.databricks_volume || '',
            appAuditLogDir: data.app_audit_log_dir || '',
            llmEnabled: data.llm_enabled || false,
            llmEndpoint: data.llm_endpoint || '',
            llmSystemPrompt: data.llm_system_prompt || '',
            llmDisclaimerText: data.llm_disclaimer_text || '',
            // Delivery mode settings
            deliveryModeDirect: data.delivery_mode_direct || false,
            deliveryModeIndirect: data.delivery_mode_indirect || false,
            deliveryModeManual: data.delivery_mode_manual ?? true,
            deliveryDirectDryRun: data.delivery_direct_dry_run || false,
            // Git settings
            gitRepoUrl: data.git_repo_url || '',
            gitBranch: data.git_branch || 'main',
          }));
        }
      } catch (error) {
        console.error('Failed to fetch settings:', error);
      } finally {
        setIsLoading(false);
      }
    };
    fetchSettings();
  }, []);

  const handleSaveGeneralSettings = async () => {
    setIsSaving(true);
    try {
      const response = await fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          workspace_deployment_path: settings.workspaceDeploymentPath,
          databricks_catalog: settings.databricksCatalog,
          databricks_schema: settings.databricksSchema,
          databricks_volume: settings.databricksVolume,
          app_audit_log_dir: settings.appAuditLogDir,
          llm_enabled: settings.llmEnabled,
          llm_endpoint: settings.llmEndpoint,
          llm_system_prompt: settings.llmSystemPrompt,
          llm_disclaimer_text: settings.llmDisclaimerText,
          // Delivery mode settings
          delivery_mode_direct: settings.deliveryModeDirect,
          delivery_mode_indirect: settings.deliveryModeIndirect,
          delivery_mode_manual: settings.deliveryModeManual,
          delivery_direct_dry_run: settings.deliveryDirectDryRun,
          // Git settings
          git_repo_url: settings.gitRepoUrl,
          git_branch: settings.gitBranch,
          git_username: '', // Placeholder - handled separately for security
          git_password: settings.gitToken, // Token is used as password
        }),
      });
      if (response.ok) {
        toast({
          title: t('settings:general.messages.saveSuccess', 'Settings saved successfully'),
        });
      } else {
        throw new Error('Failed to save settings');
      }
    } catch (error) {
      toast({
        title: t('settings:general.messages.saveError', 'Failed to save settings'),
        variant: 'destructive',
      });
    } finally {
      setIsSaving(false);
    }
  };

  const handleLlmEnabledChange = (checked: boolean) => {
    if (!settings) return;
    setSettings({ ...settings, llmEnabled: checked });
  };

  // Delivery mode handlers
  const handleDeliveryModeDirectChange = (checked: boolean) => {
    if (!settings) return;
    setSettings({ ...settings, deliveryModeDirect: checked });
  };

  const handleDeliveryModeIndirectChange = (checked: boolean) => {
    if (!settings) return;
    setSettings({ ...settings, deliveryModeIndirect: checked });
  };

  const handleDeliveryModeManualChange = (checked: boolean) => {
    if (!settings) return;
    setSettings({ ...settings, deliveryModeManual: checked });
  };

  const handleDeliveryDirectDryRunChange = (checked: boolean) => {
    if (!settings) return;
    setSettings({ ...settings, deliveryDirectDryRun: checked });
  };
  
  // Show loading while permissions are being fetched
  if (permissionsLoading) {
    return (
      <div className="py-6 flex items-center justify-center min-h-[400px]">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }
  
  // Show access denied if user doesn't have settings access
  if (!hasSettingsAccess) {
    return (
      <div className="py-6">
        <div className="max-w-2xl mx-auto">
          <Alert variant="destructive" className="border-2">
            <ShieldX className="h-5 w-5" />
            <AlertTitle className="text-lg font-semibold">
              {t('settings:accessDenied.title', 'Access Denied')}
            </AlertTitle>
            <AlertDescription className="mt-2">
              <p className="mb-4">
                {t('settings:accessDenied.message', 'You do not have permission to access the Settings page. This page is restricted to users with administrative privileges.')}
              </p>
              <p className="text-sm">
                {t('settings:accessDenied.action', 'If you believe you should have access, please contact your administrator or ')}
                <Link to="/" className="font-semibold underline hover:text-destructive-foreground">
                  {t('settings:accessDenied.returnHome', 'return to the home page')}
                </Link>
                {t('settings:accessDenied.requestRole', ' to request an appropriate role.')}
              </p>
            </AlertDescription>
          </Alert>
        </div>
      </div>
    );
  }

  // Jobs save is handled within JobsSettings; keep no-op here to preserve structure

  // No job toggling here (moved into JobsSettings)

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    if (!settings) return;
    const { name, value } = e.target;
    setSettings({ ...settings, [name]: value });
  };

  const handleSwitchChange = (checked: boolean) => {
    if (!settings) return;
    setSettings({ ...settings, enableBackgroundJobs: checked });
  };

  const handleTabChange = (value: string) => {
    navigate(`/settings/${value}`);
  };

  return (
    <div className="py-6">
      <h1 className="text-3xl font-bold mb-6 flex items-center gap-2">
        <SettingsIcon className="w-8 h-8" /> {t('settings:title')}
      </h1>

      <Tabs value={activeTab} onValueChange={handleTabChange} className="space-y-4">
        <TabsList>
          <TabsTrigger value="general">{t('settings:tabs.general')}</TabsTrigger>
          <TabsTrigger value="git">{t('settings:tabs.git')}</TabsTrigger>
          <TabsTrigger value="delivery">{t('settings:tabs.delivery', 'Delivery')}</TabsTrigger>
          <TabsTrigger value="jobs">{t('settings:tabs.jobs')}</TabsTrigger>
          <TabsTrigger value="roles">{t('settings:tabs.roles')}</TabsTrigger>
          <TabsTrigger value="tags">{t('settings:tabs.tags')}</TabsTrigger>
          <TabsTrigger value="semantic-models">{t('settings:tabs.semanticModels')}</TabsTrigger>
          <TabsTrigger value="search">{t('settings:tabs.search', 'Search')}</TabsTrigger>
          <TabsTrigger value="mcp-tokens">{t('settings:tabs.mcpTokens', 'MCP Tokens')}</TabsTrigger>
          <TabsTrigger value="ui-customization">{t('settings:tabs.uiCustomization', 'UI Customization')}</TabsTrigger>
        </TabsList>

        <TabsContent value="general">
          <Card>
            <CardHeader>
              <CardTitle>{t('settings:general.title')}</CardTitle>
              <CardDescription>{t('settings:general.description')}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Jobs Settings */}
              <div className="flex items-center space-x-2">
                <Switch
                  id="background-jobs"
                  checked={settings.enableBackgroundJobs}
                  onCheckedChange={handleSwitchChange}
                />
                <Label htmlFor="background-jobs">{t('settings:general.enableBackgroundJobs')}</Label>
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="workspaceDeploymentPath">
                  {t('settings:general.workspaceDeploymentPath.label', 'Workspace Deployment Path')}
                </Label>
                <Input
                  id="workspaceDeploymentPath"
                  name="workspaceDeploymentPath"
                  value={settings.workspaceDeploymentPath}
                  onChange={handleChange}
                  placeholder={t('settings:general.workspaceDeploymentPath.placeholder', '/Workspace/Users/user@domain.com/ontos-workflows')}
                  disabled={!hasWriteAccess || isLoading}
                />
                <p className="text-sm text-muted-foreground">
                  {t('settings:general.workspaceDeploymentPath.help', 'Path in Databricks workspace where workflow files are deployed for background jobs.')}
                </p>
              </div>

              <Separator />

              {/* Unity Catalog Settings */}
              <div>
                <h3 className="text-lg font-medium mb-3">{t('settings:general.unityCatalog.title', 'Unity Catalog')}</h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="databricksCatalog">
                      {t('settings:general.unityCatalog.catalog.label', 'Catalog')}
                    </Label>
                    <Input
                      id="databricksCatalog"
                      name="databricksCatalog"
                      value={settings.databricksCatalog}
                      onChange={handleChange}
                      placeholder={t('settings:general.unityCatalog.catalog.placeholder', 'app_data')}
                      disabled={!hasWriteAccess || isLoading}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="databricksSchema">
                      {t('settings:general.unityCatalog.schema.label', 'Schema')}
                    </Label>
                    <Input
                      id="databricksSchema"
                      name="databricksSchema"
                      value={settings.databricksSchema}
                      onChange={handleChange}
                      placeholder={t('settings:general.unityCatalog.schema.placeholder', 'app_ontos')}
                      disabled={!hasWriteAccess || isLoading}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="databricksVolume">
                      {t('settings:general.unityCatalog.volume.label', 'Volume')}
                    </Label>
                    <Input
                      id="databricksVolume"
                      name="databricksVolume"
                      value={settings.databricksVolume}
                      onChange={handleChange}
                      placeholder={t('settings:general.unityCatalog.volume.placeholder', 'app_files')}
                      disabled={!hasWriteAccess || isLoading}
                    />
                  </div>
                </div>
                <p className="text-sm text-muted-foreground mt-2">
                  {t('settings:general.unityCatalog.help', 'Unity Catalog location for storing application data.')}
                </p>
              </div>

              <Separator />

              {/* Audit Log Settings */}
              <div className="space-y-2">
                <Label htmlFor="appAuditLogDir">
                  {t('settings:general.auditLog.label', 'Audit Log Directory')}
                </Label>
                <Input
                  id="appAuditLogDir"
                  name="appAuditLogDir"
                  value={settings.appAuditLogDir}
                  onChange={handleChange}
                  placeholder={t('settings:general.auditLog.placeholder', 'audit_logs')}
                  disabled={!hasWriteAccess || isLoading}
                />
                <p className="text-sm text-muted-foreground">
                  {t('settings:general.auditLog.help', 'Directory where audit log files are stored.')}
                </p>
              </div>

              <Separator />

              {/* LLM Settings */}
              <div>
                <h3 className="text-lg font-medium mb-3">{t('settings:general.llm.title', 'AI / LLM Configuration')}</h3>
                
                <div className="space-y-4">
                  <div className="flex items-center space-x-2">
                    <Switch
                      id="llmEnabled"
                      checked={settings.llmEnabled}
                      onCheckedChange={handleLlmEnabledChange}
                      disabled={!hasWriteAccess || isLoading}
                    />
                    <Label htmlFor="llmEnabled">{t('settings:general.llm.enabled.label', 'Enable AI Features')}</Label>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="llmEndpoint">
                      {t('settings:general.llm.endpoint.label', 'LLM Endpoint')}
                    </Label>
                    <Input
                      id="llmEndpoint"
                      name="llmEndpoint"
                      value={settings.llmEndpoint}
                      onChange={handleChange}
                      placeholder={t('settings:general.llm.endpoint.placeholder', 'databricks-claude-sonnet-4-5')}
                      disabled={!hasWriteAccess || isLoading || !settings.llmEnabled}
                    />
                    <p className="text-sm text-muted-foreground">
                      {t('settings:general.llm.endpoint.help', 'Databricks serving endpoint name for the LLM.')}
                    </p>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="llmSystemPrompt">
                      {t('settings:general.llm.systemPrompt.label', 'System Prompt')}
                    </Label>
                    <Textarea
                      id="llmSystemPrompt"
                      name="llmSystemPrompt"
                      value={settings.llmSystemPrompt}
                      onChange={handleChange}
                      placeholder={t('settings:general.llm.systemPrompt.placeholder', 'You are a Data Steward...')}
                      disabled={!hasWriteAccess || isLoading || !settings.llmEnabled}
                      rows={4}
                    />
                    <p className="text-sm text-muted-foreground">
                      {t('settings:general.llm.systemPrompt.help', 'System prompt that defines the AI assistant behavior.')}
                    </p>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="llmDisclaimerText">
                      {t('settings:general.llm.disclaimer.label', 'Disclaimer Text')}
                    </Label>
                    <Textarea
                      id="llmDisclaimerText"
                      name="llmDisclaimerText"
                      value={settings.llmDisclaimerText}
                      onChange={handleChange}
                      placeholder={t('settings:general.llm.disclaimer.placeholder', 'This feature uses AI to analyze data assets...')}
                      disabled={!hasWriteAccess || isLoading || !settings.llmEnabled}
                      rows={3}
                    />
                    <p className="text-sm text-muted-foreground">
                      {t('settings:general.llm.disclaimer.help', 'Disclaimer shown to users when using AI features.')}
                    </p>
                  </div>
                </div>
              </div>
            </CardContent>
            {hasWriteAccess && (
              <CardFooter>
                <Button onClick={handleSaveGeneralSettings} disabled={isSaving}>
                  {isSaving ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <Save className="mr-2 h-4 w-4" />
                  )}
                  {t('settings:general.saveButton', 'Save Settings')}
                </Button>
              </CardFooter>
            )}
          </Card>
        </TabsContent>

        <TabsContent value="git">
          <GitSettings />
        </TabsContent>

        <TabsContent value="delivery">
          <Card>
            <CardHeader>
              <CardTitle>{t('settings:delivery.title', 'Delivery Modes')}</CardTitle>
              <CardDescription>
                {t('settings:delivery.description', 'Configure how governance changes are propagated to external systems. Multiple modes can be active simultaneously.')}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Direct Mode */}
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div className="space-y-0.5">
                    <Label htmlFor="delivery-direct" className="text-base font-medium">
                      {t('settings:delivery.direct.label', 'Direct Mode')}
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      {t('settings:delivery.direct.description', 'Apply changes directly to Unity Catalog via SDK (GRANTs, tag assignments).')}
                    </p>
                  </div>
                  <Switch
                    id="delivery-direct"
                    checked={settings.deliveryModeDirect}
                    onCheckedChange={handleDeliveryModeDirectChange}
                    disabled={!hasWriteAccess || isLoading}
                  />
                </div>
                
                {settings.deliveryModeDirect && (
                  <div className="ml-6 flex items-center space-x-2">
                    <Switch
                      id="delivery-direct-dry-run"
                      checked={settings.deliveryDirectDryRun}
                      onCheckedChange={handleDeliveryDirectDryRunChange}
                      disabled={!hasWriteAccess || isLoading}
                    />
                    <Label htmlFor="delivery-direct-dry-run" className="text-sm">
                      {t('settings:delivery.direct.dryRun', 'Dry-run mode (log changes without applying)')}
                    </Label>
                  </div>
                )}
              </div>

              <Separator />

              {/* Indirect Mode */}
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div className="space-y-0.5">
                    <Label htmlFor="delivery-indirect" className="text-base font-medium">
                      {t('settings:delivery.indirect.label', 'Indirect Mode')}
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      {t('settings:delivery.indirect.description', 'Persist changes as YAML files in a Git repository for CI/CD integration.')}
                    </p>
                  </div>
                  <Switch
                    id="delivery-indirect"
                    checked={settings.deliveryModeIndirect}
                    onCheckedChange={handleDeliveryModeIndirectChange}
                    disabled={!hasWriteAccess || isLoading}
                  />
                </div>
                
                {settings.deliveryModeIndirect && (
                  <div className="ml-6 p-4 bg-muted/50 rounded-lg space-y-3">
                    <p className="text-sm text-muted-foreground">
                      {t('settings:delivery.indirect.gitInfo', 'Configure Git repository in the Git tab. Changes will be exported to the configured UC Volume under /git-export/.')}
                    </p>
                  </div>
                )}
              </div>

              <Separator />

              {/* Manual Mode */}
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div className="space-y-0.5">
                    <Label htmlFor="delivery-manual" className="text-base font-medium">
                      {t('settings:delivery.manual.label', 'Manual Mode')}
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      {t('settings:delivery.manual.description', 'Generate notifications for admins to apply changes manually in external systems.')}
                    </p>
                  </div>
                  <Switch
                    id="delivery-manual"
                    checked={settings.deliveryModeManual}
                    onCheckedChange={handleDeliveryModeManualChange}
                    disabled={!hasWriteAccess || isLoading}
                  />
                </div>
              </div>
            </CardContent>
            {hasWriteAccess && (
              <CardFooter>
                <Button onClick={handleSaveGeneralSettings} disabled={isSaving}>
                  {isSaving ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <Save className="mr-2 h-4 w-4" />
                  )}
                  {t('settings:delivery.saveButton', 'Save Delivery Settings')}
                </Button>
              </CardFooter>
            )}
          </Card>
        </TabsContent>

        <TabsContent value="jobs">
          <JobsSettings />
        </TabsContent>

        <TabsContent value="roles">
            <RolesSettings />
        </TabsContent>
        <TabsContent value="tags">
            <TagsSettings />
        </TabsContent>
        <TabsContent value="semantic-models">
            <SemanticModelsSettings />
        </TabsContent>
        <TabsContent value="search">
            <SearchConfigEditor />
        </TabsContent>
        <TabsContent value="mcp-tokens">
            <MCPTokensSettings />
        </TabsContent>
        <TabsContent value="ui-customization">
            <UICustomizationSettings />
        </TabsContent>
      </Tabs>

      <div className="mt-6" />
    </div>
  );
} 