"""Git service for managing YAML file exports in a Git repository.

This service supports the Indirect delivery mode by persisting governance changes
as YAML files in a Git repository cloned to a Unity Catalog Volume.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import git
import yaml

from .config import get_settings, Settings
from .logging import get_logger

logger = get_logger(__name__)


class GitCloneStatus(str, Enum):
    """Status of the Git repository clone."""
    NOT_CONFIGURED = "not_configured"
    NOT_CLONED = "not_cloned"
    CLONING = "cloning"
    CLONED = "cloned"
    ERROR = "error"


@dataclass
class GitFileChange:
    """Represents a changed file in the repository."""
    path: str
    change_type: str  # 'added', 'modified', 'deleted'
    diff: Optional[str] = None


@dataclass
class GitStatus:
    """Status of the Git repository."""
    clone_status: GitCloneStatus
    repo_url: Optional[str] = None
    branch: Optional[str] = None
    volume_path: Optional[str] = None
    last_sync: Optional[datetime] = None
    pending_changes_count: int = 0
    changed_files: List[GitFileChange] = field(default_factory=list)
    error_message: Optional[str] = None
    current_commit: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "clone_status": self.clone_status.value,
            "repo_url": self.repo_url,
            "branch": self.branch,
            "volume_path": self.volume_path,
            "last_sync": self.last_sync.isoformat() if self.last_sync else None,
            "pending_changes_count": self.pending_changes_count,
            "changed_files": [
                {"path": f.path, "change_type": f.change_type, "diff": f.diff}
                for f in self.changed_files
            ],
            "error_message": self.error_message,
            "current_commit": self.current_commit,
        }


class GitService:
    """Service for managing YAML files in a Git repository.
    
    The repository is cloned to a Unity Catalog Volume for persistence.
    Path structure: {DATABRICKS_VOLUME}/git-export/{repo-contents}
    """

    GIT_EXPORT_SUBDIR = "git-export"

    def __init__(self, settings: Optional[Settings] = None) -> None:
        """Initialize the Git service.
        
        Args:
            settings: Application settings. If None, will be loaded from global settings.
        """
        self._settings = settings or get_settings()
        self._repo: Optional[git.Repo] = None
        self._clone_status = GitCloneStatus.NOT_CONFIGURED
        self._last_sync: Optional[datetime] = None
        self._error_message: Optional[str] = None
        
        # Determine repo directory based on volume configuration
        self._repo_dir: Optional[Path] = None
        self._init_paths()

    def _init_paths(self) -> None:
        """Initialize repository paths based on settings."""
        if not self._settings.GIT_REPO_URL:
            self._clone_status = GitCloneStatus.NOT_CONFIGURED
            logger.info("Git repository URL not configured")
            return
        
        # Use UC Volume path if configured, otherwise fall back to local path
        if self._settings.DATABRICKS_VOLUME:
            # DATABRICKS_VOLUME contains full path like /Volumes/catalog/schema/volume
            self._repo_dir = Path(self._settings.DATABRICKS_VOLUME) / self.GIT_EXPORT_SUBDIR
        else:
            # Fallback for local development
            self._repo_dir = Path("data/git-export")
        
        # Check if already cloned
        if self._repo_dir.exists() and (self._repo_dir / '.git').exists():
            try:
                self._repo = git.Repo(self._repo_dir)
                self._clone_status = GitCloneStatus.CLONED
                logger.info(f"Found existing Git repository at {self._repo_dir}")
            except Exception as e:
                self._clone_status = GitCloneStatus.ERROR
                self._error_message = str(e)
                logger.error(f"Error opening existing Git repository: {e}")
        else:
            self._clone_status = GitCloneStatus.NOT_CLONED
            logger.info(f"Git repository not yet cloned to {self._repo_dir}")

    def reinitialize(self, settings: Optional[Settings] = None) -> None:
        """Reinitialize the Git service with updated settings.
        
        Call this after Git settings have been changed to pick up new configuration.
        
        Args:
            settings: Updated settings. If None, will reload from global settings.
        """
        self._settings = settings or get_settings()
        self._repo = None
        self._clone_status = GitCloneStatus.NOT_CONFIGURED
        self._error_message = None
        self._repo_dir = None
        self._init_paths()
        logger.info(f"Git service reinitialized (status: {self._clone_status.value}, repo_dir: {self._repo_dir})")

    @property
    def is_configured(self) -> bool:
        """Check if Git is configured."""
        return bool(self._settings.GIT_REPO_URL)

    @property
    def is_cloned(self) -> bool:
        """Check if repository is cloned."""
        return self._clone_status == GitCloneStatus.CLONED and self._repo is not None

    @property
    def repo_dir(self) -> Optional[Path]:
        """Get the repository directory path."""
        return self._repo_dir

    def _get_auth_url(self) -> str:
        """Build authenticated URL for Git operations.
        
        For GitHub, if only a token (password) is provided without username,
        we use 'x-access-token' as the username which is the standard for PATs.
        """
        if not self._settings.GIT_REPO_URL:
            raise ValueError("Git repository URL not configured")
        
        url = self._settings.GIT_REPO_URL
        username = self._settings.GIT_USERNAME or ""
        password = self._settings.GIT_PASSWORD or ""
        
        # If we have a password/token but no username, use 'x-access-token' for GitHub
        if password and not username:
            username = "x-access-token"
        
        if username and password:
            # Insert credentials into URL
            if "://" in url:
                protocol, rest = url.split("://", 1)
                url = f"{protocol}://{username}:{password}@{rest}"
        
        return url

    def clone(self) -> GitStatus:
        """Clone the Git repository to the UC Volume.
        
        This is an explicit action triggered by admin.
        
        Returns:
            GitStatus with clone result
        """
        if not self.is_configured:
            return GitStatus(
                clone_status=GitCloneStatus.NOT_CONFIGURED,
                error_message="Git repository URL not configured"
            )
        
        if self._repo_dir is None:
            return GitStatus(
                clone_status=GitCloneStatus.ERROR,
                error_message="Repository directory not configured"
            )
        
        self._clone_status = GitCloneStatus.CLONING
        
        try:
            # Create parent directory if needed
            self._repo_dir.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove existing directory if it exists but isn't a valid repo
            if self._repo_dir.exists():
                import shutil
                shutil.rmtree(self._repo_dir)
            
            auth_url = self._get_auth_url()
            branch = self._settings.GIT_BRANCH or "main"
            
            logger.info(f"Cloning repository to {self._repo_dir}")
            try:
                self._repo = git.Repo.clone_from(
                    auth_url,
                    self._repo_dir,
                    branch=branch
                )
            except git.GitCommandError as clone_error:
                # Handle empty repository (no branches yet)
                if "Remote branch" in str(clone_error) and "not found" in str(clone_error):
                    logger.info(f"Repository appears empty, initializing fresh repo with branch '{branch}'")
                    self._repo_dir.mkdir(parents=True, exist_ok=True)
                    self._repo = git.Repo.init(self._repo_dir)
                    self._repo.create_remote('origin', auth_url)
                    
                    # Create initial README
                    readme_path = self._repo_dir / "README.md"
                    readme_path.write_text("# Ontos State Repository\n\nThis repository stores YAML state files for the Ontos application.\n")
                    self._repo.index.add(["README.md"])
                    self._repo.index.commit("Initial commit from Ontos")
                    
                    # Create/checkout the branch (may already exist from git init), then push
                    try:
                        self._repo.git.checkout('-b', branch)
                    except git.GitCommandError:
                        # Branch already exists (modern git defaults to 'main')
                        self._repo.git.checkout(branch)
                    self._repo.git.push('-u', 'origin', branch)
                    logger.info(f"Initialized and pushed new repository with branch '{branch}'")
                else:
                    raise
            
            self._clone_status = GitCloneStatus.CLONED
            self._last_sync = datetime.utcnow()
            self._error_message = None
            
            logger.info(f"Successfully cloned repository to {self._repo_dir}")
            return self.get_status()
            
        except Exception as e:
            self._clone_status = GitCloneStatus.ERROR
            self._error_message = str(e)
            logger.error(f"Error cloning repository: {e}")
            return GitStatus(
                clone_status=GitCloneStatus.ERROR,
                repo_url=self._settings.GIT_REPO_URL,
                branch=self._settings.GIT_BRANCH,
                volume_path=str(self._repo_dir) if self._repo_dir else None,
                error_message=str(e)
            )

    def pull(self) -> GitStatus:
        """Pull latest changes from remote.
        
        Returns:
            GitStatus with pull result
        """
        if not self.is_cloned:
            return GitStatus(
                clone_status=self._clone_status,
                error_message="Repository not cloned"
            )
        
        try:
            auth_url = self._get_auth_url()
            
            # Update remote URL with auth
            origin = self._repo.remote('origin')
            with origin.config_writer as cw:
                cw.set("url", auth_url)
            
            origin.pull(self._settings.GIT_BRANCH)
            self._last_sync = datetime.utcnow()
            
            logger.info("Successfully pulled latest changes")
            return self.get_status()
            
        except Exception as e:
            self._error_message = str(e)
            logger.error(f"Error pulling changes: {e}")
            return GitStatus(
                clone_status=self._clone_status,
                error_message=str(e)
            )

    def get_status(self, include_diffs: bool = False) -> GitStatus:
        """Get current repository status.
        
        Args:
            include_diffs: Whether to include file diffs in the response
            
        Returns:
            GitStatus with current state
        """
        if not self.is_configured:
            return GitStatus(clone_status=GitCloneStatus.NOT_CONFIGURED)
        
        if not self.is_cloned:
            return GitStatus(
                clone_status=self._clone_status,
                repo_url=self._settings.GIT_REPO_URL,
                branch=self._settings.GIT_BRANCH,
                volume_path=str(self._repo_dir) if self._repo_dir else None,
                error_message=self._error_message
            )
        
        try:
            # Get changed files
            changed_files: List[GitFileChange] = []
            
            # Untracked files
            for item in self._repo.untracked_files:
                diff_text = None
                if include_diffs:
                    try:
                        file_path = self._repo_dir / item
                        if file_path.exists():
                            diff_text = file_path.read_text()
                    except Exception:
                        pass
                changed_files.append(GitFileChange(
                    path=item,
                    change_type="added",
                    diff=diff_text
                ))
            
            # Modified/deleted files
            for item in self._repo.index.diff(None):
                diff_text = None
                if include_diffs:
                    try:
                        diff_text = self._repo.git.diff(item.a_path)
                    except Exception:
                        pass
                
                change_type = "deleted" if item.deleted_file else "modified"
                changed_files.append(GitFileChange(
                    path=item.a_path,
                    change_type=change_type,
                    diff=diff_text
                ))
            
            # Staged but not committed
            for item in self._repo.index.diff("HEAD"):
                if not any(f.path == item.a_path for f in changed_files):
                    diff_text = None
                    if include_diffs:
                        try:
                            diff_text = self._repo.git.diff("HEAD", item.a_path)
                        except Exception:
                            pass
                    
                    change_type = "added" if item.new_file else ("deleted" if item.deleted_file else "modified")
                    changed_files.append(GitFileChange(
                        path=item.a_path,
                        change_type=change_type,
                        diff=diff_text
                    ))
            
            # Get current commit
            current_commit = None
            try:
                current_commit = self._repo.head.commit.hexsha[:8]
            except Exception:
                pass
            
            return GitStatus(
                clone_status=GitCloneStatus.CLONED,
                repo_url=self._settings.GIT_REPO_URL,
                branch=self._settings.GIT_BRANCH,
                volume_path=str(self._repo_dir) if self._repo_dir else None,
                last_sync=self._last_sync,
                pending_changes_count=len(changed_files),
                changed_files=changed_files,
                current_commit=current_commit
            )
            
        except Exception as e:
            logger.error(f"Error getting repository status: {e}")
            return GitStatus(
                clone_status=self._clone_status,
                repo_url=self._settings.GIT_REPO_URL,
                branch=self._settings.GIT_BRANCH,
                volume_path=str(self._repo_dir) if self._repo_dir else None,
                error_message=str(e)
            )

    def commit_and_push(self, commit_message: Optional[str] = None) -> GitStatus:
        """Stage all changes, commit, and push to remote.
        
        Args:
            commit_message: Optional commit message. Auto-generated if not provided.
            
        Returns:
            GitStatus with push result
        """
        if not self.is_cloned:
            return GitStatus(
                clone_status=self._clone_status,
                error_message="Repository not cloned"
            )
        
        try:
            # Stage all changes
            self._repo.git.add(A=True)
            
            # Check if there are changes to commit
            if not self._repo.index.diff("HEAD") and not self._repo.untracked_files:
                return GitStatus(
                    clone_status=GitCloneStatus.CLONED,
                    error_message="No changes to commit"
                )
            
            # Generate commit message if not provided
            if not commit_message:
                commit_message = f"Ontos export at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            
            # Commit
            self._repo.index.commit(commit_message)
            
            # Push
            auth_url = self._get_auth_url()
            origin = self._repo.remote('origin')
            with origin.config_writer as cw:
                cw.set("url", auth_url)
            
            origin.push(self._settings.GIT_BRANCH)
            self._last_sync = datetime.utcnow()
            
            logger.info(f"Successfully committed and pushed changes: {commit_message}")
            return self.get_status()
            
        except Exception as e:
            self._error_message = str(e)
            logger.error(f"Error committing/pushing changes: {e}")
            return GitStatus(
                clone_status=self._clone_status,
                error_message=str(e)
            )

    def save_yaml(
        self,
        filename: str,
        data: Dict[str, Any],
        subdir: Optional[str] = None
    ) -> bool:
        """Save data to a YAML file in the repository (without committing).
        
        Args:
            filename: Name of the YAML file
            data: Dictionary to save as YAML
            subdir: Optional subdirectory within the repo
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_cloned:
            logger.warning("Git repository not cloned")
            return False

        try:
            # Determine file path
            if subdir:
                file_dir = self._repo_dir / subdir
                file_dir.mkdir(parents=True, exist_ok=True)
                file_path = file_dir / filename
            else:
                file_path = self._repo_dir / filename
            
            # Save YAML file
            with open(file_path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

            logger.info(f"Saved YAML file: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving YAML file: {e}")
            return False

    def load_yaml(self, filename: str, subdir: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Load a YAML file from the Git repository.
        
        Args:
            filename: Name of the YAML file
            subdir: Optional subdirectory within the repo
            
        Returns:
            Dictionary containing the YAML data, or None if not found
        """
        if not self.is_cloned:
            logger.warning("Git repository not cloned")
            return None

        try:
            if subdir:
                file_path = self._repo_dir / subdir / filename
            else:
                file_path = self._repo_dir / filename
            
            if not file_path.exists():
                return None

            with open(file_path) as f:
                return yaml.safe_load(f)

        except Exception as e:
            logger.error(f"Error loading YAML file from Git: {e}")
            return None

    def delete_file(self, filename: str, subdir: Optional[str] = None) -> bool:
        """Delete a file from the repository (without committing).
        
        Args:
            filename: Name of the file to delete
            subdir: Optional subdirectory within the repo
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_cloned:
            logger.warning("Git repository not cloned")
            return False

        try:
            if subdir:
                file_path = self._repo_dir / subdir / filename
            else:
                file_path = self._repo_dir / filename
            
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Deleted file: {file_path}")
                return True
            else:
                logger.warning(f"File not found: {file_path}")
                return False

        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            return False

    def list_files(self, pattern: str = "*.yaml", subdir: Optional[str] = None) -> List[str]:
        """List files in the repository matching a pattern.
        
        Args:
            pattern: Glob pattern to match files
            subdir: Optional subdirectory to search in
            
        Returns:
            List of matching file names (relative to subdir if provided)
        """
        if not self.is_cloned:
            logger.warning("Git repository not cloned")
            return []

        try:
            search_dir = self._repo_dir / subdir if subdir else self._repo_dir
            if not search_dir.exists():
                return []
            
            return [
                str(f.relative_to(search_dir))
                for f in search_dir.glob(pattern)
            ]

        except Exception as e:
            logger.error(f"Error listing files in Git repository: {e}")
            return []


# Global Git service instance
_git_service: Optional[GitService] = None


def init_git_service(settings: Optional[Settings] = None) -> GitService:
    """Initialize the global Git service instance.
    
    Args:
        settings: Optional settings to use. If None, uses global settings.
        
    Returns:
        The initialized GitService instance
    """
    global _git_service
    _git_service = GitService(settings)
    return _git_service


def get_git_service() -> GitService:
    """Get the global Git service instance.
    
    Returns:
        Git service
        
    Raises:
        RuntimeError: If Git service is not initialized
    """
    if not _git_service:
        raise RuntimeError("Git service not initialized. Call init_git_service() first.")
    return _git_service


def reinitialize_git_service(settings: Optional[Settings] = None) -> GitService:
    """Reinitialize the global Git service with updated settings.
    
    Call this after Git settings have been changed to pick up new configuration.
    
    Args:
        settings: Updated settings. If None, reloads from global settings.
        
    Returns:
        The reinitialized GitService instance
        
    Raises:
        RuntimeError: If Git service was never initialized
    """
    if not _git_service:
        raise RuntimeError("Git service not initialized. Call init_git_service() first.")
    _git_service.reinitialize(settings)
    return _git_service
