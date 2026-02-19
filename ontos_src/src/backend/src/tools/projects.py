"""
Projects tools for LLM.

Tools for searching, creating, updating, and deleting projects.
"""

from typing import Any, Dict, List, Optional

from src.common.logging import get_logger
from src.tools.base import BaseTool, ToolContext, ToolResult

logger = get_logger(__name__)


class SearchProjectsTool(BaseTool):
    """Search for projects by name or description."""
    
    name = "search_projects"
    category = "organization"
    description = "Search for projects by name, title, or description. Returns matching projects with their team counts."
    parameters = {
        "query": {
            "type": "string",
            "description": "Search query for projects (e.g., 'customer analytics', 'data platform')"
        }
    }
    required_params = ["query"]
    required_scope = "projects:read"
    
    async def execute(
        self,
        ctx: ToolContext,
        query: str
    ) -> ToolResult:
        """Search for projects."""
        logger.info(f"[search_projects] Starting - query='{query}'")
        
        try:
            from src.db_models.projects import ProjectDb
            
            projects_db = ctx.db.query(ProjectDb).limit(500).all()
            logger.debug(f"[search_projects] Found {len(projects_db)} total projects in database")
            
            if not projects_db:
                return ToolResult(
                    success=True,
                    data={"projects": [], "total_found": 0, "message": "No projects found"}
                )
            
            query_lower = query.lower() if query and query != '*' else ''
            filtered = []
            
            for p in projects_db:
                if not query_lower:
                    include = True
                else:
                    name_match = query_lower in (p.name or "").lower()
                    title_match = query_lower in (p.title or "").lower()
                    desc_match = query_lower in (p.description or "").lower()
                    include = name_match or title_match or desc_match
                
                if include:
                    filtered.append({
                        "id": str(p.id),
                        "name": p.name,
                        "title": p.title,
                        "description": p.description,
                        "project_type": p.project_type,
                        "team_count": len(p.teams) if p.teams else 0
                    })
            
            logger.info(f"[search_projects] SUCCESS: Found {len(filtered)} matching projects")
            return ToolResult(
                success=True,
                data={
                    "projects": filtered[:20],
                    "total_found": len(filtered)
                }
            )
            
        except Exception as e:
            logger.error(f"[search_projects] FAILED: {type(e).__name__}: {e}", exc_info=True)
            return ToolResult(success=False, error=f"{type(e).__name__}: {str(e)}", data={"projects": []})


class GetProjectTool(BaseTool):
    """Get a single project by ID."""
    
    name = "get_project"
    category = "organization"
    description = "Get detailed information about a specific project by its ID, including assigned teams."
    parameters = {
        "project_id": {
            "type": "string",
            "description": "The ID of the project to retrieve"
        }
    }
    required_params = ["project_id"]
    required_scope = "projects:read"
    
    async def execute(
        self,
        ctx: ToolContext,
        project_id: str
    ) -> ToolResult:
        """Get a project by ID."""
        logger.info(f"[get_project] Starting - project_id={project_id}")
        
        try:
            from src.controller.projects_manager import projects_manager
            
            project = projects_manager.get_project_by_id(ctx.db, project_id)
            
            if not project:
                return ToolResult(
                    success=False,
                    error=f"Project '{project_id}' not found"
                )
            
            teams = []
            if project.teams:
                for t in project.teams[:10]:  # Limit teams in response
                    teams.append({
                        "id": str(t.id),
                        "name": t.name
                    })
            
            logger.info(f"[get_project] SUCCESS: Found project {project_id}")
            return ToolResult(
                success=True,
                data={
                    "id": str(project.id),
                    "name": project.name,
                    "title": project.title,
                    "description": project.description,
                    "project_type": project.project_type,
                    "owner_team_id": str(project.owner_team_id) if project.owner_team_id else None,
                    "owner_team_name": project.owner_team_name,
                    "team_count": len(project.teams) if project.teams else 0,
                    "teams": teams,
                    "created_at": project.created_at.isoformat() if project.created_at else None
                }
            )
            
        except Exception as e:
            logger.error(f"[get_project] FAILED: {type(e).__name__}: {e}", exc_info=True)
            return ToolResult(success=False, error=f"{type(e).__name__}: {str(e)}")


class CreateProjectTool(BaseTool):
    """Create a new project."""
    
    name = "create_project"
    category = "organization"
    description = "Create a new project. Projects organize work and can have multiple teams assigned."
    parameters = {
        "name": {
            "type": "string",
            "description": "Unique name for the project (e.g., 'customer-analytics-platform')"
        },
        "title": {
            "type": "string",
            "description": "Display title for the project (e.g., 'Customer Analytics Platform')"
        },
        "description": {
            "type": "string",
            "description": "Description of the project's purpose and scope"
        },
        "project_type": {
            "type": "string",
            "description": "Type of project (e.g., 'standard', 'admin')",
            "enum": ["standard", "admin"]
        },
        "owner_team_id": {
            "type": "string",
            "description": "Optional: ID of the team that owns this project"
        },
        "team_ids": {
            "type": "array",
            "description": "Optional: IDs of teams to assign to this project",
            "items": {"type": "string"}
        }
    }
    required_params = ["name", "title"]
    required_scope = "projects:write"
    
    async def execute(
        self,
        ctx: ToolContext,
        name: str,
        title: str,
        description: Optional[str] = None,
        project_type: Optional[str] = None,
        owner_team_id: Optional[str] = None,
        team_ids: Optional[List[str]] = None
    ) -> ToolResult:
        """Create a project."""
        logger.info(f"[create_project] Starting - name='{name}', title='{title}'")
        
        try:
            from src.controller.projects_manager import projects_manager
            from src.models.projects import ProjectCreate
            
            project_create = ProjectCreate(
                name=name,
                title=title,
                description=description or "",
                project_type=project_type or "standard",
                owner_team_id=owner_team_id,
                team_ids=team_ids or []
            )
            
            created = projects_manager.create_project(
                db=ctx.db,
                project_in=project_create,
                current_user_id="llm-assistant"
            )
            
            ctx.db.commit()
            
            logger.info(f"[create_project] SUCCESS: Created project id={created.id}, name={created.name}")
            return ToolResult(
                success=True,
                data={
                    "success": True,
                    "project_id": str(created.id),
                    "name": created.name,
                    "title": created.title,
                    "message": f"Project '{title}' created successfully.",
                    "url": f"/projects/{created.id}"
                }
            )
            
        except Exception as e:
            logger.error(f"[create_project] FAILED: {type(e).__name__}: {e}", exc_info=True)
            return ToolResult(success=False, error=f"{type(e).__name__}: {str(e)}")


class UpdateProjectTool(BaseTool):
    """Update an existing project."""
    
    name = "update_project"
    category = "organization"
    description = "Update an existing project's name, title, description, or owner team."
    parameters = {
        "project_id": {
            "type": "string",
            "description": "ID of the project to update"
        },
        "name": {
            "type": "string",
            "description": "New unique name for the project"
        },
        "title": {
            "type": "string",
            "description": "New display title"
        },
        "description": {
            "type": "string",
            "description": "New description"
        },
        "owner_team_id": {
            "type": "string",
            "description": "New owner team ID"
        }
    }
    required_params = ["project_id"]
    required_scope = "projects:write"
    
    async def execute(
        self,
        ctx: ToolContext,
        project_id: str,
        name: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        owner_team_id: Optional[str] = None
    ) -> ToolResult:
        """Update a project."""
        logger.info(f"[update_project] Starting - project_id={project_id}")
        
        try:
            from src.controller.projects_manager import projects_manager
            from src.models.projects import ProjectUpdate
            
            update_data: Dict[str, Any] = {}
            if name is not None:
                update_data["name"] = name
            if title is not None:
                update_data["title"] = title
            if description is not None:
                update_data["description"] = description
            if owner_team_id is not None:
                update_data["owner_team_id"] = owner_team_id if owner_team_id else None
            
            if not update_data:
                return ToolResult(
                    success=False,
                    error="No fields to update. Provide at least one of: name, title, description, owner_team_id"
                )
            
            project_update = ProjectUpdate(**update_data)
            
            updated = projects_manager.update_project(
                db=ctx.db,
                project_id=project_id,
                project_in=project_update,
                current_user_id="llm-assistant"
            )
            
            if not updated:
                return ToolResult(
                    success=False,
                    error=f"Project '{project_id}' not found"
                )
            
            ctx.db.commit()
            
            logger.info(f"[update_project] SUCCESS: Updated project id={updated.id}")
            return ToolResult(
                success=True,
                data={
                    "success": True,
                    "project_id": str(updated.id),
                    "name": updated.name,
                    "title": updated.title,
                    "message": f"Project '{updated.title}' updated successfully.",
                    "url": f"/projects/{updated.id}"
                }
            )
            
        except Exception as e:
            logger.error(f"[update_project] FAILED: {type(e).__name__}: {e}", exc_info=True)
            return ToolResult(success=False, error=f"{type(e).__name__}: {str(e)}")


class DeleteProjectTool(BaseTool):
    """Delete a project."""
    
    name = "delete_project"
    category = "organization"
    description = "Delete a project by its ID. This will also remove all team assignments for this project."
    parameters = {
        "project_id": {
            "type": "string",
            "description": "The ID of the project to delete"
        }
    }
    required_params = ["project_id"]
    required_scope = "projects:write"
    
    async def execute(
        self,
        ctx: ToolContext,
        project_id: str
    ) -> ToolResult:
        """Delete a project."""
        logger.info(f"[delete_project] Starting - project_id={project_id}")
        
        try:
            from src.controller.projects_manager import projects_manager
            
            deleted = projects_manager.delete_project(ctx.db, project_id)
            
            if not deleted:
                return ToolResult(
                    success=False,
                    error=f"Project '{project_id}' not found or could not be deleted"
                )
            
            ctx.db.commit()
            
            logger.info(f"[delete_project] SUCCESS: Deleted project {project_id}")
            return ToolResult(
                success=True,
                data={
                    "success": True,
                    "message": f"Project '{deleted.name}' deleted successfully",
                    "project_id": project_id
                }
            )
            
        except Exception as e:
            logger.error(f"[delete_project] FAILED: {type(e).__name__}: {e}", exc_info=True)
            return ToolResult(success=False, error=f"{type(e).__name__}: {str(e)}")
