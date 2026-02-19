"""
LLM Search Routes

API endpoints for conversational LLM search functionality.

Note: These routes do not use PermissionChecker, following the pattern of the
existing /api/search endpoint. Access control is handled internally by the
LLM tools which filter results based on user permissions.
"""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, Request, status

from src.common.dependencies import (
    DBSessionDep,
    AuditManagerDep,
    AuditCurrentUserDep,
    CurrentUserDep
)
from src.common.workspace_client import get_obo_workspace_client
from src.models.llm_search import (
    ChatMessageCreate, ChatResponse, ConversationSession,
    SessionSummary, LLMSearchStatus
)
from src.controller.llm_search_manager import LLMSearchManager

from src.common.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/llm-search", tags=["LLM Search"])


# ============================================================================
# Dependency for LLMSearchManager
# ============================================================================

async def get_llm_search_manager(request: Request, db: DBSessionDep) -> LLMSearchManager:
    """Get the LLMSearchManager instance with fresh manager references.
    
    Uses OBO (On-Behalf-Of) workspace client so UC operations run with the
    user's permissions, ensuring proper access control and audit trail.
    """
    from src.common.config import get_settings
    settings = get_settings()
    
    # Always get fresh manager references from app state
    # This ensures we use the properly initialized managers
    data_products_manager = getattr(request.app.state, 'data_products_manager', None)
    data_contracts_manager = getattr(request.app.state, 'data_contracts_manager', None)
    semantic_models_manager = getattr(request.app.state, 'semantic_models_manager', None)
    search_manager = getattr(request.app.state, 'search_manager', None)
    
    # Get OBO workspace client - uses user's token for proper access control
    # Falls back to SP client if OBO token not available (local dev)
    obo_ws_client = get_obo_workspace_client(request, settings)
    
    # Create new instance for each request with current db session
    # (Don't cache because we need fresh db session and OBO client)
    return LLMSearchManager(
        db=db,
        settings=settings,
        data_products_manager=data_products_manager,
        data_contracts_manager=data_contracts_manager,
        semantic_models_manager=semantic_models_manager,
        search_manager=search_manager,
        workspace_client=obo_ws_client
    )


LLMSearchManagerDep = Depends(get_llm_search_manager)


# ============================================================================
# Routes
# ============================================================================

@router.get("/status", response_model=LLMSearchStatus)
async def get_llm_search_status(
    manager: LLMSearchManager = LLMSearchManagerDep
) -> LLMSearchStatus:
    """
    Get the status of LLM search functionality.
    
    Returns whether LLM search is enabled and the configured endpoint.
    """
    return manager.get_status()


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: Request,
    message: ChatMessageCreate,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    audit_manager: AuditManagerDep,
    audit_user: AuditCurrentUserDep,
    manager: LLMSearchManager = LLMSearchManagerDep
) -> ChatResponse:
    """
    Send a chat message and receive the assistant's response.
    
    The assistant can search for data products, glossary terms, costs,
    and execute analytics queries to answer your questions.
    
    Provide a session_id to continue an existing conversation.
    """
    success = False
    details = {
        "params": {
            "session_id": message.session_id,
            "message_length": len(message.content)
        }
    }
    
    try:
        logger.info(f"LLM chat request from user {current_user.email}, session={message.session_id}")
        
        # Note: manager already has OBO workspace client from get_llm_search_manager dependency
        response = await manager.chat(
            user_message=message.content,
            user_id=current_user.email,
            session_id=message.session_id
        )
        
        success = True
        details["session_id"] = response.session_id
        details["tool_calls"] = response.tool_calls_executed
        
        return response
        
    except Exception as e:
        logger.error(f"Error in LLM chat: {e}", exc_info=True)
        details["error"] = str(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chat processing failed: {str(e)}"
        )
    finally:
        audit_manager.log_action(
            db=db,
            username=audit_user.username,
            ip_address=audit_user.ip,
            feature="llm-search",
            action="CHAT",
            success=success,
            details=details
        )


@router.get("/sessions", response_model=List[SessionSummary])
async def list_sessions(
    current_user: CurrentUserDep,
    manager: LLMSearchManager = LLMSearchManagerDep
) -> List[SessionSummary]:
    """
    List conversation sessions for the current user.
    
    Sessions are ordered by last update time (most recent first).
    """
    return manager.list_sessions(current_user.email)


@router.get("/sessions/{session_id}", response_model=ConversationSession)
async def get_session(
    session_id: str,
    current_user: CurrentUserDep,
    manager: LLMSearchManager = LLMSearchManagerDep
) -> ConversationSession:
    """
    Get a specific conversation session with full message history.
    """
    session = manager.get_session(session_id, current_user.email)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )
    return session


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: str,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    audit_manager: AuditManagerDep,
    audit_user: AuditCurrentUserDep,
    manager: LLMSearchManager = LLMSearchManagerDep
):
    """
    Delete a conversation session.
    """
    success = manager.delete_session(session_id, current_user.email)
    
    audit_manager.log_action(
        db=db,
        username=audit_user.username,
        ip_address=audit_user.ip,
        feature="llm-search",
        action="DELETE_SESSION",
        success=success,
        details={"session_id": session_id}
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )


# ============================================================================
# Route Registration
# ============================================================================

def register_routes(app):
    """Register LLM search routes with the FastAPI app."""
    app.include_router(router)

