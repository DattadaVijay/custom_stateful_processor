"""
LLM Search Manager

Orchestrates conversational LLM search with tool-calling capabilities.
Uses Claude Sonnet 4.5 via Databricks serving endpoints to answer
business questions about data products, glossary terms, costs, and analytics.
"""

import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from src.common.config import Settings, get_settings
from src.common.logging import get_logger
from src.models.llm_search import (
    ConversationSession, ChatMessage, ChatResponse, MessageRole,
    ToolCall, ToolName, SessionSummary, LLMSearchStatus,
    SearchDataProductsParams, SearchGlossaryTermsParams,
    GetDataProductCostsParams, GetTableSchemaParams, ExecuteAnalyticsQueryParams
)
from src.tools import ToolRegistry, ToolContext, create_default_registry

logger = get_logger(__name__)


# ============================================================================
# System Prompt
# ============================================================================

SYSTEM_PROMPT = """You are Ontos, an intelligent data governance assistant. You help users discover, understand, and analyze data within their organization.

## Your Capabilities

You have access to the following tools:

1. **search_data_products** - Search for data products by name, domain, description, or keywords. Use this to find available datasets.

2. **search_glossary_terms** - Search the knowledge graph for business concepts, terms, and their definitions from loaded ontologies and taxonomies.

3. **get_data_product_costs** - Get cost information for data products, including infrastructure, HR, storage, and other costs.

4. **get_table_schema** - Get the schema (columns and types) of a specific table. Use this before writing analytics queries.

5. **execute_analytics_query** - Execute a read-only SQL SELECT query against Databricks tables. Use this for aggregations, joins, and data analysis.

6. **explore_catalog_schema** - List all tables and views in a Unity Catalog schema with their columns. Use this to understand what data assets exist and suggest semantic models or data products.

7. **create_draft_data_contract** - Create a new draft data contract from schema information. Always create contracts in draft status for user review.

8. **create_draft_data_product** - Create a new draft data product, optionally linked to a contract. Always create products in draft status for user review.

9. **update_data_product** - Update an existing data product's domain, description, or status.

10. **update_data_contract** - Update an existing data contract's domain, description, or status.

11. **add_semantic_link** - Link a data product or contract to a business term/concept from the knowledge graph. Use search_glossary_terms first to find the concept IRI.

12. **list_semantic_links** - List semantic links (business term associations) for a data product or contract.

13. **remove_semantic_link** - Remove a semantic link. Use list_semantic_links first to find the link ID.

14. **search_tags** - Search for existing tags by name, namespace, or description.

15. **create_tag** - Create a new tag. Tags use the format `namespace/tag_name` (e.g., `import/healthcare` creates tag 'healthcare' in namespace 'import'). If no slash is present, the tag goes into the 'default' namespace. Namespaces are auto-created if they don't exist.

16. **assign_tag_to_entity** - Assign an existing tag to a data product, data contract, domain, team, or project. Use search_tags first to find the tag ID.

17. **list_entity_tags** - List all tags assigned to a specific entity.

18. **remove_tag_from_entity** - Remove a tag assignment from an entity.

## Tag Naming Convention

Tags are organized using namespaces with a slash (`/`) separator:
- `namespace/tag_name` format (e.g., `import/healthcare`, `compliance/gdpr`, `pii/sensitive`)
- If no slash is present (e.g., just `pii`), the tag uses the 'default' namespace
- Examples:
  - `import/healthcare` → namespace='import', tag='healthcare'
  - `compliance/gdpr` → namespace='compliance', tag='gdpr'
  - `customer-data` → namespace='default', tag='customer-data'

## Guidelines

- Always search for relevant data products or glossary terms before attempting analytics queries
- When executing analytics queries, first get the table schema to understand available columns
- Use explore_catalog_schema to discover tables in a database before suggesting semantic models
- Explain your reasoning and cite the data sources you used
- If you don't have access to certain data or a query fails, explain why and suggest alternatives
- Format responses with clear sections, tables, and bullet points for readability
- Be concise but thorough - include relevant context without unnecessary verbosity

## Response Format

When presenting data:
- Use markdown tables for tabular results. IMPORTANT: Tables must have proper line breaks between each row:
  ```
  | Column1 | Column2 |
  |---------|---------|
  | value1  | value2  |
  | value3  | value4  |
  ```
  Never put multiple table rows on a single line.
- Use bullet points for lists
- Bold important numbers and findings
- Include units (USD, %, etc.) where applicable

## Limitations

- You can only execute read-only SELECT queries
- Query results are limited to 1000 rows
- You can only access tables the user has permissions for
- Cost data may not be complete for all products
"""


# ============================================================================
# Session Storage - Database-backed with in-memory cache
# ============================================================================

from src.repositories.llm_sessions_repository import llm_sessions_repository, LLMSessionsRepository
from src.db_models.llm_sessions import LLMSessionDb, LLMMessageDb

# Type alias for clarity (Session is SQLAlchemy Session, already imported at top)
SQLSession = Session


class DatabaseSessionStore:
    """
    Database-backed session storage with in-memory write-through cache.
    
    Sessions are persisted to the database but cached in memory for the
    duration of a request to avoid repeated DB queries during tool execution.
    """
    
    def __init__(self, repository: LLMSessionsRepository):
        self._repository = repository
        # In-memory cache for active sessions (within a request)
        self._cache: Dict[str, ConversationSession] = {}
        self.max_sessions_per_user: int = 50
        self.session_ttl_days: int = 30
    
    def _db_to_pydantic(self, db_session: LLMSessionDb) -> ConversationSession:
        """Convert DB session to Pydantic model."""
        messages = []
        for db_msg in db_session.messages:
            tool_calls = None
            if db_msg.tool_calls:
                try:
                    tool_calls_data = json.loads(db_msg.tool_calls)
                    tool_calls = [
                        ToolCall(
                            id=tc['id'],
                            name=ToolName(tc['name']),
                            arguments=tc.get('arguments', {})
                        )
                        for tc in tool_calls_data
                    ]
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    logger.warning(f"Failed to parse tool_calls for message {db_msg.id}: {e}")
            
            messages.append(ChatMessage(
                id=db_msg.id,
                role=MessageRole(db_msg.role),
                content=db_msg.content,
                tool_calls=tool_calls,
                tool_call_id=db_msg.tool_call_id,
                timestamp=db_msg.timestamp
            ))
        
        return ConversationSession(
            id=db_session.id,
            user_id=db_session.user_id,
            title=db_session.title,
            messages=messages,
            created_at=db_session.created_at,
            updated_at=db_session.updated_at
        )
    
    def get(self, db: SQLSession, session_id: str) -> Optional[ConversationSession]:
        """Get a session by ID."""
        # Check cache first
        if session_id in self._cache:
            return self._cache[session_id]
        
        # Load from database
        db_session = self._repository.get_session(db, session_id)
        if db_session:
            session = self._db_to_pydantic(db_session)
            self._cache[session_id] = session
            return session
        return None
    
    def get_for_user(self, db: SQLSession, session_id: str, user_id: str) -> Optional[ConversationSession]:
        """Get a session by ID if owned by user."""
        session = self.get(db, session_id)
        if session and session.user_id == user_id:
            return session
        return None
    
    def create(self, db: SQLSession, user_id: str) -> ConversationSession:
        """Create a new session for a user."""
        db_session = self._repository.create_session(db, user_id)
        session = self._db_to_pydantic(db_session)
        self._cache[session.id] = session
        return session
    
    def delete(self, db: SQLSession, session_id: str, user_id: str) -> bool:
        """Delete a session if owned by user."""
        result = self._repository.delete_session_for_user(db, session_id, user_id)
        if result and session_id in self._cache:
            del self._cache[session_id]
        return result
    
    def list_for_user(self, db: SQLSession, user_id: str) -> List[SessionSummary]:
        """List sessions for a user."""
        db_sessions = self._repository.list_sessions_for_user(db, user_id, limit=self.max_sessions_per_user)
        return [
            SessionSummary(
                id=s.id,
                title=s.title,
                message_count=len(s.messages),
                created_at=s.created_at,
                updated_at=s.updated_at
            )
            for s in db_sessions
        ]
    
    def add_message(
        self,
        db: SQLSession,
        session_id: str,
        role: MessageRole,
        content: Optional[str] = None,
        tool_calls: Optional[List[ToolCall]] = None,
        tool_call_id: Optional[str] = None
    ) -> ChatMessage:
        """Add a message to a session and persist to DB."""
        db_session = self._repository.get_session(db, session_id)
        if not db_session:
            raise ValueError(f"Session {session_id} not found")
        
        # Serialize tool calls for DB
        tool_calls_json = None
        if tool_calls:
            tool_calls_json = [
                {'id': tc.id, 'name': tc.name.value, 'arguments': tc.arguments}
                for tc in tool_calls
            ]
        
        db_message = self._repository.add_message(
            db=db,
            session=db_session,
            role=role.value,
            content=content,
            tool_calls=tool_calls_json,
            tool_call_id=tool_call_id
        )
        
        # Create Pydantic message
        message = ChatMessage(
            id=db_message.id,
            role=role,
            content=content,
            tool_calls=tool_calls,
            tool_call_id=tool_call_id,
            timestamp=db_message.timestamp
        )
        
        # Update cache
        if session_id in self._cache:
            self._cache[session_id].messages.append(message)
            self._cache[session_id].updated_at = db_session.updated_at
            if not self._cache[session_id].title and db_session.title:
                self._cache[session_id].title = db_session.title
        
        return message
    
    def clear_cache(self):
        """Clear the in-memory cache (call at end of request)."""
        self._cache.clear()


# Global singleton session store
_global_session_store = DatabaseSessionStore(llm_sessions_repository)


def get_session_store() -> DatabaseSessionStore:
    """Get the global session store singleton."""
    return _global_session_store


# ============================================================================
# LLM Search Manager
# ============================================================================

class LLMSearchManager:
    """
    Orchestrates conversational LLM search with tool-calling.
    
    Architecture:
    1. User sends message
    2. LLM processes with available tools
    3. If LLM requests tool calls, execute them via ToolRegistry
    4. Feed results back to LLM
    5. Repeat until LLM provides final response
    """
    
    def __init__(
        self,
        db: Session,
        settings: Settings,
        data_products_manager: Optional[Any] = None,
        data_contracts_manager: Optional[Any] = None,
        semantic_models_manager: Optional[Any] = None,
        costs_manager: Optional[Any] = None,
        search_manager: Optional[Any] = None,
        workspace_client: Optional[Any] = None
    ):
        self._db = db
        self._settings = settings
        self._data_products_manager = data_products_manager
        self._data_contracts_manager = data_contracts_manager
        self._semantic_models_manager = semantic_models_manager
        self._costs_manager = costs_manager
        self._search_manager = search_manager
        self._ws_client = workspace_client
        self._session_store = get_session_store()  # Use global singleton
        
        # Initialize tool registry with all default tools
        self._tool_registry = create_default_registry()
        
        logger.info(f"LLMSearchManager initialized (ws_client={workspace_client is not None}, semantic_models_manager={semantic_models_manager is not None}, tools={len(self._tool_registry)})")
    
    def _create_tool_context(self) -> ToolContext:
        """Create a ToolContext with current dependencies."""
        return ToolContext(
            db=self._db,
            settings=self._settings,
            workspace_client=self._ws_client,
            data_products_manager=self._data_products_manager,
            data_contracts_manager=self._data_contracts_manager,
            semantic_models_manager=self._semantic_models_manager,
            costs_manager=self._costs_manager,
            search_manager=self._search_manager
        )
    
    def _get_latest_user_message(self, session: ConversationSession) -> str:
        """Get the latest user message content from a session for query classification."""
        for msg in reversed(session.messages):
            if msg.role == MessageRole.USER and msg.content:
                return msg.content
        return ""
    
    # ========================================================================
    # Public API
    # ========================================================================
    
    def get_status(self) -> LLMSearchStatus:
        """Get the status of LLM search functionality."""
        model = self._settings.LLM_ENDPOINT
        return LLMSearchStatus(
            enabled=self._settings.LLM_ENABLED,
            endpoint=model,
            model_name=model,
            disclaimer=self._settings.LLM_DISCLAIMER_TEXT or (
                "This feature uses AI to analyze data assets. AI-generated content may contain errors. "
                "Review all suggestions carefully before taking action."
            )
        )
    
    def list_sessions(self, user_id: str) -> List[SessionSummary]:
        """List conversation sessions for a user."""
        return self._session_store.list_for_user(self._db, user_id)
    
    def delete_session(self, session_id: str, user_id: str) -> bool:
        """Delete a session if owned by user."""
        return self._session_store.delete(self._db, session_id, user_id)
    
    def get_session(self, session_id: str, user_id: str) -> Optional[ConversationSession]:
        """Get a session by ID if owned by user."""
        return self._session_store.get_for_user(self._db, session_id, user_id)
    
    async def chat(
        self,
        user_message: str,
        user_id: str,
        session_id: Optional[str] = None
    ) -> ChatResponse:
        """
        Process a chat message and return the assistant's response.
        
        Note: The workspace client passed to this manager should already have
        user credentials (OBO) for proper access control and audit trail.
        
        Args:
            user_message: The user's message
            user_id: ID of the user
            session_id: Optional session ID to continue conversation
            
        Returns:
            ChatResponse with the assistant's message
        """
        # Check if LLM is enabled
        if not self._settings.LLM_ENABLED:
            logger.warning("LLM chat requested but LLM_ENABLED is False")
            return ChatResponse(
                session_id="",
                message=ChatMessage(
                    role=MessageRole.ASSISTANT,
                    content="LLM search is not enabled. Please contact your administrator."
                ),
                tool_calls_executed=0,
                sources=[]
            )
        
        # Get or create session
        if session_id:
            session = self._session_store.get_for_user(self._db, session_id, user_id)
            if not session:
                session = self._session_store.create(self._db, user_id)
        else:
            session = self._session_store.create(self._db, user_id)
        
        # Add user message to database (also updates in-memory cache)
        self._session_store.add_message(
            self._db, session.id, MessageRole.USER, content=user_message
        )
        # Re-fetch session from cache to ensure we have updated messages
        session = self._session_store.get(self._db, session.id)
        
        # Process with LLM
        try:
            response_content, tool_calls_executed, sources = await self._process_with_llm(
                session
            )
            
            # Add assistant response to database
            assistant_msg = self._session_store.add_message(
                self._db, session.id, MessageRole.ASSISTANT, content=response_content
            )
            
            return ChatResponse(
                session_id=session.id,
                message=assistant_msg,
                tool_calls_executed=tool_calls_executed,
                sources=sources
            )
            
        except Exception as e:
            logger.error(f"Error processing chat: {e}", exc_info=True)
            
            # Create a user-friendly error message
            error_type = type(e).__name__
            if "ValidationError" in str(e):
                user_message = (
                    "I encountered a validation error while processing a request. "
                    "This typically happens when data doesn't match expected formats. "
                    f"Details: {str(e)[:200]}"
                )
            elif "timeout" in str(e).lower() or "timed out" in str(e).lower():
                user_message = (
                    "The request timed out. This might happen with complex queries. "
                    "Please try a simpler question or try again later."
                )
            elif "connection" in str(e).lower() or "network" in str(e).lower():
                user_message = (
                    "I had trouble connecting to a required service. "
                    "Please try again in a moment."
                )
            else:
                user_message = (
                    f"I encountered an error while processing your request ({error_type}). "
                    "Please try rephrasing your question or try again."
                )
            
            # Try to persist the error message to the session
            try:
                error_msg = self._session_store.add_message(
                    self._db, session.id, MessageRole.ASSISTANT,
                    content=user_message
                )
                return ChatResponse(
                    session_id=session.id,
                    message=error_msg,
                    tool_calls_executed=0,
                    sources=[]
                )
            except Exception as persist_error:
                # If we can't persist the error message (e.g., session was lost),
                # return a synthetic response without persisting
                logger.error(f"Failed to persist error message to session: {persist_error}", exc_info=True)
                synthetic_msg = ChatMessage(
                    id=str(uuid.uuid4()),
                    role=MessageRole.ASSISTANT,
                    content=user_message,
                    timestamp=datetime.utcnow()
                )
                return ChatResponse(
                    session_id=session.id,
                    message=synthetic_msg,
                    tool_calls_executed=0,
                    sources=[]
                )
    
    # ========================================================================
    # LLM Processing
    # ========================================================================
    
    async def _process_with_llm(
        self,
        session: ConversationSession
    ) -> Tuple[str, int, List[Dict[str, Any]]]:
        """
        Process conversation with LLM, handling tool calls.
        
        Returns:
            Tuple of (response_content, tool_calls_count, sources)
        """
        client = self._get_openai_client()
        total_tool_calls = 0
        sources: List[Dict[str, Any]] = []
        max_iterations = 10  # Prevent infinite loops (increased for complex multi-tool queries)
        session_id = session.id  # Save session ID for re-fetching
        
        # Get the latest user message for query classification
        from src.tools.query_classifier import classify_query
        user_query = self._get_latest_user_message(session)
        categories = classify_query(user_query)
        
        # Get filtered tool definitions based on query categories
        tool_definitions = self._tool_registry.get_openai_definitions_filtered(categories)
        logger.info(f"Using {len(tool_definitions)} tools for categories: {categories}")
        
        for iteration in range(max_iterations):
            # Re-fetch session from cache to ensure we have latest messages
            current_session = self._session_store.get(self._db, session_id)
            if not current_session:
                raise RuntimeError(f"Session {session_id} not found in cache")
            
            # Build messages for LLM
            messages = current_session.get_messages_for_llm(SYSTEM_PROMPT)
            
            # Call LLM
            try:
                logger.debug(f"Calling LLM (iteration {iteration + 1}/{max_iterations})")
                response = client.chat.completions.create(
                    model=self._settings.LLM_ENDPOINT,
                    messages=messages,
                    tools=tool_definitions,
                    tool_choice="auto",
                    max_tokens=4096
                )
                logger.debug(f"LLM response received successfully")
            except Exception as llm_error:
                logger.error(f"LLM API call failed: {llm_error}", exc_info=True)
                raise RuntimeError(f"Failed to connect to LLM endpoint: {llm_error}")
            
            assistant_message = response.choices[0].message
            
            # Check if LLM wants to call tools
            if assistant_message.tool_calls:
                # Add assistant message with tool calls to session
                tool_calls = [
                    ToolCall(
                        id=tc.id,
                        name=ToolName(tc.function.name),
                        arguments=json.loads(tc.function.arguments) if tc.function.arguments else {}
                    )
                    for tc in assistant_message.tool_calls
                ]
                # Persist tool call message to database (also updates in-memory cache)
                self._session_store.add_message(
                    self._db, session_id, MessageRole.ASSISTANT,
                    content=None, tool_calls=tool_calls
                )
                
                # Create tool context for this batch of tool calls
                ctx = self._create_tool_context()
                
                # Execute each tool call via the registry
                for tc in assistant_message.tool_calls:
                    total_tool_calls += 1
                    tool_name = tc.function.name
                    tool_args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                    
                    logger.info(f"Executing tool: {tool_name} with args: {tool_args}")
                    
                    # Execute tool via registry
                    result = await self._tool_registry.execute(tool_name, ctx, tool_args)
                    result_dict = result.to_dict()
                    
                    # Log the result summary
                    if not result.success:
                        logger.warning(f"Tool {tool_name} returned error: {result.error}")
                        sources.append({
                            "tool": tool_name,
                            "args": tool_args,
                            "success": False,
                            "error": result.error
                        })
                    else:
                        result_summary = str(result_dict)[:500] + "..." if len(str(result_dict)) > 500 else str(result_dict)
                        logger.info(f"Tool {tool_name} result: {result_summary}")
                        sources.append({
                            "tool": tool_name,
                            "args": tool_args,
                            "success": True
                        })
                    
                    # Persist tool result to database (also updates in-memory cache)
                    self._session_store.add_message(
                        self._db, session_id, MessageRole.TOOL,
                        content=json.dumps(result_dict), tool_call_id=tc.id
                    )
            else:
                # No tool calls - return the response
                return assistant_message.content or "", total_tool_calls, sources
        
        # Max iterations reached
        logger.warning(f"Max LLM iterations ({max_iterations}) reached after {total_tool_calls} tool calls")
        return f"I apologize, but I reached the maximum number of steps ({max_iterations}) while processing your request. I made {total_tool_calls} tool calls. Please try a simpler question or break it into smaller parts.", total_tool_calls, sources
    
    def _get_openai_client(self):
        """Get OpenAI client for Databricks LLM serving endpoint.
        
        Authentication priority:
        1. DATABRICKS_TOKEN from settings/.env (for local development)
        2. Databricks SDK default config (OBO token in Databricks Apps)
        
        Note: We check .env settings first so local development uses the configured
        workspace, not ~/.databrickscfg. In Databricks Apps, DATABRICKS_TOKEN is
        typically not set, so it falls through to SDK config (OBO).
        """
        try:
            from openai import OpenAI
            
            token = None
            
            # First try explicit token from settings/.env (local development)
            token = self._settings.DATABRICKS_TOKEN or os.environ.get('DATABRICKS_TOKEN')
            if token:
                logger.info("Using token from settings/environment (PAT)")
            
            # Fall back to Databricks SDK config (OBO in Apps, ~/.databrickscfg locally)
            if not token:
                try:
                    from databricks.sdk.core import Config
                    config = Config()
                    headers = config.authenticate()
                    if headers and 'Authorization' in headers:
                        auth_header = headers['Authorization']
                        if auth_header.startswith('Bearer '):
                            token = auth_header[7:]
                            logger.info("Using token from Databricks SDK (user credentials)")
                except Exception as sdk_err:
                    logger.debug(f"Could not get token from SDK config: {sdk_err}")
            
            if not token:
                raise RuntimeError("No authentication token available. Ensure the app has access to a serving endpoint or set DATABRICKS_TOKEN.")
            
            # Determine base URL
            base_url = self._settings.LLM_BASE_URL
            if not base_url and self._settings.DATABRICKS_HOST:
                host = self._settings.DATABRICKS_HOST.rstrip('/')
                # Ensure the URL has a protocol
                if not host.startswith('http://') and not host.startswith('https://'):
                    host = f"https://{host}"
                base_url = f"{host}/serving-endpoints"
            
            if not base_url:
                raise RuntimeError("LLM_BASE_URL not configured. Set LLM_BASE_URL or DATABRICKS_HOST.")
            
            logger.info(f"Creating OpenAI client for base_url={base_url}, endpoint={self._settings.LLM_ENDPOINT}")
            return OpenAI(api_key=token, base_url=base_url)
            
        except Exception as e:
            logger.error(f"Failed to create OpenAI client: {e}", exc_info=True)
            raise RuntimeError(f"LLM connection failed: {e}")
