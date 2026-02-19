"""
Pydantic models for MCP (Model Context Protocol) JSON-RPC communication.

Models for JSON-RPC 2.0 requests and responses used by the MCP server.
"""

from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field


class JSONRPCRequest(BaseModel):
    """JSON-RPC 2.0 request."""
    jsonrpc: str = Field(default="2.0")
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[Union[str, int]] = None


class JSONRPCError(BaseModel):
    """JSON-RPC 2.0 error."""
    code: int
    message: str
    data: Optional[Any] = None


class JSONRPCResponse(BaseModel):
    """JSON-RPC 2.0 response."""
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[JSONRPCError] = None
    id: Optional[Union[str, int]] = None

