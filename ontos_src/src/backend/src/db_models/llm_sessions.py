"""
LLM Session Database Models

Stores conversational AI sessions and message history for persistence across
server restarts and multi-instance deployments.
"""

from sqlalchemy import Column, String, DateTime, Text, Integer, ForeignKey, func, Index
from sqlalchemy.orm import relationship
from uuid import uuid4

from src.common.database import Base


class LLMSessionDb(Base):
    """Stores LLM conversation sessions."""
    __tablename__ = 'llm_sessions'

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String, nullable=False, index=True)
    title = Column(String, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    messages = relationship(
        "LLMMessageDb",
        back_populates="session",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="LLMMessageDb.sequence"
    )

    # Indexes for common queries
    __table_args__ = (
        Index('ix_llm_sessions_user_updated', 'user_id', 'updated_at'),
    )

    def __repr__(self):
        return f"<LLMSessionDb(id='{self.id}', user_id='{self.user_id}', title='{self.title}')>"


class LLMMessageDb(Base):
    """Stores individual messages within an LLM session."""
    __tablename__ = 'llm_messages'

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id = Column(String, ForeignKey('llm_sessions.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Message content
    role = Column(String, nullable=False)  # 'user', 'assistant', 'tool'
    content = Column(Text, nullable=True)  # Can be null for tool call messages
    
    # Tool-related fields (stored as JSON strings)
    tool_calls = Column(Text, nullable=True)  # JSON array of tool calls
    tool_call_id = Column(String, nullable=True)  # For tool response messages
    
    # Ordering
    sequence = Column(Integer, nullable=False)  # Order within session
    
    # Timestamp
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    session = relationship("LLMSessionDb", back_populates="messages")

    # Indexes
    __table_args__ = (
        Index('ix_llm_messages_session_sequence', 'session_id', 'sequence'),
    )

    def __repr__(self):
        return f"<LLMMessageDb(id='{self.id}', role='{self.role}', sequence={self.sequence})>"

