"""
SQL Query Validator

Validates SQL queries for safety in the LLM search context.
Only allows read-only SELECT queries with resource limits.
"""

import re
from typing import List, Set, Tuple, Optional
from dataclasses import dataclass

from src.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SQLValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    is_read_only: bool
    tables_referenced: Set[str]
    error_message: Optional[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class SQLValidator:
    """
    Validates SQL queries for safety in LLM-generated query execution.
    
    Security principles:
    1. Only SELECT statements allowed (read-only)
    2. No DDL (CREATE, DROP, ALTER, TRUNCATE)
    3. No DML (INSERT, UPDATE, DELETE, MERGE)
    4. No administrative commands (GRANT, REVOKE, etc.)
    5. No dangerous functions (COPY, EXECUTE, etc.)
    6. Enforce row limits
    7. Extract table references for permission checking
    """
    
    # SQL keywords that indicate write operations
    WRITE_KEYWORDS = {
        # DDL
        'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'RENAME',
        # DML  
        'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'UPSERT',
        # Administrative
        'GRANT', 'REVOKE', 'DENY',
        # Transactions (could be used maliciously)
        'COMMIT', 'ROLLBACK', 'SAVEPOINT',
        # Databricks specific
        'VACUUM', 'OPTIMIZE', 'REFRESH', 'INVALIDATE',
        'MSCK', 'REPAIR', 'RESTORE',
    }
    
    # Dangerous functions/commands
    DANGEROUS_PATTERNS = [
        r'\bEXECUTE\s+IMMEDIATE\b',
        r'\bEXEC\s*\(',
        r'\bCOPY\s+INTO\b',
        r'\bCALL\s+',
        r'\bSET\s+',  # SET commands can change session state
        r'\bUSE\s+',  # USE can change context
        r';\s*\w',    # Multiple statements (SQL injection attempt)
        r'--',        # SQL comments (could hide malicious code)
        r'/\*',       # Block comments
        r'\bINTO\s+',  # SELECT INTO (writes data)
    ]
    
    # Maximum query length
    MAX_QUERY_LENGTH = 10000
    
    # Default row limit
    DEFAULT_ROW_LIMIT = 1000
    
    def __init__(self, max_row_limit: int = 1000):
        self.max_row_limit = max_row_limit
    
    def validate(self, sql: str) -> SQLValidationResult:
        """
        Validate a SQL query for safety.
        
        Args:
            sql: The SQL query to validate
            
        Returns:
            SQLValidationResult with validation status and details
        """
        warnings = []
        
        # Check query length
        if len(sql) > self.MAX_QUERY_LENGTH:
            return SQLValidationResult(
                is_valid=False,
                is_read_only=False,
                tables_referenced=set(),
                error_message=f"Query too long ({len(sql)} chars, max {self.MAX_QUERY_LENGTH})"
            )
        
        # Normalize for analysis
        normalized = self._normalize_sql(sql)
        
        # Check for write keywords
        if not self._is_read_only(normalized):
            detected = self._detect_write_keywords(normalized)
            return SQLValidationResult(
                is_valid=False,
                is_read_only=False,
                tables_referenced=set(),
                error_message=f"Write operations not allowed. Detected: {', '.join(detected)}"
            )
        
        # Check for dangerous patterns
        dangerous = self._detect_dangerous_patterns(sql)
        if dangerous:
            return SQLValidationResult(
                is_valid=False,
                is_read_only=False,
                tables_referenced=set(),
                error_message=f"Potentially dangerous SQL patterns detected: {', '.join(dangerous)}"
            )
        
        # Must start with SELECT, WITH, or DESCRIBE/SHOW (for schema info)
        if not self._starts_with_allowed_keyword(normalized):
            return SQLValidationResult(
                is_valid=False,
                is_read_only=True,
                tables_referenced=set(),
                error_message="Query must start with SELECT, WITH, DESCRIBE, or SHOW"
            )
        
        # Extract table references
        tables = self._extract_table_references(sql)
        
        # Check for LIMIT clause
        if not self._has_limit_clause(normalized):
            warnings.append(f"No LIMIT clause found. Will add LIMIT {self.max_row_limit}")
        
        return SQLValidationResult(
            is_valid=True,
            is_read_only=True,
            tables_referenced=tables,
            warnings=warnings
        )
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for analysis (uppercase, single spaces)."""
        # Remove string literals to avoid false positives
        # Replace 'string' with empty placeholder
        normalized = re.sub(r"'[^']*'", "''", sql)
        normalized = re.sub(r'"[^"]*"', '""', normalized)
        # Uppercase and normalize whitespace
        normalized = ' '.join(normalized.upper().split())
        return normalized
    
    def _is_read_only(self, normalized_sql: str) -> bool:
        """Check if the query is read-only."""
        for keyword in self.WRITE_KEYWORDS:
            # Match keyword as whole word
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, normalized_sql):
                return False
        return True
    
    def _detect_write_keywords(self, normalized_sql: str) -> List[str]:
        """Detect which write keywords are present."""
        detected = []
        for keyword in self.WRITE_KEYWORDS:
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, normalized_sql):
                detected.append(keyword)
        return detected
    
    def _detect_dangerous_patterns(self, sql: str) -> List[str]:
        """Detect dangerous SQL patterns."""
        detected = []
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                detected.append(pattern)
        return detected
    
    def _starts_with_allowed_keyword(self, normalized_sql: str) -> bool:
        """Check if query starts with an allowed keyword."""
        allowed_starts = ['SELECT', 'WITH', 'DESCRIBE', 'SHOW', 'EXPLAIN']
        stripped = normalized_sql.strip()
        for keyword in allowed_starts:
            if stripped.startswith(keyword):
                return True
        return False
    
    def _has_limit_clause(self, normalized_sql: str) -> bool:
        """Check if query has a LIMIT clause."""
        return bool(re.search(r'\bLIMIT\s+\d+', normalized_sql))
    
    def _extract_table_references(self, sql: str) -> Set[str]:
        """
        Extract table references from SQL query.
        
        Handles:
        - FROM table
        - JOIN table
        - FROM catalog.schema.table
        - Table aliases
        """
        tables = set()
        
        # Normalize but preserve case for table names
        normalized = ' '.join(sql.split())
        
        # Pattern for table references: catalog.schema.table or schema.table or table
        # After FROM or JOIN keywords
        table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})'
        
        matches = re.findall(table_pattern, normalized, re.IGNORECASE)
        for match in matches:
            # Clean up and normalize table name
            table_name = match.strip()
            tables.add(table_name)
        
        return tables
    
    def add_row_limit(self, sql: str, max_rows: Optional[int] = None) -> str:
        """
        Add or modify LIMIT clause to enforce row limits.
        
        Args:
            sql: Original SQL query
            max_rows: Maximum rows to return (uses default if not specified)
            
        Returns:
            SQL with LIMIT clause enforced
        """
        limit = max_rows or self.max_row_limit
        normalized = self._normalize_sql(sql)
        
        # Check if LIMIT already exists
        limit_match = re.search(r'\bLIMIT\s+(\d+)', normalized)
        
        if limit_match:
            existing_limit = int(limit_match.group(1))
            if existing_limit > limit:
                # Replace with lower limit
                sql = re.sub(
                    r'\bLIMIT\s+\d+',
                    f'LIMIT {limit}',
                    sql,
                    flags=re.IGNORECASE
                )
                logger.info(f"Reduced LIMIT from {existing_limit} to {limit}")
        else:
            # Add LIMIT clause before any trailing semicolon
            sql = sql.rstrip().rstrip(';')
            sql = f"{sql} LIMIT {limit}"
            logger.info(f"Added LIMIT {limit} to query")
        
        return sql
    
    def sanitize_identifier(self, identifier: str) -> str:
        """
        Sanitize a SQL identifier (table/column name).
        
        Args:
            identifier: The identifier to sanitize
            
        Returns:
            Sanitized identifier safe for use in SQL
        """
        # Only allow alphanumeric, underscore, dot (for qualified names)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){0,2}$', identifier):
            raise ValueError(f"Invalid SQL identifier: {identifier}")
        return identifier


def validate_and_prepare_query(
    sql: str,
    allowed_tables: Optional[Set[str]] = None,
    max_rows: int = 1000
) -> Tuple[bool, str, Optional[str]]:
    """
    Convenience function to validate and prepare a SQL query.
    
    Args:
        sql: The SQL query to validate
        allowed_tables: Optional set of tables the user has access to
        max_rows: Maximum rows to return
        
    Returns:
        Tuple of (is_valid, prepared_sql_or_error, error_message_or_none)
    """
    validator = SQLValidator(max_row_limit=max_rows)
    result = validator.validate(sql)
    
    if not result.is_valid:
        return False, "", result.error_message
    
    # Check table permissions if specified
    if allowed_tables is not None:
        unauthorized = result.tables_referenced - allowed_tables
        if unauthorized:
            return False, "", f"Access denied to tables: {', '.join(unauthorized)}"
    
    # Add/enforce row limit
    prepared_sql = validator.add_row_limit(sql, max_rows)
    
    return True, prepared_sql, None

