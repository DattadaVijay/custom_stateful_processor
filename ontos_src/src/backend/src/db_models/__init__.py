# Makes api/db_models a package.
# Import all model modules so that every table is registered with Base.metadata
# (used by database.py, Alembic, and schema introspection).

from . import app_settings
from . import access_grants
from . import audit_log
from . import change_log
from . import comments
from . import compliance
from . import costs
from . import data_asset_reviews
from . import data_contract_validations
from . import data_contracts
from . import data_domains
from . import data_products
from . import data_quality_checks
from . import datasets
from . import genie_spaces
from . import llm_sessions
from . import mcp_tokens
from . import mdm
from . import metadata as metadata_db
from . import notifications
from . import process_workflows
from . import projects
from . import rdf_triples
from . import semantic_links
from . import semantic_models
from . import settings
from . import tags
from . import teams
from . import workflow_configurations
from . import workflow_installations
from . import workflow_job_runs

