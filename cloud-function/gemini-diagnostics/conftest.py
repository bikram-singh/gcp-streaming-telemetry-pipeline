import sys
from unittest.mock import MagicMock

# main.py constructs real clients at module load time:
#   gemini_client = genai.Client(vertexai=True, project=..., location=...)
#   bq_client = bigquery.Client(project=...)
# Both would otherwise try to resolve real GCP credentials just to import
# the module, which fails on a bare CI runner or any machine without ADC
# configured. Stubbing the SDK modules before import means `genai.Client(...)`
# and `bigquery.Client(...)` just return MagicMock instances — no network,
# no credentials, and the returned mocks are directly configurable per test
# via `main.gemini_client.models.generate_content.return_value = ...` etc.
sys.modules.setdefault("google.genai", MagicMock())
sys.modules.setdefault("google.cloud.bigquery", MagicMock())
