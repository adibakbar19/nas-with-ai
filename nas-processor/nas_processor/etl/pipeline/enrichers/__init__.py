from .base import AddressEnricher
from .noop import NoOpEnricher

__all__ = ["AddressEnricher", "NoOpEnricher"]

# BedrockEnricher is NOT exported here intentionally.
# Import it explicitly only when Bedrock is enabled:
#   from nas_processor.etl.pipeline.enrichers.bedrock import BedrockEnricher
#
# This means removing Bedrock requires zero changes to
# any file that imports from enrichers/.
