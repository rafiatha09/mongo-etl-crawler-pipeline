class MarketIntelligenceETLError(Exception):
    """Base exception for the ETL project."""


class ImproperlyConfigured(MarketIntelligenceETLError):
    """Raised when required settings or dependencies are missing."""
