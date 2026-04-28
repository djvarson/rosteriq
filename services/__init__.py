"""Services package."""
try:
    from .config import AppConfig, get_config, init_config, reset_config, Environment
except ImportError:
    AppConfig = None
    get_config = None
    init_config = None
    reset_config = None
    Environment = None

try:
    from .auth import auth_service
except ImportError:
    auth_service = None

try:
    from .tanda_webhooks import (
        TandaEventType,
        WebhookPayload,
        EventHandlerRegistry,
        get_registry,
    )
except ImportError:
    TandaEventType = None
    WebhookPayload = None
    EventHandlerRegistry = None
    get_registry = None

try:
    from .tanda_webhook_manager import (
        WebhookSubscription,
        WebhookManager,
        init_webhook_manager,
        get_webhook_manager,
    )
except ImportError:
    WebhookSubscription = None
    WebhookManager = None
    init_webhook_manager = None
    get_webhook_manager = None

try:
    from .tanda_roster_push import (
        TandaRosterPush,
        PushResult,
        RosterDiff,
        ShiftMapping,
    )
except ImportError:
    TandaRosterPush = None
    PushResult = None
    RosterDiff = None
    ShiftMapping = None

try:
    from .http_client import (
        ResilientHttpClient,
        get_http_client,
        get_tanda_client,
        get_stripe_client,
        get_xero_client,
        get_weather_client,
    )
except ImportError:
    ResilientHttpClient = None
    get_http_client = None
    get_tanda_client = None
    get_stripe_client = None
    get_xero_client = None
    get_weather_client = None

__all__ = [
    "AppConfig",
    "get_config",
    "init_config",
    "reset_config",
    "Environment",
    "auth_service",
    "TandaEventType",
    "WebhookPayload",
    "EventHandlerRegistry",
    "get_registry",
    "WebhookSubscription",
    "WebhookManager",
    "init_webhook_manager",
    "get_webhook_manager",
    "TandaRosterPush",
    "PushResult",
    "RosterDiff",
    "ShiftMapping",
    "ResilientHttpClient",
    "get_http_client",
    "get_tanda_client",
    "get_stripe_client",
    "get_xero_client",
    "get_weather_client",
]
