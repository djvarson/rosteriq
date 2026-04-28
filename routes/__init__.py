"""Routes package."""

from .auth import router as auth_router
from .availability import router as availability_router

try:
    from .webhook_routes import router as webhook_router
except ImportError:
    webhook_router = None

try:
    from .onboarding import router as onboarding_router
except ImportError:
    onboarding_router = None

__all__ = ["auth_router", "availability_router", "webhook_router", "onboarding_router"]
