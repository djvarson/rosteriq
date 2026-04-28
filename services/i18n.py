"""
Simple key-value translation system for RosterIQ.

Provides lightweight i18n without heavy frameworks. Supports
variable substitution in translation strings.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# Default locale for Australian hospitality context
DEFAULT_LOCALE = "en-AU"

# Supported locales for future expansion
SUPPORTED_LOCALES = [
    "en-AU",  # Australian English
    "en-US",  # US English
    # "zh-Hans",  # Simplified Chinese (future)
    # "vi",       # Vietnamese (future)
    # "ko",       # Korean (future)
]


class I18n:
    """Simple translation system for RosterIQ."""

    def __init__(self, locales_dir: Optional[str] = None):
        """
        Initialize i18n with translation files.

        Args:
            locales_dir: Directory containing locale JSON files.
                        Defaults to ./locales relative to this file.
        """
        self._current_locale = DEFAULT_LOCALE
        self._translations: Dict[str, Dict[str, Any]] = {}
        self._locales_dir = locales_dir

        if not locales_dir:
            # Default to ./locales next to this file
            module_dir = Path(__file__).parent.parent
            self._locales_dir = str(module_dir / "locales")

        self._load_all_locales()

    def _load_all_locales(self) -> None:
        """Load all supported locale files from disk."""
        locales_path = Path(self._locales_dir)

        if not locales_path.exists():
            logger.warning(
                f"Locales directory not found: {self._locales_dir}. "
                "Create it with locale JSON files for i18n."
            )
            return

        for locale in SUPPORTED_LOCALES:
            locale_file = locales_path / f"{locale}.json"
            if locale_file.exists():
                try:
                    with open(locale_file, "r", encoding="utf-8") as f:
                        self._translations[locale] = json.load(f)
                    logger.debug(f"Loaded locale: {locale}")
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Failed to load locale {locale}: {e}")
            else:
                logger.debug(f"Locale file not found: {locale_file}")

    def load_locale(self, locale: str) -> bool:
        """
        Explicitly load a locale file.

        Args:
            locale: Locale code (e.g., 'en-AU', 'en-US')

        Returns:
            bool: True if loaded successfully, False otherwise
        """
        if locale not in SUPPORTED_LOCALES:
            logger.warning(f"Unsupported locale: {locale}")
            return False

        if locale in self._translations:
            return True

        locale_path = Path(self._locales_dir) / f"{locale}.json"
        if not locale_path.exists():
            logger.error(f"Locale file not found: {locale_path}")
            return False

        try:
            with open(locale_path, "r", encoding="utf-8") as f:
                self._translations[locale] = json.load(f)
            logger.info(f"Loaded locale: {locale}")
            return True
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load locale {locale}: {e}")
            return False

    def t(self, key: str, **kwargs) -> str:
        """
        Translate a key with optional variable substitution.

        Args:
            key: Translation key using dot notation (e.g., 'nav.roster', 'btn.submit')
            **kwargs: Variables for string substitution (e.g., name="John")

        Returns:
            str: Translated string, or key itself if not found
        """
        locale = self._current_locale
        translations = self._translations.get(locale, {})

        # Navigate nested keys (e.g., 'nav.roster' -> translations['nav']['roster'])
        keys = key.split(".")
        value = translations

        try:
            for k in keys:
                value = value[k]
        except (KeyError, TypeError):
            logger.debug(f"Translation key not found: {key} (locale: {locale})")
            # Fall back to key itself
            return key

        if not isinstance(value, str):
            logger.warning(f"Translation value is not a string: {key}")
            return key

        # Perform variable substitution if kwargs provided
        if kwargs:
            try:
                return value.format(**kwargs)
            except KeyError as e:
                logger.warning(
                    f"Missing variable in translation {key}: {e}"
                )
                return value

        return value

    def get_locale(self) -> str:
        """Get the current locale."""
        return self._current_locale

    def set_locale(self, locale: str) -> bool:
        """
        Set the current locale.

        Args:
            locale: Locale code (e.g., 'en-AU')

        Returns:
            bool: True if locale was set, False if unsupported
        """
        if locale not in SUPPORTED_LOCALES:
            logger.warning(f"Unsupported locale: {locale}")
            return False

        if locale not in self._translations and not self.load_locale(locale):
            logger.warning(f"Could not load locale: {locale}")
            return False

        self._current_locale = locale
        return True

    def available_locales(self) -> List[str]:
        """
        Get list of available (loaded) locales.

        Returns:
            List of locale codes
        """
        return list(self._translations.keys())

    def get_all_translations(self, locale: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all translations for a locale.

        Args:
            locale: Locale code. Defaults to current locale.

        Returns:
            Dictionary of all translations
        """
        if locale is None:
            locale = self._current_locale

        return self._translations.get(locale, {})


# Global i18n instance
_i18n_instance: Optional[I18n] = None


def init_i18n(locales_dir: Optional[str] = None) -> I18n:
    """
    Initialize the global i18n instance.

    Args:
        locales_dir: Directory containing locale JSON files

    Returns:
        I18n instance
    """
    global _i18n_instance
    _i18n_instance = I18n(locales_dir)
    return _i18n_instance


def get_i18n() -> I18n:
    """
    Get the global i18n instance.

    Creates one with defaults if not yet initialized.

    Returns:
        I18n instance
    """
    global _i18n_instance
    if _i18n_instance is None:
        _i18n_instance = I18n()
    return _i18n_instance


def t(key: str, **kwargs) -> str:
    """
    Convenience function for translating using the global i18n instance.

    Args:
        key: Translation key
        **kwargs: Variables for substitution

    Returns:
        Translated string
    """
    return get_i18n().t(key, **kwargs)
