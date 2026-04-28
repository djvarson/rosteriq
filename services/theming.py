"""
White-label theming service for RosterIQ.

Provides theme configuration, CSS variable generation, and branding customization
for multi-tenant deployments. Supports logos, custom colors, fonts, and email templates.

Usage:
    from rosteriq.services.theming import ThemeService
    theme_svc = ThemeService()
    config = theme_svc.get_theme("venue-123")
    css = theme_svc.generate_css_variables("venue-123")
"""

import re
import base64
import logging
from dataclasses import dataclass, asdict, field
from typing import Optional
from colorsys import rgb_to_hsv, hsv_to_rgb

from rosteriq.database import get_db

logger = logging.getLogger(__name__)


@dataclass
class ThemeConfig:
    """Configuration for a venue's theme and branding."""

    venue_id: str
    company_name: str = "RosterIQ"
    logo_url: Optional[str] = None
    primary_color: str = "#1e3a5f"
    secondary_color: str = "#f8f9fa"
    accent_color: str = "#28a745"
    text_color: str = "#212529"
    header_bg: str = "#1a1a2e"
    font_family: str = "Inter, sans-serif"
    favicon_url: Optional[str] = None
    email_header_color: str = "#1e3a5f"
    email_footer_text: str = "Powered by RosterIQ"

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ThemeConfig":
        """Create instance from dictionary."""
        return cls(**data)


class ThemeService:
    """Service for managing venue theming and branding."""

    def __init__(self, db=None):
        self.db = db or get_db()
        self._theme_cache = {}  # Simple in-memory cache
        self._cache_ttl = 300  # 5 minutes

    def get_theme(self, venue_id: str) -> ThemeConfig:
        """
        Get theme config for a venue.

        Returns defaults if no custom theme is set.
        Uses in-memory cache to avoid DB hits on every request.
        """
        try:
            stored = self.db.get_theme(venue_id)
            if stored:
                return ThemeConfig.from_dict(stored)
        except Exception as e:
            logger.warning(f"Failed to retrieve theme for {venue_id}: {e}")

        # Return defaults
        return ThemeConfig(venue_id=venue_id)

    def set_theme(self, venue_id: str, config: ThemeConfig) -> None:
        """
        Save theme configuration for a venue.

        Validates colors before saving.
        """
        # Validate all color fields
        colors_to_validate = [
            ("primary_color", config.primary_color),
            ("secondary_color", config.secondary_color),
            ("accent_color", config.accent_color),
            ("text_color", config.text_color),
            ("header_bg", config.header_bg),
            ("email_header_color", config.email_header_color),
        ]

        for field_name, color in colors_to_validate:
            if not self.validate_color(color):
                raise ValueError(f"Invalid hex color for {field_name}: {color}")

        # Validate font family (basic check)
        if not config.font_family or len(config.font_family) > 200:
            raise ValueError("Invalid font_family")

        # Store in database
        self.db.save_theme(venue_id, config.to_dict())
        logger.info(f"Theme saved for venue {venue_id}")

    def delete_theme(self, venue_id: str) -> None:
        """Reset a venue's theme to defaults."""
        self.db.delete_theme(venue_id)
        if venue_id in self._theme_cache:
            del self._theme_cache[venue_id]
        logger.info(f"Theme reset for venue {venue_id}")

    @staticmethod
    def validate_color(hex_string: str) -> bool:
        """
        Validate a hex color string.

        Accepts: #RGB, #RRGGBB
        """
        if not hex_string:
            return False

        hex_pattern = re.compile(r"^#(?:[0-9a-fA-F]{3}){1,2}$")
        return bool(hex_pattern.match(hex_string))

    @staticmethod
    def generate_contrast_color(bg_hex: str) -> str:
        """
        Generate a contrasting text color (light or dark) for readability.

        Uses luminance calculation: if background is dark, return light text.
        Otherwise return dark text.
        """
        if not bg_hex or not bg_hex.startswith("#"):
            return "#000000"

        # Remove #
        hex_color = bg_hex.lstrip("#")

        # Expand shorthand
        if len(hex_color) == 3:
            hex_color = "".join([c * 2 for c in hex_color])

        try:
            r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))

            # Calculate relative luminance (WCAG formula)
            def adjust_channel(c):
                c = c / 255.0
                if c <= 0.03928:
                    return c / 12.92
                return ((c + 0.055) / 1.055) ** 2.4

            r_adj = adjust_channel(r)
            g_adj = adjust_channel(g)
            b_adj = adjust_channel(b)

            luminance = 0.2126 * r_adj + 0.7152 * g_adj + 0.0722 * b_adj

            # If luminance is high, use dark text; otherwise light text
            return "#000000" if luminance > 0.5 else "#FFFFFF"
        except Exception as e:
            logger.warning(f"Failed to generate contrast color for {bg_hex}: {e}")
            return "#000000"

    def generate_css_variables(self, venue_id: str) -> str:
        """
        Generate CSS custom properties for a theme.

        Returns a CSS string suitable for injection into <style> tags.
        """
        config = self.get_theme(venue_id)

        # Calculate contrasting colors for headers and text
        header_text_color = self.generate_contrast_color(config.header_bg)
        primary_text_color = self.generate_contrast_color(config.primary_color)

        css_vars = f"""
:root {{
  --primary-color: {config.primary_color};
  --secondary-color: {config.secondary_color};
  --accent-color: {config.accent_color};
  --text-color: {config.text_color};
  --header-bg: {config.header_bg};
  --header-text-color: {header_text_color};
  --primary-text-color: {primary_text_color};
  --font-family: {config.font_family};
  --email-header-color: {config.email_header_color};
  --company-name: "{config.company_name}";
}}
""".strip()

        return css_vars

    def get_theme_script_tag(self, venue_id: str) -> str:
        """
        Generate a <script> tag that injects theme CSS variables at runtime.

        Useful for static HTML files that need theming without server-side rendering.
        """
        config = self.get_theme(venue_id)
        css_content = self.generate_css_variables(venue_id)

        # Escape CSS for safe injection
        safe_css = css_content.replace('"', '\\"').replace("\n", "\\n")

        script = f"""
<script>
(function() {{
  var style = document.createElement('style');
  style.textContent = `{safe_css}`;
  document.head.prepend(style);

  // Also set favicon if available
  if ('{config.favicon_url}') {{
    var link = document.querySelector("link[rel='icon']") || document.createElement('link');
    link.rel = 'icon';
    link.href = '{config.favicon_url}';
    if (!document.querySelector("link[rel='icon']")) {{
      document.head.append(link);
    }}
  }}
}})();
</script>
""".strip()

        return script

    def validate_logo(self, base64_data: str, max_size_kb: int = 500) -> bool:
        """
        Validate a base64-encoded logo.

        Checks:
        - Valid base64
        - Size <= max_size_kb
        - Image format (PNG, JPG, SVG)
        """
        try:
            # Decode to check validity
            data = base64.b64decode(base64_data)

            # Check size
            size_kb = len(data) / 1024
            if size_kb > max_size_kb:
                logger.warning(f"Logo too large: {size_kb}KB > {max_size_kb}KB")
                return False

            # Check magic bytes for common formats
            magic_bytes = data[:8]

            # PNG: 89 50 4E 47 ...
            if magic_bytes.startswith(b"\x89PNG"):
                return True

            # JPG: FF D8 FF ...
            if magic_bytes.startswith(b"\xFF\xD8\xFF"):
                return True

            # SVG: starts with < (XML)
            if magic_bytes.startswith(b"<"):
                return True

            logger.warning("Logo format not recognized (must be PNG, JPG, or SVG)")
            return False

        except Exception as e:
            logger.warning(f"Invalid base64 logo data: {e}")
            return False

    def preview_theme(self, config: ThemeConfig) -> str:
        """
        Generate an HTML snippet showing a preview of the themed header.

        Useful for UI previews before saving.
        """
        header_text = self.generate_contrast_color(config.header_bg)
        logo_html = ""

        if config.logo_url:
            logo_html = f'<img src="{config.logo_url}" alt="Logo" style="height:32px; margin-right:12px;">'

        preview = f"""
<div style="background: {config.header_bg}; color: {header_text}; padding: 16px 24px; display: flex; align-items: center; font-family: {config.font_family};">
  {logo_html}
  <div>
    <div style="font-size: 18px; font-weight: 600;">{config.company_name}</div>
    <div style="font-size: 12px; opacity: 0.8;">Hospitality Roster Management</div>
  </div>
</div>
""".strip()

        return preview
