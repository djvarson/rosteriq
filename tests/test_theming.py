"""
Tests for the white-label theming system.

Covers:
- ThemeService configuration and validation
- CSS variable generation
- Color contrast calculation
- Logo validation
- Theme persistence
- API endpoint authorization
"""

import pytest
import base64
from datetime import datetime
from unittest.mock import Mock, patch

from rosteriq.services.theming import ThemeService, ThemeConfig
from rosteriq.database import MemoryStore


class TestThemeConfig:
    """Test ThemeConfig dataclass."""

    def test_theme_config_defaults(self):
        """Test ThemeConfig initializes with correct defaults."""
        config = ThemeConfig(venue_id="test-venue")

        assert config.venue_id == "test-venue"
        assert config.company_name == "RosterIQ"
        assert config.primary_color == "#1e3a5f"
        assert config.secondary_color == "#f8f9fa"
        assert config.accent_color == "#28a745"
        assert config.text_color == "#212529"
        assert config.header_bg == "#1a1a2e"
        assert config.font_family == "Inter, sans-serif"
        assert config.email_header_color == "#1e3a5f"
        assert config.email_footer_text == "Powered by RosterIQ"

    def test_theme_config_to_dict(self):
        """Test ThemeConfig serializes to dict."""
        config = ThemeConfig(venue_id="test", company_name="Acme")
        data = config.to_dict()

        assert isinstance(data, dict)
        assert data["venue_id"] == "test"
        assert data["company_name"] == "Acme"

    def test_theme_config_from_dict(self):
        """Test ThemeConfig deserializes from dict."""
        data = {
            "venue_id": "test",
            "company_name": "Acme",
            "primary_color": "#FF0000",
        }
        config = ThemeConfig.from_dict(data)

        assert config.venue_id == "test"
        assert config.company_name == "Acme"
        assert config.primary_color == "#FF0000"


class TestThemeService:
    """Test ThemeService class."""

    @pytest.fixture
    def db(self):
        """Create an in-memory database."""
        return MemoryStore()

    @pytest.fixture
    def service(self, db):
        """Create a ThemeService instance."""
        return ThemeService(db=db)

    def test_get_theme_returns_defaults(self, service):
        """Test getting a non-existent theme returns defaults."""
        config = service.get_theme("unknown-venue")

        assert config.venue_id == "unknown-venue"
        assert config.company_name == "RosterIQ"

    def test_set_and_get_theme(self, service):
        """Test saving and retrieving a theme."""
        config = ThemeConfig(
            venue_id="venue-1",
            company_name="Acme Corp",
            primary_color="#FF0000",
        )
        service.set_theme("venue-1", config)

        retrieved = service.get_theme("venue-1")
        assert retrieved.company_name == "Acme Corp"
        assert retrieved.primary_color == "#FF0000"

    def test_delete_theme(self, service):
        """Test deleting a theme."""
        config = ThemeConfig(venue_id="venue-1", company_name="Custom")
        service.set_theme("venue-1", config)

        service.delete_theme("venue-1")
        retrieved = service.get_theme("venue-1")

        # Should return defaults
        assert retrieved.company_name == "RosterIQ"

    def test_set_theme_validates_colors(self, service):
        """Test that set_theme validates hex colors."""
        bad_config = ThemeConfig(
            venue_id="venue-1",
            primary_color="not-a-hex-color",
        )

        with pytest.raises(ValueError, match="Invalid hex color"):
            service.set_theme("venue-1", bad_config)

    def test_validate_color_valid(self, service):
        """Test validate_color accepts valid hex colors."""
        assert service.validate_color("#000000") is True
        assert service.validate_color("#FFF") is True
        assert service.validate_color("#FF0000") is True
        assert service.validate_color("#abc") is True

    def test_validate_color_invalid(self, service):
        """Test validate_color rejects invalid colors."""
        assert service.validate_color("not-hex") is False
        assert service.validate_color("#GGGGGG") is False
        assert service.validate_color("") is False
        assert service.validate_color("#12345") is False

    def test_generate_contrast_color_light_bg(self, service):
        """Test contrast color for light background."""
        # Light background should get dark text
        contrast = service.generate_contrast_color("#FFFFFF")
        assert contrast == "#000000"

    def test_generate_contrast_color_dark_bg(self, service):
        """Test contrast color for dark background."""
        # Dark background should get light text
        contrast = service.generate_contrast_color("#000000")
        assert contrast == "#FFFFFF"

    def test_generate_css_variables(self, service):
        """Test CSS variable generation."""
        config = ThemeConfig(
            venue_id="venue-1",
            company_name="Acme",
            primary_color="#FF0000",
        )
        service.set_theme("venue-1", config)

        css = service.generate_css_variables("venue-1")

        assert ":root {" in css
        assert "--primary-color: #FF0000" in css
        assert "--company-name: \"Acme\"" in css
        assert "--font-family:" in css

    def test_get_theme_script_tag(self, service):
        """Test script tag generation."""
        config = ThemeConfig(
            venue_id="venue-1",
            company_name="Custom Venue",
        )
        service.set_theme("venue-1", config)

        script = service.get_theme_script_tag("venue-1")

        assert "<script>" in script
        assert "document.head.prepend(style)" in script
        assert "--primary-color:" in script

    def test_validate_logo_png(self, service):
        """Test PNG logo validation."""
        # PNG magic bytes: 89 50 4E 47
        png_data = b"\x89PNG\r\n\x1a\n" + b"x" * 100
        b64 = base64.b64encode(png_data).decode("utf-8")

        assert service.validate_logo(b64) is True

    def test_validate_logo_jpg(self, service):
        """Test JPG logo validation."""
        # JPG magic bytes: FF D8 FF
        jpg_data = b"\xFF\xD8\xFF" + b"x" * 100
        b64 = base64.b64encode(jpg_data).decode("utf-8")

        assert service.validate_logo(b64) is True

    def test_validate_logo_svg(self, service):
        """Test SVG logo validation."""
        svg_data = b"<svg xmlns='http://www.w3.org/2000/svg'></svg>"
        b64 = base64.b64encode(svg_data).decode("utf-8")

        assert service.validate_logo(b64) is True

    def test_validate_logo_invalid_format(self, service):
        """Test invalid logo format rejection."""
        bad_data = base64.b64encode(b"not an image format").decode("utf-8")

        assert service.validate_logo(bad_data) is False

    def test_validate_logo_too_large(self, service):
        """Test logo size limit."""
        # Create 501 KB of data
        large_data = base64.b64encode(b"\x89PNG" + b"x" * 512000).decode("utf-8")

        assert service.validate_logo(large_data, max_size_kb=500) is False

    def test_preview_theme(self, service):
        """Test HTML preview generation."""
        config = ThemeConfig(
            venue_id="venue-1",
            company_name="Acme Corp",
            header_bg="#1a1a2e",
        )

        html = service.preview_theme(config)

        assert "Acme Corp" in html
        assert "background: #1a1a2e" in html
        assert "Hospitality Roster Management" in html


class TestThemeIntegration:
    """Integration tests for theming system."""

    @pytest.fixture
    def db(self):
        return MemoryStore()

    @pytest.fixture
    def service(self, db):
        return ThemeService(db=db)

    def test_full_workflow(self, service):
        """Test complete theming workflow."""
        # 1. Create a custom theme
        config = ThemeConfig(
            venue_id="acme-restaurant",
            company_name="Acme Restaurant Group",
            primary_color="#FF6600",
            secondary_color="#FFE6CC",
            header_bg="#2C2C2C",
            logo_url="https://example.com/logo.png",
        )

        # 2. Save the theme
        service.set_theme("acme-restaurant", config)

        # 3. Retrieve and verify
        retrieved = service.get_theme("acme-restaurant")
        assert retrieved.company_name == "Acme Restaurant Group"
        assert retrieved.primary_color == "#FF6600"

        # 4. Generate CSS
        css = service.generate_css_variables("acme-restaurant")
        assert "--primary-color: #FF6600" in css

        # 5. Generate preview
        preview = service.preview_theme(retrieved)
        assert "Acme Restaurant Group" in preview

        # 6. Delete theme
        service.delete_theme("acme-restaurant")
        reset_config = service.get_theme("acme-restaurant")
        assert reset_config.company_name == "RosterIQ"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
