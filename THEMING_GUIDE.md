# White-Label Theming System

## Overview

RosterIQ now supports complete white-label branding customization, allowing each venue to customize colors, logos, fonts, and company name. The theming system is composed of four main components:

1. **services/theming.py** - Core theming logic
2. **routes/theming.py** - REST API endpoints
3. **middleware/theming.py** - Automatic CSS injection middleware
4. **database.py** - Persistence layer (BaseStore, MemoryStore, PostgresStore)

## Architecture

### ThemeService

Manages all theme operations:

```python
from rosteriq.services.theming import ThemeService, ThemeConfig

service = ThemeService()

# Get theme (returns defaults if not set)
config = service.get_theme("venue-123")

# Update theme
config.company_name = "Acme Restaurant"
config.primary_color = "#FF6600"
service.set_theme("venue-123", config)

# Get CSS variables
css = service.generate_css_variables("venue-123")
# Output: :root { --primary-color: #FF6600; ... }

# Generate script tag for dynamic injection
script = service.get_theme_script_tag("venue-123")
```

### ThemeConfig Dataclass

All configurable theme properties:

```python
@dataclass
class ThemeConfig:
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
```

## API Endpoints

All endpoints require JWT authentication and user authorization for the venue.

### Get Theme

```
GET /api/theme/{venue_id}
Authorization: Bearer <jwt>

Response:
{
  "venue_id": "venue-123",
  "company_name": "Acme Restaurant",
  "primary_color": "#FF6600",
  "secondary_color": "#FFE6CC",
  "accent_color": "#28a745",
  "text_color": "#212529",
  "header_bg": "#2C2C2C",
  "font_family": "Inter, sans-serif",
  "logo_url": "data:image/png;base64,...",
  "favicon_url": null,
  "email_header_color": "#FF6600",
  "email_footer_text": "Acme Restaurant Group"
}
```

### Update Theme

```
PUT /api/theme/{venue_id}
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "company_name": "Acme Restaurant",
  "primary_color": "#FF6600",
  "secondary_color": "#FFE6CC",
  "header_bg": "#2C2C2C"
}

Response: Updated ThemeConfig
```

### Upload Logo

```
POST /api/theme/{venue_id}/logo
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "data": "iVBORw0KGgoAAAANSUhEUgAAAAEA..."
}

Response:
{
  "status": "success",
  "logo_url": "data:image/png;base64,iVBORw0KG...",
  "size_kb": 45.2
}
```

Accepted formats: PNG, JPG, SVG (max 500 KB)

### Get CSS Variables

```
GET /api/theme/{venue_id}/css
Authorization: Bearer <jwt>

Response (text/css):
:root {
  --primary-color: #FF6600;
  --secondary-color: #FFE6CC;
  --accent-color: #28a745;
  --text-color: #212529;
  --header-bg: #2C2C2C;
  --header-text-color: #FFFFFF;
  --font-family: Inter, sans-serif;
  --company-name: "Acme Restaurant";
}
```

### Get Preview

```
GET /api/theme/{venue_id}/preview
Authorization: Bearer <jwt>

Response (text/html):
<div style="background: #2C2C2C; color: #FFFFFF; ...">
  <div style="font-size: 18px; font-weight: 600;">Acme Restaurant</div>
  <div style="font-size: 12px;">Hospitality Roster Management</div>
</div>
```

### Reset to Defaults

```
DELETE /api/theme/{venue_id}
Authorization: Bearer <jwt>

Response: 204 No Content
```

## Middleware Integration

The `ThemeInjectorMiddleware` automatically injects theme CSS variables into HTML responses for the following routes:

- `/dashboard`
- `/staff`
- `/admin`
- `/settings`

The middleware:

1. Extracts `venue_id` from JWT token
2. Generates theme CSS variables
3. Injects `<style>:root { ... }</style>` before `</head>` tag
4. Caches themes in memory (5 minute TTL) for performance

### How It Works

```
Request: GET /dashboard
  ↓
Extract venue_id from JWT
  ↓
Check cache / load theme from database
  ↓
Generate CSS variables
  ↓
Inject <style> into HTML response
  ↓
Response: HTML with styled content
```

## Color Validation

The system includes automatic color validation:

```python
# Valid hex colors
service.validate_color("#000000")  # 6-digit
service.validate_color("#FFF")     # 3-digit
service.validate_color("#abc")     # lowercase

# Invalid
service.validate_color("not-hex")  # No match
service.validate_color("#GGGGGG")  # Invalid hex digits
service.validate_color("")         # Empty
```

## Contrast Calculation

Automatic text color selection for readability:

```python
service.generate_contrast_color("#FFFFFF")  # Returns #000000 (dark)
service.generate_contrast_color("#000000")  # Returns #FFFFFF (light)

# Uses WCAG luminance formula for accessibility
```

## Logo Validation

Logo uploads are validated for:

- **Format**: PNG, JPG, SVG (magic byte detection)
- **Size**: Max 500 KB
- **Encoding**: Valid base64

```python
# PNG magic bytes: 89 50 4E 47
# JPG magic bytes: FF D8 FF
# SVG: Starts with <
```

## Database Schema (PostgreSQL)

For production use with PostgreSQL, create the themes table:

```sql
CREATE TABLE themes (
    venue_id VARCHAR(255) PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_themes_venue_id ON themes(venue_id);
```

The `config` column stores the full ThemeConfig as JSON.

## Usage Example

### Complete Workflow

```python
from rosteriq.services.theming import ThemeService, ThemeConfig

# Initialize service
service = ThemeService()

# Create a custom theme
config = ThemeConfig(
    venue_id="pizza-palace-123",
    company_name="Pizza Palace",
    primary_color="#E63946",
    secondary_color="#F1FAEE",
    accent_color="#A8DADC",
    header_bg="#1D3557",
    logo_url="https://storage.example.com/pizza-palace-logo.png",
    favicon_url="https://storage.example.com/favicon.ico",
    email_header_color="#E63946",
    email_footer_text="© 2026 Pizza Palace. All rights reserved."
)

# Save theme
service.set_theme("pizza-palace-123", config)

# Generate CSS for embedding
css = service.generate_css_variables("pizza-palace-123")
# Use in <style> tag or CSS file

# Generate HTML preview
preview = service.preview_theme(config)
# Display in UI for user confirmation

# Later: Update theme
config = service.get_theme("pizza-palace-123")
config.primary_color = "#FF0000"
service.set_theme("pizza-palace-123", config)

# Reset to defaults
service.delete_theme("pizza-palace-123")
```

### Frontend Integration

In HTML templates, include the theme CSS:

```html
<!-- Option 1: Server-side injection (middleware handles this automatically) -->
<!-- The middleware injects CSS variables into responses -->

<!-- Option 2: Dynamic client-side injection -->
<script src="/api/theme/{{venue_id}}/script"></script>

<!-- Option 3: Direct CSS file link -->
<link rel="stylesheet" href="/api/theme/{{venue_id}}/css">
```

## Testing

Run the test suite:

```bash
cd /sessions/fervent-adoring-goodall/dropbox_rosteriq/RosterIQ
pytest tests/test_theming.py -v
```

Tests cover:

- Theme configuration and defaults
- Color validation
- Contrast calculation
- Logo validation
- CSS generation
- Database persistence
- API authorization

## Performance Considerations

1. **Caching**: Themes are cached in-memory for 5 minutes to reduce database hits
2. **CSS Generation**: CSS is generated once per theme and cached
3. **Logo Storage**: Logos stored as data: URLs (base64) for immediate use without external storage
4. **Middleware**: Only injects CSS for specific routes to minimize overhead

## Security

1. **Authorization**: All endpoints require JWT authentication and venue access verification
2. **Color Validation**: Prevents CSS injection via invalid hex values
3. **Logo Validation**: Magic byte detection prevents arbitrary file uploads
4. **Size Limits**: Logos limited to 500 KB to prevent DoS
5. **CORS**: Respects existing CORS configuration

## Customization Points

### Adding New Theme Properties

1. Add field to `ThemeConfig` dataclass
2. Update `ThemeConfigRequest` in routes
3. Update `ThemeConfigResponse` in routes
4. Add to `generate_css_variables()` output
5. Update tests

### Extending Validation

```python
class CustomThemeService(ThemeService):
    def validate_company_name(self, name: str) -> bool:
        # Add custom validation logic
        return len(name) > 0 and len(name) <= 100
```

### Custom CSS Variables

Extend `generate_css_variables()` to add custom properties:

```python
def generate_css_variables(self, venue_id: str) -> str:
    config = self.get_theme(venue_id)
    # ... existing code ...
    
    # Add custom variables
    css_vars += f"""
  --custom-property: {config.custom_value};
"""
    return css_vars
```

## Troubleshooting

### Theme Not Applied

1. Check JWT contains `venue_id` claim
2. Verify middleware is registered in `api.py`
3. Check that HTML response includes `</head>` tag
4. Verify database has theme entry (check `get_theme()`)

### Colors Not Validating

1. Ensure hex format: `#RGB` or `#RRGGBB`
2. Check case (both uppercase and lowercase valid)
3. Verify digits are 0-9 or A-F

### Logo Upload Failing

1. Check file format (PNG, JPG, SVG only)
2. Verify size < 500 KB
3. Ensure valid base64 encoding
4. Check file magic bytes

## Future Enhancements

- [ ] Theme versioning and rollback
- [ ] A/B testing themes across venues
- [ ] Pre-built theme templates
- [ ] Dark mode variant support
- [ ] Custom CSS injection (safe mode)
- [ ] Theme preview link sharing
- [ ] Analytics on theme changes
- [ ] Theme inheritance (venue group templates)
