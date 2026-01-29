# ADUmedia Custom Scrapers Pipeline

## Overview

This is a dedicated news aggregation pipeline for architecture and design publications that lack working RSS feeds. The system scrapes article URLs from various architecture magazines worldwide, filters content using AI, generates summaries, and stores processed articles for editorial selection.

The pipeline runs on a scheduled basis (cron at 21:30 UTC) and processes articles through: URL discovery → content scraping → AI filtering → summarization → cloud storage.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Pipeline Design

The system follows a modular operator pattern where each stage is handled by a dedicated component:

1. **Custom Scrapers** - Site-specific scrapers inherit from `BaseCustomScraper` and implement URL discovery logic for each publication
2. **Article Scraper** - Uses Playwright with Railway Browserless to fetch full article content
3. **AI Filter** - GPT-4o-mini classifies articles before summarization to reduce API costs
4. **Summarizer** - Generates standardized summaries with project name, architect, and tags
5. **Storage** - Saves to Cloudflare R2 with date-organized folder structure

### Scraper Architecture

Each custom scraper follows a consistent pattern:
- Inherits from `BaseCustomScraper` base class
- Auto-registers via `CustomScraperRegistry` decorator pattern
- Discovers URLs from source homepage/category pages only
- Delegates content scraping, date extraction, and filtering to main pipeline
- Uses `ArticleTracker` (PostgreSQL) to prevent reprocessing seen URLs

Currently implemented scrapers cover publications from Middle East, Asia-Pacific, Europe, and Americas regions including Identity Magazine, Archiposition, Gooood, Domus, Metalocus, and others.

### Storage Strategy

R2 bucket uses a hierarchical date-based structure:
```
/YYYY/Month/Week-N/YYYY-MM-DD/
  ├── images/        # Shared hero images
  ├── candidates/    # Articles pending editorial review
  ├── selected/      # Curated digest
  └── archive/       # Processed articles
```

Images stored at date level (not inside candidates/archive) so they remain accessible regardless of article status.

### Browser Automation

Uses Playwright connected to Railway Browserless service for JavaScript-rendered pages. Some scrapers fall back to cloudscraper for sites with anti-bot protection.

## External Dependencies

### Cloud Services
- **Railway** - Deployment platform with cron scheduling and Browserless browser service
- **Cloudflare R2** - S3-compatible object storage for articles and images
- **Supabase** - Optional database for cross-edition article tracking

### Databases
- **PostgreSQL** (via `asyncpg`) - Primary article URL tracking to prevent reprocessing
- **Supabase** (optional) - Additional article recording at fetch time

### AI/ML Services
- **OpenAI GPT-4o-mini** - Article filtering and summarization via LangChain
- **LangSmith** - Optional tracing for LangChain operations

### Key Python Libraries
- `playwright` - Browser automation for content scraping
- `langchain` + `langchain-openai` - AI orchestration
- `boto3` - R2/S3 storage operations
- `beautifulsoup4` - HTML parsing for URL extraction
- `feedparser` - RSS feed parsing (used by related RSS service)
- `cloudscraper` - Bypasses basic anti-bot protection
- `Pillow` - Image processing

### Environment Variables Required
```
OPENAI_API_KEY              - OpenAI API access
BROWSER_PLAYWRIGHT_ENDPOINT - Railway Browserless WebSocket URL
R2_ACCOUNT_ID               - Cloudflare account
R2_ACCESS_KEY_ID            - R2 credentials
R2_SECRET_ACCESS_KEY        - R2 credentials
R2_BUCKET_NAME              - Target bucket
DATABASE_URL                - PostgreSQL connection string
SUPABASE_URL                - Optional Supabase project
SUPABASE_KEY                - Optional Supabase API key
SCRAPER_TEST_MODE           - Set "true" to ignore seen status for testing
```