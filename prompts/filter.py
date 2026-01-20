# prompts/filter.py
"""
Article Filter Prompts
Prompts for classifying and filtering architecture news articles.

Filters OUT:
- Interior design articles
- Private residences / single-family homes
- Product/furniture design
- Small-scale renovations

Keeps:
- Large-scale architectural projects
- Well-known architecture firms
- Public buildings, cultural institutions
- Urban planning, infrastructure
"""

from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate

# System prompt for the article filter
FILTER_SYSTEM_PROMPT = """You are an architecture news editor filtering articles for a professional digest focused on significant architectural projects.

Your task is to classify whether an article should be INCLUDED or EXCLUDED from the digest.

INCLUDE articles about:
- Large-scale architectural projects (commercial, cultural, institutional, public)
- Well-known or award-winning architecture firms (Zaha Hadid, BIG, OMA, Foster + Partners, Herzog & de Meuron, SANAA, Renzo Piano, Snohetta, MVRDV, etc.)
- Famous architects (Norman Foster, Richard Rogers, Tadao Ando, etc.)
- Public buildings: museums, libraries, theaters, stadiums, airports, stations
- Urban planning and masterplans
- Educational and healthcare facilities
- Mixed-use and commercial developments
- Infrastructure projects (bridges, transit, public spaces)
- Award-winning projects (Pritzker, RIBA, AIA awards)
- Significant sustainability/innovation in architecture
- Architecture exhibitions and biennales

EXCLUDE articles about:
- New issues of architectural magazines even if they mention projects
- pices about newsletters, even if they mention projects
- Interior design and decoration
- Private residences, single-family homes, villas, apartments
- Furniture and product design
- Small renovations or home improvements
- Retail store fit-outs (unless flagship by major firm)
- Restaurant/cafe/bar interiors
- Office interior redesigns
- Residential real estate listings
- DIY or home decor content

Be strict: when in doubt about private residences or interiors, EXCLUDE.
Do not use emoji in your response."""

# User message template
FILTER_USER_TEMPLATE = """Classify this architecture article:

Title: {title}

Description: {description}

Source: {source}

Respond with ONLY one line in this exact format:
VERDICT: INCLUDE or EXCLUDE
REASON: One brief sentence explaining why

Example responses:
VERDICT: INCLUDE
REASON: Major cultural center by Zaha Hadid Architects

VERDICT: EXCLUDE
REASON: Private residence interior renovation"""

# Combined ChatPromptTemplate for LangChain
FILTER_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(FILTER_SYSTEM_PROMPT),
    HumanMessagePromptTemplate.from_template(FILTER_USER_TEMPLATE)
])


def parse_filter_response(response_text: str) -> dict:
    """
    Parse AI filter response into structured result.

    Args:
        response_text: Raw AI response

    Returns:
        Dict with 'include' (bool), 'reason' (str)
    """
    lines = response_text.strip().split('\n')

    include = True  # Default to include if parsing fails
    reason = ""

    for line in lines:
        line = line.strip()

        if line.upper().startswith('VERDICT:'):
            verdict = line.split(':', 1)[1].strip().upper()
            include = verdict == 'INCLUDE'

        elif line.upper().startswith('REASON:'):
            reason = line.split(':', 1)[1].strip()

    return {
        "include": include,
        "reason": reason
    }