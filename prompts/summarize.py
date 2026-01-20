# prompts/summarize.py
"""
Summarization Prompts
Prompts for generating article summaries and tags.
"""

from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate

# System prompt for the AI summarizer
SUMMARIZE_SYSTEM_PROMPT = """You are an architecture news editor for a professional digest. 
Your task is to create concise, informative summaries of architecture and design articles.

Today's date is {current_date}. Use this for temporal context when describing projects.

Guidelines:
- Write exactly 2 sentences in British English
- First sentence: What is the project (who designed what, where)
- Second sentence: What makes it notable or interesting
- Be specific and factual, avoid generic praise
- Use professional architectural terminology where appropriate
- Keep the tone informative but engaging
- If the article is an opinion piece, note that it's an opinion piece, but still mention the project discussed
- If it's an interview, note that it's an interview, but still mention the project discussed
- Do not use emoji"""

# User message template
SUMMARIZE_USER_TEMPLATE = """Summarize this architecture article:

Title: {title}

Description: {description}

Source: {url}

Respond with ONLY:
1. A 2-sentence summary
2. On a new line, 2-3 relevant tags as comma-separated lowercase words. Tags shouldn't have spaces or hyphens. 
Example format:
Studio XYZ has completed a residential tower in Tokyo featuring a diagrid structural system. The 32-story building uses cross-laminated timber for its facade, making it one of the tallest timber-hybrid structures in Asia.

residential, tokyo, timberconstruction"""

# Combined ChatPromptTemplate for LangChain
SUMMARIZE_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(SUMMARIZE_SYSTEM_PROMPT),
    HumanMessagePromptTemplate.from_template(SUMMARIZE_USER_TEMPLATE)
])


def parse_summary_response(response_text: str) -> dict:
    """
    Parse AI response into summary and tags.

    Args:
        response_text: Raw AI response

    Returns:
        Dict with 'summary' and 'tags' keys
    """
    lines = response_text.strip().split('\n')

    # Find the tags line (last non-empty line with commas)
    tags_line = ""
    summary_lines = []

    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        if not tags_line and ',' in line and len(line) < 100:
            # Likely the tags line
            tags_line = line
        else:
            summary_lines.insert(0, line)

    summary = ' '.join(summary_lines).strip()

    # Parse tags
    tags = []
    if tags_line:
        tags = [tag.strip().lower() for tag in tags_line.split(',')]
        # Clean up tags (remove any that look like sentences)
        tags = [t for t in tags if len(t) < 30 and ' ' not in t or t.replace(' ', '_')]

    return {
        "summary": summary,
        "tags": tags[:3]  # Max 3 tags
    }