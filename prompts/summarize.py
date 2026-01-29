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
- Title format: PROJECT NAME / ARCHITECT OR BUREAU (e.g., "Cloud 11 Office Complex / Snøhetta"). If the architect or bureau is unknown, don't write anything, just the name of the project. Don't write Unknown or Unknown Architect.
- Write description: exactly 2 sentences in British English. First sentence: What is the project (who designed what, where). Second sentence: What makes it notable or interesting
- If the project name is in the language that doesn't match the country language (for example, in ArchDaily Brasil a project in Cina is named in Portuguese), translate the name of the project to English
- Be specific and factual, avoid generic praise
- Use professional architectural terminology where appropriate
- Keep the tone informative but engaging
- If the article is an opinion piece, note that it's an opinion piece, but still mention the project discussed
- If it's an interview, note that it's an interview, but still mention the project discussed
- CRITICAL: Do not use emojis anywhere in your response
- CRITICAL: Keep the title clean and professional - just the project name and architect/bureau separated by a forward slash"""

# User message template
SUMMARIZE_USER_TEMPLATE = """Summarize this architecture article:

Title: {title}
Description: {description}
Source: {url}

Respond with ONLY:
1. Title in format: PROJECT NAME / ARCHITECT OR BUREAU or just PROJECT NAME oif author unknown or irrelevant
2. On a new line, a 2-sentence summary
3. On a new line, 1 relevant tag (one word, the realm or type of the project: urbanism, museums, library, culture, education, airport,  etc.). No spaces, hyphens, or special characters in the tag.

Example format:
Cloud 11 Office Complex / Snøhetta
Snøhetta has completed an office complex in Tokyo featuring a diagrid structural system. The 32-story building uses cross-laminated timber for its facade, making it one of the tallest timber-hybrid structures in Asia.
commercial"""

# Combined ChatPromptTemplate for LangChain
SUMMARIZE_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(SUMMARIZE_SYSTEM_PROMPT),
    HumanMessagePromptTemplate.from_template(SUMMARIZE_USER_TEMPLATE)
])


def parse_summary_response(response_text: str) -> dict:
    """
    Parse AI response into headline, summary and tag.

    Args:
        response_text: Raw AI response

    Returns:
        Dict with 'headline', 'summary' and 'tag' keys
    """
    lines = [line.strip() for line in response_text.strip().split('\n') if line.strip()]

    if len(lines) >= 3:
        headline = lines[0]
        summary = lines[1]
        tag = lines[2].lower().strip()
    elif len(lines) == 2:
        headline = lines[0]
        summary = lines[1]
        tag = ""
    else:
        headline = ""
        summary = lines[0] if lines else ""
        tag = ""

    return {
        "headline": headline,
        "summary": summary,
        "tag": tag
    }