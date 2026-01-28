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
- Start with the project name and author or bureau in this format: Cloud 11 Office Complex / Snøhetta
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
1. Headline
2. On a new line, a 2-sentence summary
3. On a new line, 1 relevant tag, the realm of the project (landscapearchitecture, urbanism, residentialdevelopment, etc.). No spaces or hyphens. 

Example format:
Residential tower in Tokyo / Studio XYZ
Studio XYZ has completed a residential tower in Tokyo featuring a diagrid structural system. The 32-story building uses cross-laminated timber for its facade, making it one of the tallest timber-hybrid structures in Asia.
residential"""

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