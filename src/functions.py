"""Functions for OpenAI function calling.
"""
from src.prompts import ChatGPTSession, Prompt

import json


def extract_line_items(chat_gpt_session: ChatGPTSession, extract: str) -> json:
    """Extract the financial line items from a source sentence.

    Args:
        source_sentence (str): A text excerpt from an earnings call transcript.

    Returns:
        json: JSON object of line items
    """
    prompt = Prompt(
        role='system',
        content="""Return a json object below for each financial line item in the extract. If multiple line items exist, return multiple jsons for each line item. Only create a new json for unique line items, be very strict. Do not include whitespace in the json.

        json template:
        "line_item": Return the name of the financial line item referenced,
        "statement_type": Return forward-looking for future guidance and retrospective for historical info,
        "period": Return only the time period related to line_item, be very strict
        """,
        response_type='json'
    )
    response = chat_gpt_session.openai_gpt_api_call(
        prompt=prompt,
        temperature=0,
        presence_penalty=-1.5
    )
    return response


FUNCTIONS = [
    {
        "name": "extract_line_items",
        "description": "Extract financial line items from a source sentence.",
        "parameters": {
            "type": "object",
            "properties": {
                "extract": {
                    "type": "string",
                    "description": "A text excerpt from an earnings call transcript.",
                }
            },
            "required": ["extract"],
        }
    }
]

AVAILABLE_FUNCTIONS = {
    "extract_line_items": extract_line_items
}
