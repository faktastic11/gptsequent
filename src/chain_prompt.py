"""Driver method for chain prompting
"""
from src.utils.loggers import reg_logger
from src.prompts import ChatGPTSession, Prompt


logger = reg_logger(__name__)


def chain_processor(
    model: str,
    transcript: list[str],
    issuer: str,
    period: str,
    chain_prompts: dict,
    source: str = "Earnings Call Transcript",
    issuer_description: str = None
) -> list[dict]:
    """
    Args:
        transcript (list[str]): Tokenized list of transcript extracts
        issuer (str): Issuer name
        period (str): Period of transcript
        source (str, optional): Source of transcript. Defaults to "Earnings Call Transcript".
        issuer_description (str, optional): Issuer description. Defaults to None.
        chain_prompts (dict): Chain prompts

    Returns:
        list[dict]: List of extracted financial line items
    """

    issuer_description = issuer_description if issuer_description is not None else issuer
    # Load chat gpt session

    gpt_session = ChatGPTSession(
        model=model,
        termination_key='TERMINATE',
        base_context=[
            Prompt(
                role=chain_prompts['base_context']['role'],
                content=chain_prompts['base_context']['content'],
                kwargs={
                    'issuer': issuer,
                    'description': issuer_description,
                    'period': period,
                    'source': source
                }
            )
        ]
    )

    i = 0  # Counter to keep track of transcript extracts and retrieve surrounding context
    # Iterate through transcript extracts
    for sentence in transcript:
        extract = sentence
        logger.info(f'Extract: {extract}')
        extract_line_item_prompt = chain_prompts['extract_line_item']
        extract_prompt = Prompt(
            role=extract_line_item_prompt['role'],
            content=extract_line_item_prompt['content'],
            kwargs={'extract': extract},
            response_type=extract_line_item_prompt['response_type'],
            next_prompt_key=extract_line_item_prompt['next_prompt_key']
        )
        try:
            # Get line items
            line_items = gpt_session.openai_gpt_api_call(prompt=extract_prompt)
            if line_items is not None:
                # Iterate through line items
                for line_item in line_items:
                    # Get next prompt
                    additional_context = {
                        'extract': extract,
                        'source_sentence': sentence,
                        'line_item': line_item['line_item'],
                        'statement_type': line_item['statement_type']
                    }
                    logger.info(f'Line item: {line_item}')
                    next_prompt_key = line_item[extract_prompt.next_prompt_key]
                    logger.info(f'Next prompt key: {next_prompt_key}')
                    # Get next prompt
                    while next_prompt_key != 'TERMINATE':
                        next_prompt = chain_prompts[next_prompt_key]
                        next_prompt = Prompt(
                            role=next_prompt['role'],
                            content=next_prompt['content'],
                            kwargs=additional_context,
                            response_type=next_prompt['response_type'],
                            next_prompt_key=next_prompt['next_prompt_key']
                        )
                        response = gpt_session.openai_gpt_api_call(
                            prompt=next_prompt)
                        logger.info(response)
                        additional_context.update(response[0])
                        logger.info(f'Response: {response}')
                        logger.info(
                            f'Next prompt key: {next_prompt.next_prompt_key}')
                        if response is None or next_prompt.next_prompt_key == 'TERMINATE':
                            next_prompt_key = 'TERMINATE'
                        else:
                            next_prompt_key = response[0][next_prompt.next_prompt_key]
                            logger.info(f'Next prompt key: {next_prompt_key}')
                    logger.info(f'Financial Line Item: {additional_context}')
                    yield additional_context
                i += 1
            else:
                i += 1
                yield {
                    'extract': extract,
                    'source_sentence': sentence,
                    'line_item': None
                }
        except Exception as exc:
            logger.error(exc)
            i += 1
            yield {
                'extract': extract,
                'source_sentence': sentence,
                'line_item': None,
                'error': str(exc)
            }
