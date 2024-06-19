from dotenv import find_dotenv, load_dotenv
from langchain.text_splitter import CharacterTextSplitter
from src.prompts import ChatGPTSession, Prompt
from src.utils.loggers import reg_logger
from src.utils.mongo_utils import connect_mongo, get_data_from_collection, insert_data_into_collection
from typing import List, Dict

import asyncio
import datetime
import json
import re


logger = reg_logger('process_transcript')

load_dotenv(dotenv_path=find_dotenv(), override=True)


def metrics_parser(metrics):
    metrics = metrics.replace("\n", " ")
    logger.debug(f'original metrics {metrics}')

    parsing_pattern = '(rawPeriod|rawLow|rawHigh|rawUnit|rawScale|metricType):\s*\-?([A-Za-z0-9]+)'
    parsed_metrics = re.findall(
        parsing_pattern,
        metrics
    )
    logger.debug(f"parsed metrics to {parsed_metrics}")

    parsed_metrics = {metric: value.strip()
                      for metric, value in parsed_metrics}
    return parsed_metrics


def split_transcript(raw_transcript_doc: Dict) -> List:
    """Split transcript into chunks

    Args:
        raw_transcript_doc (Dict): Raw transcript

    Returns:
        List: List of transcript chunks
    """
    transcript = [x['text']
                  for x in raw_transcript_doc['transcript'] if x not in [None, '', '\n']]

    transcript = ' \n '.join(transcript)
    transcript = re.sub(r'(\\n\s+)+', '\n', transcript)

    splitter = CharacterTextSplitter(
        separator=' \n ',
        chunk_size=1000,
        chunk_overlap=100
    )
    transcriptDocument = splitter.create_documents([transcript])
    return transcriptDocument


async def process_transcript(mongo_client, raw_transcript_doc: Dict) -> None:
    """Process raw transcript into staging

    Args:
        raw_transcript (Dict): Raw transcript
    """
    companyName = raw_transcript_doc['companyName']
    companyTicker = raw_transcript_doc['companyTicker']
    Year = raw_transcript_doc['fiscalYear']
    Quarter = raw_transcript_doc['fiscalQuarter']
    nextYear = Year + 1
    nextQuarter = 1 if Quarter == 4 else Quarter + 1
    QuarterYear = "Q" + str(Quarter) + "Y" + str(Year)
    nextQuarterYear = "Q" + str(nextQuarter) + "Y" + str(Year)
    priorQuarterYear = "Q" + str(Quarter) + "Y" + str(Year - 1)

    logger.info(f"Company Ticker: {companyName}")
    logger.info(f"Fiscal Year: {Year}")
    logger.info(f"Fiscal Quarter: {Quarter}")

    excerpt_count = 0

    with open('prompts/extraction_prompt.json') as file:
        extraction_prompt_json = json.load(file)
        extraction_prompt_json = extraction_prompt_json['extract_line_items']

    with open('prompts/qa_prompts.json') as file:
        qa_prompt_json = json.load(file)
        qa_prompt_json_one = qa_prompt_json['qa_one']
        qa_prompt_json_two = qa_prompt_json['qa_two']

    gpt_session = ChatGPTSession(
        model='gpt-4-1106-preview',
        termination_key='TERMINATE'
    )

    error_positions = []
    staging_line_items = []
    tasks = []

    for excerpt in split_transcript(raw_transcript_doc)[4:]:
        excerpt_count += 1
        if len(excerpt.page_content) > 20:
            guidance_prompt = Prompt(
                role=extraction_prompt_json['role'],
                content=extraction_prompt_json['content'],
                temperature=extraction_prompt_json['temperature'],
                prescence_penalty=-1,
                kwargs={
                    'companyName': companyName,
                    'Year': Year,
                    'Quarter': Quarter,
                    'nextYear': nextYear,
                    'nextQuarter': nextQuarter,
                    'excerpt': excerpt.page_content,
                    'QuarterYear': QuarterYear,
                    'nextQuarterYear': nextQuarterYear,
                    'priorQuarterYear': priorQuarterYear
                },
                response_type=extraction_prompt_json['response_type']
            )
            response = asyncio.run(gpt_session.openai_gpt_api_call(
                prompt=guidance_prompt,
                model='gpt-4-1106-preview'
            ))
            logger.debug(response)
            try:
                line_items = response['lineItems']
            except:
                continue
            try:
                for line in line_items:
                    qa_one_prompt = Prompt(
                        role=qa_prompt_json_one['role'],
                        content=qa_prompt_json_one['content'],
                        temperature=qa_prompt_json_one['temperature'],
                        prescence_penalty=-1,
                        kwargs={
                            'metric_name': line['rawLineItem'],
                            'rawTranscriptSentence': line['rawTranscriptSourceSentence']
                        },
                        response_type=qa_prompt_json_one['response_type']
                    )
                    response = asyncio.run(gpt_session.openai_gpt_api_call(
                        prompt=qa_one_prompt,
                        model='gpt-4'
                    ))
                    new_line_item = response
                    logger.debug(f"{line['rawLineItem']} to {new_line_item}")
                    embedding = asyncio.run(
                        gpt_session.get_embedding(new_line_item))

                    qa_two_prompt = Prompt(
                        role=qa_prompt_json_two['role'],
                        content=qa_prompt_json_two['content'],
                        temperature=qa_prompt_json_two['temperature'],
                        prescence_penalty=-1,
                        kwargs={
                            'rawPeriod': line['rawPeriod'],
                            'rawLow': str(line['rawLow']),
                            'rawHigh': str(line['rawHigh']),
                            'rawUnit': line['rawUnit'],
                            'rawScale': line['rawScale'],
                            'metricType': line['metricType'],
                            'rawTranscriptSourceSentence': line['rawTranscriptSourceSentence'],
                            'QuarterYear': QuarterYear,
                            'priorQuarterYear': priorQuarterYear,
                            'Year': Year,
                            'nextQuarterYear': nextQuarterYear
                        },
                        response_type=qa_prompt_json_two['response_type']
                    )
                    response = asyncio.run(gpt_session.openai_gpt_api_call(
                        prompt=qa_two_prompt,
                        model='gpt-4-1106-preview'
                    ))
                    corrected_metrics = response
                    parsed_metrics = metrics_parser(corrected_metrics)

                    staging_line_item = {
                        'rawLineItem': new_line_item,
                        'rawPeriod': parsed_metrics['rawPeriod'],
                        'rawLow': str(parsed_metrics['rawLow']),
                        'rawHigh': str(parsed_metrics['rawHigh']),
                        'rawUnit': parsed_metrics['rawUnit'],
                        'rawScale': parsed_metrics['rawScale'],
                        'metricType': parsed_metrics['metricType'],
                        'rawTranscriptParagraph': excerpt.page_content,
                        'rawTranscriptSourceSentence': line['rawTranscriptSourceSentence'],
                        'transcriptPosition': excerpt_count,
                        'rawLineItemEmbedding': embedding.data[0].embedding
                    }
                    logger.debug(
                        {k: v for k, v in staging_line_item.items() if k != 'rawLineItemEmbedding'})
                    staging_line_items.append(staging_line_item)

            except Exception as exc:
                logger.error(exc)
                error_positions.append((excerpt_count, exc))
                continue

    staging_line_item_doc = {
        'companyName': companyName,
        'companyTicker': companyTicker,
        'fiscalYear': Year,
        'fiscalQuarter': Quarter,
        'rawTranscriptId': raw_transcript_doc['_id'],
        'sessionId': gpt_session.session_id,
        'stagingLineItems': staging_line_items,
        'createdAt': datetime.datetime.now().isoformat(),
        'updatedAt': datetime.datetime.now().isoformat(),
        'processingStage': 'processing'
    }
    staging_id = insert_data_into_collection(
        mongo_client,
        'transcripts',
        'stagingTranscripts',
        **staging_line_item_doc
    )
    logger.info(
        f"Inserted staging line items into collection stagingTranscripts: "
        f"{staging_id}"
    )
    if len(error_positions) > 0:
        logger.error(f"Error positions: {error_positions}")
    return staging_id


async def async_process_excerpt(excerpt, gpt_session: ChatGPTSession, extraction_prompt_json, qa_prompt_json_one, qa_prompt_json_two, companyName, thisYear, thisQuarter, nextYear, nextQuarter, excerpt_count, error_positions, priorYearQuarter):
    thisQuarterYear = "Q" + str(thisQuarter) + ", Y" + str(thisYear)
    nextQuarterYear = "Q" + str(nextQuarter) + ", Y" + str(nextYear)
    staging_line_items = []
    if len(excerpt.page_content) > 20:
        guidance_prompt = Prompt(
            role=extraction_prompt_json['role'],
            content=extraction_prompt_json['content'],
            temperature=extraction_prompt_json['temperature'],
            prescence_penalty=-1,
            kwargs={
                'companyName': companyName,
                'thisYear': thisYear,
                'thisQuarter': thisQuarter,
                'nextYear': nextYear,
                'nextQuarter': nextQuarter,
                'excerpt': excerpt.page_content,
                'priorYearQuarter': priorYearQuarter,
                'thisQuarterYear': thisQuarterYear
            },
            response_type=extraction_prompt_json['response_type']
        )
        response = await gpt_session.openai_gpt_api_call(
            prompt=guidance_prompt,
            model='gpt-4-1106-preview'
        )
        logger.debug(response)
        try:
            line_items = response['lineItems']
        except:
            logger.error("No line items")
            return []
        qa = 1
        try:
            for line in line_items:
                qa = 1
                qa_one_prompt = Prompt(
                    role=qa_prompt_json_one['role'],
                    content=qa_prompt_json_one['content'],
                    temperature=qa_prompt_json_one['temperature'],
                    prescence_penalty=-1,
                    kwargs={
                        'metric_name': line['rawLineItem'],
                    },
                    response_type=qa_prompt_json_one['response_type']
                )
                response = await gpt_session.openai_gpt_api_call(
                    prompt=qa_one_prompt,
                    model='gpt-4'
                )
                new_line_item = response
                logger.debug(f"{line['rawLineItem']} to {new_line_item}")
                embedding = await gpt_session.get_embedding(new_line_item)

                qa = 2

                qa_two_prompt = Prompt(
                    role=qa_prompt_json_two['role'],
                    content=qa_prompt_json_two['content'],
                    temperature=qa_prompt_json_two['temperature'],
                    prescence_penalty=-1,
                    kwargs={
                        'rawPeriod': line['rawPeriod'],
                        'rawLow': str(line['rawLow']),
                        'rawHigh': str(line['rawHigh']),
                        'rawUnit': line['rawUnit'],
                        'rawScale': line['rawScale'],
                        'metricType': line['metricType'],
                        'rawTranscriptParagraph': excerpt.page_content,
                        'rawTranscriptSourceSentence': line['rawTranscriptSourceSentence'],
                        'thisQuarterYear': thisQuarterYear,
                        'priorYearQuarter': priorYearQuarter,
                        'thisYear': thisYear,
                        'nextQuarterYear': nextQuarterYear
                    },
                    response_type=qa_prompt_json_two['response_type']
                )
                response = await gpt_session.openai_gpt_api_call(
                    prompt=qa_two_prompt,
                    model='gpt-4-1106-preview'
                )
                corrected_metrics = response
                logger.debug(f"corrected metrics to {corrected_metrics}")

                staging_line_item = {
                    'rawLineItem': new_line_item,
                    'rawPeriod': corrected_metrics['rawPeriod'],
                    'rawLow': str(corrected_metrics['rawLow']),
                    'rawHigh': str(corrected_metrics['rawHigh']),
                    'rawUnit': corrected_metrics['rawUnit'],
                    'rawScale': corrected_metrics['rawScale'],
                    'metricType': corrected_metrics['metricType'],
                    'rawTranscriptParagraph': excerpt.page_content,
                    'rawTranscriptSourceSentence': corrected_metrics['rawTranscriptSourceSentence'],
                    'transcriptPosition': excerpt_count,
                    'rawLineItemEmbedding': embedding.data[0].embedding
                }
                logger.debug(
                    {k: v for k, v in staging_line_item.items() if k != 'rawLineItemEmbedding'})
                staging_line_items.append(staging_line_item)
        except Exception as exc:
            logger.error(f"error during qa {qa}", exc)
            error_positions.append((excerpt_count, exc))

    return staging_line_items


async def run_transcript_processor(ticker: str, fiscal_year: int, fiscal_quarter: int) -> None:
    """Main function"""
    monngo_client = connect_mongo()
    documents = get_data_from_collection(
        monngo_client,
        'transcripts',
        'rawTranscripts',
        projection={},
        query={'companyTicker': ticker, 'fiscalYear': fiscal_year,
               'fiscalQuarter': fiscal_quarter}
    )
    staging_id = await process_transcript(monngo_client, documents[0])
    monngo_client.close()
    return staging_id


if __name__ == '__main__':
    transcripts = [('IBM', 2022, 4)]
    for x in transcripts:
        asyncio.run(run_transcript_processor(x[0], x[1], x[2]))
