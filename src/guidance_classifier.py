from bson.objectid import ObjectId
from pymongo.collection import Collection
from dotenv import find_dotenv, load_dotenv
from retry import retry

import datetime
import json
import openai
import os
import pandas as pd


load_dotenv(dotenv_path=find_dotenv(), override=True)
openai.organization = os.getenv('OPENAI_ORGANIZATION')
openai.api_key = os.getenv('OPENAI_API_KEY')

with open('prompts/guidance_prompt.json') as fp:
    topic_json = json.load(fp)
    list(topic_json)


@retry(openai.OpenAIError, tries=5, delay=1)
def response_function(message: str):
    return openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=message,
        request_timeout=20
    )


def binary_classification(collection: Collection, doc: object, use_collection: bool = False):
    raw_transcript_id = str(doc['_id'])
    company_ticker = doc['companyTicker']
    fiscal_year = doc['fiscalYear']
    fiscal_quarter = doc['fiscalQuarter']
    transcript = doc['transcript']
    print(f"Processing transcript {raw_transcript_id}")
    print(f"Company Ticker: {company_ticker}")
    print(f"Fiscal Year: {fiscal_year}")
    print(f"Fiscal Quarter: {fiscal_quarter}")

    results = []
    in_qa_section = False

    for i, excerpt in enumerate(transcript):
        # If the excerpt text is empty or whitespace
        if not excerpt['text'].strip() or excerpt['text'].strip()[-1] not in ['.', '!', '?']:
            continue
        else:

            print(f"Extracting from excerpt {i}")
            topic_json[1][
                'content'] = f'Here is the transcript paragraph for you to analyze: \'{excerpt["text"]}\''
            topic_json[2]['content'] = f'Keep in mind that the fiscal year is {fiscal_year}, the fiscal quarter is Q{fiscal_quarter} and the company is {company_ticker}'

            # Call the OpenAI API
            response = response_function(topic_json)

            # Extract the result from the API response
            # print(response)
            result = response.choices[0].message.content.strip()
            # print(result)

            if "true" in result.lower():
                excerpt["furtherProcess"] = True
                # print(f"Excerpt {i} deemed guidance. Marking for further processing.")
                if in_qa_section:
                    start_idx = len(results) - 2
                    for idx in range(start_idx, len(results)):
                        print(idx, start_idx, results[-2:])
                        results[idx][1] = True
                        print('running')
            else:
                excerpt["furtherProcess"] = False
                # print(f"Excerpt {i} deemed non-guidance. Marking for no further processing.")

        results.append([excerpt["text"], excerpt["furtherProcess"], i])

    if use_collection:
        # Update the MongoDB document regardless of the decision (yes/no/empty)
        for result in results:
            update = {
                "$set": {
                    f"transcript.{result[2]}.furtherProcess": result[1]
                }
            }
            collection.update_one({"_id": ObjectId(raw_transcript_id)}, update)

    if not use_collection:
        df = pd.DataFrame(results, columns=["Text", "FurtherProcess", 'index'])
        df.set_index('index', inplace=True)
        return df
