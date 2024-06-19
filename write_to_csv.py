from datetime import datetime
from src.utils.mongo_utils import connect_mongo, get_data_from_collection
from dotenv import find_dotenv, load_dotenv
from src.prompts import ChatGPTSession, Prompt, get_function_dict
from typing import List, Dict
from bson import ObjectId

import json
import openai
import os
import pandas as pd


load_dotenv(dotenv_path=find_dotenv(), override=True)
openai.organization = os.getenv('OPENAI_ORGANIZATION')
openai.api_key = os.getenv('OPENAI_API_KEY')

monngo_conn = connect_mongo()


for ticker in ['IBM']:

    documents = get_data_from_collection(
        monngo_conn,
        'transcripts',
        'stagingTranscripts',
        projection={},
        query={
            '_id': ObjectId('65dfc3d612042088816180c3')
        }
    )
    print(len(documents))

    for doc in documents:
        rawTranscriptId_list = []
        sessionId_list = []
        rawLineItem_list = []
        rawPeriod_list = []
        rawLow_list = []
        rawHigh_list = []
        rawUnit_list = []
        rawScale_list = []
        metricType_list = []
        rawTranscriptSourceSentence_list = []
        rawTranscriptParagraph_list = []
        transcriptPosition_list = []
        createdAt_list = []

        raw_transcript = get_data_from_collection(
            monngo_conn,
            'transcripts',
            'rawTranscripts',
            projection={},
            query={'_id': doc['rawTranscriptId']}
        )
        for line_item in doc['stagingLineItems']:
            # Try to append each item but if it fails, append an empty string
            try:
                rawTranscriptId_list.append(doc['rawTranscriptId'])
            except:
                rawTranscriptId_list.append(None)
            try:
                sessionId_list.append(doc['sessionId'])
            except:
                sessionId_list.append(None)
            try:
                rawLineItem_list.append(line_item['rawLineItem'])
            except:
                rawLineItem_list.append(None)
            try:
                rawPeriod_list.append(line_item['rawPeriod'])
            except:
                rawPeriod_list.append(None)
            try:
                rawLow_list.append(line_item['rawLow'])
            except:
                rawLow_list.append(None)
            try:
                rawHigh_list.append(line_item['rawHigh'])
            except:
                rawHigh_list.append(None)
            try:
                rawUnit_list.append(line_item['rawUnit'])
            except:
                rawUnit_list.append(None)
            try:
                rawScale_list.append(line_item['rawScale'])
            except:
                rawScale_list.append(None)
            try:
                metricType_list.append(line_item['metricType'])
            except:
                metricType_list.append(None)
            try:
                rawTranscriptSourceSentence_list.append(
                    line_item['rawTranscriptSourceSentence'])
            except:
                rawTranscriptSourceSentence_list.append(None)
            try:
                rawTranscriptParagraph_list.append(
                    line_item['rawTranscriptParagraph'])
            except:
                rawTranscriptParagraph_list.append(None)
            try:
                transcriptPosition_list.append(
                    f"From {line_item['transcriptPosition']['from']} to {line_item['transcriptPosition']['to']}")
            except:
                transcriptPosition_list.append(None)
            createdAt_list.append(doc['createdAt'])

        transcript_df = pd.DataFrame({
            'rawTranscriptId': rawTranscriptId_list,
            'sessionId': sessionId_list,
            'rawLineItem': rawLineItem_list,
            'rawPeriod': rawPeriod_list,
            'rawLow': rawLow_list,
            'rawHigh': rawHigh_list,
            'rawUnit': rawUnit_list,
            'rawScale': rawScale_list,
            'metricType': metricType_list,
            'rawTranscriptSourceSentence': rawTranscriptSourceSentence_list,
            'rawTranscriptParagraph': rawTranscriptParagraph_list,
            'transcriptPosition': transcriptPosition_list,
            'createdAt': createdAt_list
        })
        transcript_df.to_csv(
            f"{ticker}_Q{raw_transcript[0]['fiscalQuarter']}_{raw_transcript[0]['fiscalYear']}_{doc['createdAt']}.csv", index=False)
