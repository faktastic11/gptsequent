from utils.mongo_utils import connect_mongo, get_data_from_collection
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv, find_dotenv
from prompts import ChatGPTSession

load_dotenv(dotenv_path=find_dotenv(), override=True)

monngo_conn = connect_mongo()


def read_csv(file_path) -> pd.DataFrame:
    return pd.read_csv(file_path)


def convert_string_to_array(str_array) -> np.ndarray:
    # Assuming each string in str_array is a representation of a numeric array
    return np.array([np.array(vec) for vec in str_array])


def calculate_similarity(vectors_1, vectors_2) -> np.ndarray:
    # Convert to numpy arrays if not already
    if isinstance(vectors_1, list) or isinstance(vectors_1, pd.Series):
        vectors_1 = convert_string_to_array(vectors_1)
    if isinstance(vectors_2, list) or isinstance(vectors_2, pd.Series):
        vectors_2 = convert_string_to_array(vectors_2)

    # Check if vectors are of the same length
    if vectors_1.shape[1] != vectors_2.shape[1]:
        raise ValueError("Embeddings must be of the same length")

    similarity_matrix = cosine_similarity(vectors_1, vectors_2)
    return similarity_matrix


def compare_dataframes(df_processed, df_test, embedding_column, similarity_threshold=0.98) -> tuple[list, list, list]:
    if embedding_column not in df_processed.columns:
        df_processed[embedding_column] = df_processed['lineItem'].apply(
            lambda x: ChatGPTSession.get_embedding(text=x))
    if embedding_column not in df_test.columns:
        df_test[embedding_column] = df_test['lineItem'].apply(
            lambda x: ChatGPTSession.get_embedding(text=x))
    similarity_matrix = calculate_similarity(
        df_processed[embedding_column], df_test[embedding_column])

    print(similarity_matrix.shape)

    matches = []
    misses = []
    points_of_interest = []

    max_similarities = np.max(similarity_matrix, axis=1)
    max_indices = np.argmax(similarity_matrix, axis=1)

    for i, (max_similarity, max_index) in enumerate(zip(max_similarities, max_indices)):
        if max_similarity >= similarity_threshold:
            processed_row = df_processed.iloc[i]
            test_row = df_test.iloc[max_index]
            matches.append((processed_row, test_row))
        else:
            processed_row = df_processed.iloc[i]
            misses.append(processed_row)

    matched_test_indices = {test_row.name for _, test_row in matches}
    for i, test_row in df_test.iterrows():
        if i not in matched_test_indices:
            points_of_interest.append(test_row)

    return matches, misses, points_of_interest


def compare_specific_columns(processed_row, test_row):
    column_mappings = {
        'matchedLow': (['value', 'raw', 'low'], 'rawLow'),
        'matchedHigh': (['value', 'raw', 'high'], 'rawHigh'),
        'matchedUnit': (['value', 'raw', 'unit'], 'rawUnit'),
        'matchedScale': (['value', 'raw', 'scale'], 'rawScale'),
        'matchedPeriod': (['guidancePeriod', 'raw'], 'rawPeriod'),
        'matchedMetricType': ('metrictype', 'metrictype')
    }
    comparison_results = {}
    for key, (processed_keys, test_col) in column_mappings.items():
        if isinstance(processed_keys, list):  # For nested dictionary keys in processed_row
            processed_val = processed_row
            for k in processed_keys:
                processed_val = processed_val.get(k, None)
        else:  # For direct key in processed_row
            processed_val = processed_row.get(processed_keys, None)
        test_val = test_row.get(test_col, None)
        comparison_results[key] = processed_val == test_val
    return comparison_results

# Example usage


def compare_chats(ticker: str, processedQuarter: int, processedYear: int, staging: str):
    processed_doc_lines = get_data_from_collection(
        monngo_conn,
        'transcripts',
        'processedTranscripts',
        projection={},
        query={'transcriptPeriod.fiscalYear': int(processedYear),
               'transcriptPeriod.fiscalQuarter': int(processedQuarter),
               'companyName': ticker}
    )
    processed_doc = pd.DataFrame(processed_doc_lines)

    staging_doc_lines = get_data_from_collection(
        monngo_conn,
        'transcripts',
        'stagingTranscripts',
        projection={},
        query={'sessionId': staging}
    )
    staging_doc = pd.DataFrame(staging_doc_lines)
    if len(staging_doc) == 1 and 'stagingLineItems' in staging_doc.columns:
        df_exploded = staging_doc.explode(
            'stagingLineItems').drop(["_id"], axis=1)
        line_items_df = pd.json_normalize(df_exploded['stagingLineItems'])
        df_exploded = df_exploded.reset_index(drop=True)
        staging_doc = pd.concat(
            [df_exploded.drop(columns=['stagingLineItems']), line_items_df], axis=1)

    if len(staging_doc) == 0 or len(processed_doc) == 0:
        print('ERROR! theres no comparison data! aborting...')
        return

    matches, misses, points_of_interest = compare_dataframes(
        processed_doc, staging_doc, 'rawLineItemEmbedding')
    comparison_results = []

    for processed_row, test_row in matches:
        column_comparisons = compare_specific_columns(processed_row, test_row)
        comparison_results.append({
            'Processed Line Item': processed_row['lineItem'],
            'Processed Raw Transcript Source Sentence': processed_row['rawTranscriptSourceSentence'],
            'Test Line Item': test_row['rawLineItem'],
            'Test Raw Transcript Source Sentence': test_row['rawTranscriptSourceSentence'],
            **column_comparisons
        })

    matches_df = pd.DataFrame(comparison_results)
    misses_df = pd.DataFrame(misses)
    points_of_interest_df = pd.DataFrame(points_of_interest)

    # Save to Excel
    output_file = f'{processed_doc_lines[0]["companyName"]}_Q{processed_doc_lines[0]["transcriptPeriod"]["fiscalQuarter"]}_comparison_results.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        matches_df.to_excel(writer, sheet_name='Matches', index=False)
        misses_df.to_excel(writer, sheet_name='Misses', index=False)
        points_of_interest_df.to_excel(
            writer, sheet_name='Points of Interest', index=False)


compare_chats(ticker='NKE', processedQuarter=2, processedYear=2023,
              staging='de263777-580b-4586-8b8f-876c987607ee')
