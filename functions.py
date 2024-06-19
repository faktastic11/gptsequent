import tiktoken
import json
import datetime
import os

ITER = os.getenv('ITER', 'JSON')
MODEL = os.getenv('MODEL', 'gpt-3.5-turbo')

def create_message(prompt: str, system: str, user_context: str = None, asst_context: str = None) -> list:
    """
    Creates a message list with the given prompt, system message, and optional user and assistant context.

    Args:
        prompt (str): The user prompt.
        system (str): The system message.
        user_context (str, optional): Optional user context. Defaults to None.
        asst_context (str, optional): Optional assistant context. Defaults to None.

    Returns:
        list: The generated message list.
    """
    message = []
    message.append({"role": "system", "content": system})
    if asst_context:
        message.append({"role": "user", "content": prompt})
        message.append({"role": "assistant", "content": asst_context})
        message.append(
            {"role": "user",
             "content": "That's perfect, now please do the following in the exact same style as the last" + prompt
             }
        )
        return message
    message.append({"role": "user", "content": prompt})
    return message


def read_json_prompt(json_prompt, updates):
    """
    Reads the JSON prompt, updates the specified indices with new content, and returns the modified messages list.

    Args:
        json_prompt (str): The JSON prompt file path.
        updates (list): A list of tuples where each tuple contains a string and an index.

    Returns:
        list: The modified messages list with updated content.
    """
    with open(json_prompt) as file:
        messages = json.load(file)

    for content, index in updates:
        messages[index]["content"] = content

    return messages


def count_tokens(model: str, text: str) -> None:
    """
    Counts the number of tokens in the given text using the specified model.

    Args:
        model (str): The name or version ID of the model.
        text (str): The input text to count tokens for.

    Returns:
        None
    """
    enc = tiktoken.encoding_for_model(model)
    tokens = enc.encode(text)
    print(len(tokens))


def num_tokens_from_messages(messages: list, model: str = "gpt-3.5-turbo-0613") -> int:
    """
    Calculates the number of tokens used by a list of messages.

    Args:
        messages (list): The list of messages to calculate tokens for.
        model (str, optional): The name or version ID of the model. Defaults to "gpt-3.5-turbo-0613".

    Returns:
        int: The total number of tokens used by the messages.
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    if model in {
        "gpt-3.5-turbo-0613",
        "gpt-3.5-turbo-16k-0613",
        "gpt-4-0314",
        "gpt-4-32k-0314",
        "gpt-4-0613",
        "gpt-4-32k-0613",
    }:
        tokens_per_message = 3
        tokens_per_name = 1
    elif model == "gpt-3.5-turbo-0301":
        # every message follows {role/name}\n{content}\n
        tokens_per_message = 4
        tokens_per_name = -1  # if there's a name, the role is omitted
    elif "gpt-3.5-turbo" in model:
        print("Warning: gpt-3.5-turbo may update over time. Returning num tokens assuming gpt-3.5-turbo-0613.")
        return num_tokens_from_messages(messages, model="gpt-3.5-turbo-0613")
    elif "gpt-4" in model:
        print(
            "Warning: gpt-4 may update over time. Returning num tokens assuming gpt-4-0613.")
        return num_tokens_from_messages(messages, model="gpt-4-0613")
    else:
        raise NotImplementedError(
            f"""num_tokens_from_messages() is not implemented for model {model}. See https://github.com/openai/openai-python/blob/main/chatml.md for information on how messages are converted to tokens."""
        )
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with assistant
    return num_tokens

def save_response(response: list | str, folder: str) -> None:
    """
    Logs the given response into a file with a unique name based on the model, iteration, and current date/time.

    Args:
        response (str): The response to log.
        MODEL (str): The model name.
        ITER (int): The iteration number.

    Returns:
        None
    """
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = "./" + folder

    # Check if the logs directory exists, if not, create it
    if not os.path.isdir(path):
        os.mkdir(path)

    filename = f"{folder}_{MODEL}_{current_datetime}.txt"
    filepath = os.path.join(path, filename)

    if type(response) == list:
        with open(filepath, 'w') as f:
            for data in response:
                # remove empty lines from the response and remove inconsistent headings
                f.write(data)
    else:
        with open(filepath, "w") as file:
            file.write(response)
    
    print(f"Response logged in {filepath}")


def process_line(line: str) -> str:
    if "\xa0" in line or line.startswith('*'):
        return ''
    
    # Check if string is a sentence or a header
    if line.endswith('.') or line.endswith('?') or line.endswith('!'):
        # This is a sentence, add it to the accumulator
        return line
    else:
        return '\n'+line+'\n'

def process_transcript_list(transcript_lines: list[str], chunk_size: int):
    chunks = []
    current_chunk = ""

    for line in transcript_lines:
        processed_line = process_line(line)
        # Checking if adding the new line would exceed the chunk size
        if len(current_chunk) + len(processed_line) + 1 > chunk_size:  # +1 for the space between sentences
            # If it would, add the current chunk to the list of chunks
            chunks.append(current_chunk)
            print(f'text chunk size: {len(current_chunk)}')
            # And start a new chunk with the processed line
            current_chunk = processed_line
        else:
            # If not, add the processed line to the current chunk
            # If current_chunk is not empty, add a space before adding the new line
            current_chunk += (" " + processed_line) if current_chunk else processed_line

    # Adding the final chunk to the list of chunks
    if current_chunk:
        chunks.append(current_chunk)

    return chunks
