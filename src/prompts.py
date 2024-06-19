"""Prompt and response objects
"""
from dataclasses import dataclass
from dotenv import find_dotenv, load_dotenv
from openai import AsyncOpenAI
from retry import retry
from typing import Any, List

import asyncio
import json
import openai
import os
import uuid

from src.utils.loggers import openai_logger, reg_logger
from src.utils.mongo_utils import connect_mongo


client = connect_mongo()
db_logger = openai_logger(__name__, console=False, client=client,
                          db_name='logs', collection_name='GPTLogs')
logger = reg_logger('prompt_logger')

load_dotenv(dotenv_path=find_dotenv(), override=True)


class OpenAIResponseError(Exception):
    """Exceptions for parsing OpenAI responses
    """


def get_function_dict(filepath: str) -> List[dict]:
    """Function for parsing function dict from JSON file

    Args:
        filepath (str): Filepath to function JSON

    Returns:
        List[dict]: List of functions dictionaries
    """
    with open(filepath) as file:
        functions_json = json.load(file)
    # loop through each key, value in the dictionary and add the value to a list
    functions = [value for key, value in functions_json.items()]
    return functions


@dataclass
class Prompt:
    role: str
    content: str
    temperature: float = 0
    prescence_penalty: float = -1.5
    kwargs: dict = None
    response_type: str = 'str'
    next_prompt_key: str = None
    _response: str = None

    def __post_init__(self):
        """Fill content with kwargs
        """
        self.prompt_name = self.__class__.__name__
        if self.kwargs is not None:
            self.content = self.content.format(**self.kwargs)

    def prompt_dict(self):
        return {"role": self.role, "content": self.content}

    @property
    def response(self):
        """Get response
        """
        return self._response

    @response.setter
    def response(self, response):
        """Set response
        """
        self._response = response


@dataclass
class OpenAICompletion:
    session_id: str
    base_context: str
    prompt: Prompt
    raw_response: object

    def __post_init__(self) -> str or dict:
        """Parse Open AI chat completion response
        """
        self.session_id = str(self.session_id)
        self.chat_id = self.raw_response.id
        self.created = self.raw_response.created
        self.model = self.raw_response.model
        self.role = self.raw_response.choices[0].message.role
        self.content = self.raw_response.choices[0].message.content
        if self.raw_response.choices[0].message.function_call is not None:
            self.function_name = self.raw_response.choices[0].message.tool_calls
            self.function_arguments = self.raw_response.choices[0].message.too_calls['arguments']
        else:
            self.function_name = None
            self.function_arguments = None
        self.finish_reason = self.raw_response.choices[0].finish_reason
        self.prompt_tokens = self.raw_response.usage.prompt_tokens
        self.completion_tokens = self.raw_response.usage.completion_tokens
        self.total_tokens = self.raw_response.usage.total_tokens
        per_token_cost = {
            'gpt-4-0125-preview': {
                'input': 0.00001,
                'output': 0.00003
            },
            'gpt-4-1106-preview': {
                'input': 0.00001,
                'output': 0.00003
            },
            'gpt-4': {
                'input': 0.00003,
                'output': 0.00006
            },
            'gpt-4-0613': {
                'input': 0.00003,
                'output': 0.00006
            },
            'gpt-4-32k': {
                'input': 0.00006,
                'output': 0.00012,
            },
            'gpt-3.5-turbo': {
                'input': 0.0000015,
                'output': 0.000002,
            },
            'gpt-3.5-turbo-0613': {
                'input': 0.0000015,
                'output': 0.000002,
            },
            'gpt-3.5-turbo-16k': {
                'input': 0.000003,
                'output': 0.000004,
            }
        }
        try:
            self.cost = (self.prompt_tokens * per_token_cost[self.model]['input']) + \
                (self.completion_tokens * per_token_cost[self.model]['output'])
        except KeyError:
            self.cost = 'Unrecognized model: ' + self.model

    def to_dict(self):
        return {
            "session_id": self.session_id,
            "chat_id": self.chat_id,
            "created": self.created,
            "model": self.model,
            "base_context": self.base_context,
            "prompt": self.prompt,
            "role": self.role,
            "content": self.content,
            "function_name": self.function_name,
            "function_arguments": self.function_arguments,
            "finish_reason": self.finish_reason,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "prompt_cost": self.cost,
        }


class ChatGPTSession:
    """Chat GPT session
    """

    def __init__(
        self,
        model: str,
        termination_key: str,
        base_context: List[Prompt] = None
    ):
        self.openai_client = AsyncOpenAI(
            organization = os.getenv('OPENAI_ORGANIZATION'),
            api_key=os.getenv("OPENAI_API_KEY")
        )
        self.default_model = model
        self.termination_key = termination_key
        if base_context is None:
            self.base_context = []
        else:
            self.base_context = base_context
        self.past_prompts = []
        self.session_id = uuid.uuid4()
        self._current_prompt = None

    @property
    def current_prompt(self):
        """Current prompt property
        """
        return self._current_prompt

    @current_prompt.setter
    def current_prompt(self, prompt: Prompt):
        """Set current prompt
        """
        self._current_prompt = prompt

    def get_base_context_str(self):
        """Get base context
        """
        return '\n'.join([prompt.content for prompt in self.base_context])

    def add_base_context(self, prompt: Prompt):
        """Add prompt to base context

        Args:
            prompt (Prompt): Prompt object
        """
        self.base_context.append(prompt)

    @retry(openai.APITimeoutError, tries=5, delay=1)
    async def openai_gpt_api_call(
        self,
        prompt: Prompt,
        model: str = None,
        include_base_context: bool = True
    ) -> Any:
        """generate docstring for this function

        Args:
            model (str): OpenAI model
            prompts (Union[Prompt, List[Prompt]]): Prompt or list of prompts

        Raises:
            OpenAIResponseError: Error parsing OpenAI response

        Returns:
            str, int, float or dict: Response
        """
        if include_base_context:
            prompts = [prompt.prompt_dict() for prompt in self.base_context] + \
                [prompt.prompt_dict()]
            base_context = self.get_base_context_str()
        else:
            base_context = None
            prompts = [prompt.prompt_dict()]

        if model is None:
            model = self.default_model

        try:
            raw_response = await self.openai_client.chat.completions.create(
                model=model,
                response_format={ "type": prompt.response_type },
                messages=prompts,
                temperature=prompt.temperature,
                presence_penalty=prompt.prescence_penalty
            )

            response = OpenAICompletion(
                session_id=self.session_id,
                base_context=base_context,
                prompt=prompt.content,
                raw_response=raw_response
            )
            prompt.response = response.content
            self.past_prompts.append(prompt)
        except Exception as exc:
            raise exc
        # db_logger.info(response.to_dict())
        return self.process_response(prompt, response)

    def gpt_function_call(
        self,
        prompt: Prompt,
        avail_functions: List[dict],
        function_call: str = "auto",
        include_base_context: bool = True,
        model: str = None
    ) -> Any:
        """
        """
        if include_base_context:
            prompts = [prompt.prompt_dict() for prompt in self.base_context] + \
                [prompt.prompt_dict()]
            base_context = self.get_base_context_str()
        else:
            base_context = None
            prompts = [prompt.prompt_dict()]

        try:
            if model is None:
                model = self.default_model

            raw_response = openai.ChatCompletion.create(
                model=model,
                messages=prompts,
                functions=avail_functions,
                function_call={"name": function_call},
                temperature=prompt.temperature,
                presence_penalty=prompt.prescence_penalty
            )

            response = raw_response
        except Exception as exc:
            raise exc
        return response

    def process_response(
        self,
        prompt: Prompt,
        response: OpenAICompletion
    ) -> Any:
        """Process OpenAIResponse object

        Args:
            prompt (Prompt): The prompt submitted
            response (OpenAIResponse): OpenAI chat completion response

        Raises:
            OpenAIResponseError: Error parsing OpenAIResponse
            exc: General exception
            ValueError: The prompt doesn't have a valid response type.

        Returns:
            _type_: processed response in the given type
        """
        if response.content == self.termination_key:
            return None
        if prompt.response_type == 'text':
            return response.content
        elif prompt.response_type == 'json_object':
            try:
                parsed_response = json.loads(response.content)
                return parsed_response
            except Exception as exc:
                raise OpenAIResponseError(f"Could not parse JSON: {response.content}\n{exc}")
        elif prompt.response_type == 'list':
            response_list = response.content.split('|')
            final_response = [x.strip() for x in response_list]
            return final_response
        elif prompt.response_type == 'float':
            return float(response.content)
        elif prompt.response_type == 'int':
            return int(response.content)
        else:
            raise ValueError("Invalid response type")

    @retry(tries=3)
    async def get_embedding(self, text, model="text-embedding-3-small"):
        text = text.replace("\n", " ")
        response = await self.openai_client.embeddings.create(input=[text], model=model)
        return response
