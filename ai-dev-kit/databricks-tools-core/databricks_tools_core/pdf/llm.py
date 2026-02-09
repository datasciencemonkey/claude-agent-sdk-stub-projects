"""LiteLLM-based model configuration for PDF generation.

Simple abstraction over LiteLLM that auto-configures from environment variables.

For Azure OpenAI, set:
  - AZURE_API_KEY (or AZURE_OPENAI_API_KEY)
  - AZURE_API_BASE (or AZURE_OPENAI_ENDPOINT)
  - AZURE_API_VERSION (or AZURE_OPENAI_API_VERSION)
  - AZURE_OPENAI_DEPLOYMENT (default model)
  - AZURE_OPENAI_DEPLOYMENT_MINI (mini model)

For Databricks, set:
  - DATABRICKS_TOKEN (or DATABRICKS_API_KEY)
  - DATABRICKS_HOST (auto-builds API_BASE)
  - DATABRICKS_MODEL (default model endpoint name)
  - DATABRICKS_MODEL_MINI (mini model endpoint)

Set LLM_PROVIDER to "AZURE" or "DATABRICKS" (default: DATABRICKS)
"""

import asyncio
import json
import logging
import os
from typing import Any, Optional, Union

import litellm
from pydantic import BaseModel

# Automatically drop unsupported params (e.g., temperature for reasoning models)
litellm.drop_params = True

logger = logging.getLogger(__name__)

# LLM Provider constants
LLM_PROVIDER_AZURE = 'AZURE'
LLM_PROVIDER_DATABRICKS = 'DATABRICKS'


class LLMConfigurationError(Exception):
    """Raised when LLM is not properly configured."""

    pass


def _validate_llm_configuration() -> None:
    """Validate that LLM is properly configured.

    Raises:
        LLMConfigurationError: If required environment variables are missing.
    """
    provider = _get_provider()

    if provider == LLM_PROVIDER_DATABRICKS:
        # Check for Databricks configuration
        has_token = bool(os.getenv('DATABRICKS_TOKEN') or os.getenv('DATABRICKS_API_KEY'))
        has_host = bool(os.getenv('DATABRICKS_HOST') or os.getenv('DATABRICKS_API_BASE'))
        has_model = bool(os.getenv('DATABRICKS_MODEL'))

        missing = []
        if not has_token:
            missing.append('DATABRICKS_TOKEN')
        if not has_host:
            missing.append('DATABRICKS_HOST')
        if not has_model:
            missing.append('DATABRICKS_MODEL')

        if missing:
            raise LLMConfigurationError(
                f"Databricks LLM not configured. Missing environment variables: {', '.join(missing)}.\n"
                f"Set these variables or switch to Azure with LLM_PROVIDER=AZURE.\n\n"
                f"Required for Databricks:\n"
                f"  - DATABRICKS_TOKEN: Your Databricks personal access token\n"
                f"  - DATABRICKS_HOST: Your workspace URL (e.g., https://my-workspace.cloud.databricks.com)\n"
                f"  - DATABRICKS_MODEL: Model serving endpoint name (e.g., databricks-meta-llama-3-3-70b-instruct)"
            )

    elif provider == LLM_PROVIDER_AZURE:
        # Check for Azure configuration
        has_key = bool(os.getenv('AZURE_API_KEY') or os.getenv('AZURE_OPENAI_API_KEY'))
        has_endpoint = bool(os.getenv('AZURE_API_BASE') or os.getenv('AZURE_OPENAI_ENDPOINT'))
        has_deployment = bool(os.getenv('AZURE_OPENAI_DEPLOYMENT'))

        missing = []
        if not has_key:
            missing.append('AZURE_OPENAI_API_KEY')
        if not has_endpoint:
            missing.append('AZURE_OPENAI_ENDPOINT')
        if not has_deployment:
            missing.append('AZURE_OPENAI_DEPLOYMENT')

        if missing:
            raise LLMConfigurationError(
                f"Azure OpenAI not configured. Missing environment variables: {', '.join(missing)}.\n"
                f"Set these variables or switch to Databricks with LLM_PROVIDER=DATABRICKS.\n\n"
                f"Required for Azure OpenAI:\n"
                f"  - AZURE_OPENAI_API_KEY: Your Azure OpenAI API key\n"
                f"  - AZURE_OPENAI_ENDPOINT: Your Azure endpoint (e.g., https://my-resource.openai.azure.com/)\n"
                f"  - AZURE_OPENAI_DEPLOYMENT: Deployment name (e.g., gpt-4o)"
            )
    else:
        raise LLMConfigurationError(
            f"Unknown LLM provider: '{provider}'. "
            f"Set LLM_PROVIDER to 'DATABRICKS' or 'AZURE'."
        )


def _setup_env():
    """Map legacy env vars to LiteLLM expected names if needed."""
    # Azure: map AZURE_OPENAI_* to AZURE_*
    if os.getenv('AZURE_OPENAI_API_KEY') and not os.getenv('AZURE_API_KEY'):
        os.environ['AZURE_API_KEY'] = os.getenv('AZURE_OPENAI_API_KEY')
    if os.getenv('AZURE_OPENAI_ENDPOINT') and not os.getenv('AZURE_API_BASE'):
        os.environ['AZURE_API_BASE'] = os.getenv('AZURE_OPENAI_ENDPOINT')
    if os.getenv('AZURE_OPENAI_API_VERSION') and not os.getenv('AZURE_API_VERSION'):
        os.environ['AZURE_API_VERSION'] = os.getenv('AZURE_OPENAI_API_VERSION', '2024-05-01-preview')

    # Databricks: map DATABRICKS_TOKEN to DATABRICKS_API_KEY, build API_BASE from HOST
    if os.getenv('DATABRICKS_TOKEN') and not os.getenv('DATABRICKS_API_KEY'):
        os.environ['DATABRICKS_API_KEY'] = os.getenv('DATABRICKS_TOKEN')
    if os.getenv('DATABRICKS_HOST') and not os.getenv('DATABRICKS_API_BASE'):
        host = os.getenv('DATABRICKS_HOST').rstrip('/')
        os.environ['DATABRICKS_API_BASE'] = f'{host}/serving-endpoints'


# Run setup on module load
_setup_env()


def _get_provider() -> str:
    """Get the configured LLM provider from environment."""
    return os.getenv('LLM_PROVIDER', LLM_PROVIDER_DATABRICKS).upper()


def _get_model_name(mini: bool = False, model_name: Optional[str] = None) -> str:
    """Get the LiteLLM model name with appropriate prefix.

    Args:
        mini: Use smaller/faster model
        model_name: Override model name

    Returns:
        LiteLLM model string (e.g., 'azure/gpt-4o', 'databricks/my-endpoint')
    """
    provider = _get_provider()

    if provider == LLM_PROVIDER_DATABRICKS:
        if model_name:
            deployment = model_name
        elif mini:
            deployment = os.getenv(
                'DATABRICKS_MODEL_MINI',
                os.getenv('DATABRICKS_MODEL', 'databricks-meta-llama-3-3-70b-instruct'),
            )
        else:
            deployment = os.getenv('DATABRICKS_MODEL', 'databricks-meta-llama-3-3-70b-instruct')
        return f'databricks/{deployment}'

    # Azure provider
    if model_name:
        deployment = model_name
    elif mini:
        deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT_MINI', 'gpt-4o-mini')
    else:
        deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT', 'gpt-4o')

    return f'azure/{deployment}'


async def call_llm(
    prompt: str,
    system_prompt: Optional[str] = None,
    mini: bool = False,
    max_tokens: int = 4000,
    temperature: float = 1.0,
    response_format: Optional[Union[str, dict[str, Any], type[BaseModel]]] = None,
    model_name: Optional[str] = None,
    timeout: float = 300.0,
) -> str:
    """Call LLM model asynchronously.

    Args:
        prompt: User prompt
        system_prompt: Optional system prompt
        mini: Use smaller/faster model
        max_tokens: Maximum tokens in response
        temperature: Model temperature (default: 1.0)
        response_format: Response format - 'json_object', dict schema, or Pydantic model
        model_name: Override model name
        timeout: Request timeout in seconds (default: 300)

    Returns:
        Generated content string

    Raises:
        LLMConfigurationError: If LLM is not properly configured
    """
    # Validate configuration before making any calls
    _validate_llm_configuration()

    litellm_model = _get_model_name(mini=mini, model_name=model_name)

    # Build messages
    messages: list[dict[str, str]] = []
    if system_prompt:
        messages.append({'role': 'system', 'content': system_prompt})
    messages.append({'role': 'user', 'content': prompt})

    # Build call params
    call_params: dict[str, Any] = {
        'model': litellm_model,
        'messages': messages,
        'max_tokens': max_tokens,
    }

    # Temperature
    if temperature != 1.0:
        call_params['temperature'] = temperature

    # Response format
    if response_format:
        if isinstance(response_format, type) and issubclass(response_format, BaseModel):
            schema = response_format.model_json_schema()
            call_params['response_format'] = {
                'type': 'json_schema',
                'json_schema': {'name': response_format.__name__, 'schema': schema, 'strict': True},
            }
        elif isinstance(response_format, dict):
            call_params['response_format'] = response_format
        elif response_format == 'json_object':
            call_params['response_format'] = {'type': 'json_object'}

    logger.info(f'Calling LiteLLM: {litellm_model}')

    # Non-streaming with retry
    max_retries = 2
    for attempt in range(max_retries):
        try:
            response = await asyncio.wait_for(litellm.acompletion(**call_params), timeout=timeout)
            break
        except litellm.RateLimitError as e:
            if attempt >= max_retries - 1:
                logger.error(f'LiteLLM rate limit exceeded for {litellm_model}: {e}')
                raise
            wait_time = 10
            logger.warning(f'Rate limit hit. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}')
            await asyncio.sleep(wait_time)
        except asyncio.TimeoutError:
            logger.error(f'LiteLLM timeout for {litellm_model} after {timeout} seconds')
            raise Exception(f'Model call timed out after {timeout} seconds')
        except Exception as e:
            logger.error(f'LiteLLM error for {litellm_model}: {type(e).__name__}: {e}')
            raise

    if not response.choices or not response.choices[0].message.content:
        finish_reason = response.choices[0].finish_reason if response.choices else 'unknown'
        raise Exception(f'Empty response from model. finish_reason={finish_reason}')

    content = response.choices[0].message.content

    # Validate Pydantic response
    if isinstance(response_format, type) and issubclass(response_format, BaseModel):
        try:
            response_format.model_validate(json.loads(content))
        except Exception as e:
            logger.warning(f'Response validation failed: {e}')

    return content
