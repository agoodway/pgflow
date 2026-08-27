defmodule PgflowDemo.LLM do
  @moduledoc """
  LLM integration for the PgFlow demo via ReqLLM.

  Routes through an OpenAI-compatible endpoint (Fireworks by default).
  Configure via env vars (loaded by `runtime.exs`):

      AI_API_KEY=<fireworks key>
      AI_API_BASE=https://api.fireworks.ai/inference/v1
      AI_MODEL_NAME=accounts/fireworks/models/deepseek-v3p1
  """

  @max_content_length 12_000

  @doc """
  Returns the ReqLLM model spec for the configured provider/model.
  """
  @spec model_spec() :: %{id: String.t(), provider: :openai}
  def model_spec do
    %{id: model_name(), provider: :openai}
  end

  @doc """
  Returns per-call options (api_key + base_url + timeouts) merged with overrides.
  """
  @spec request_opts(keyword()) :: keyword()
  def request_opts(overrides \\ []) do
    Keyword.merge(
      [
        api_key: api_key(),
        base_url: api_base(),
        receive_timeout: 45_000,
        max_retries: 0,
        req_http_options: [pool_timeout: 3_000]
      ],
      overrides
    )
  end

  @doc """
  Summarizes article content into a 2-3 paragraph summary.
  """
  @spec summarize(String.t()) :: {:ok, String.t()} | {:error, term()}
  def summarize(content) do
    prompt = """
    Please provide a concise summary of the following article in 2-3 paragraphs.
    Focus on the main points and key takeaways.

    Article:
    #{truncate_content(content)}
    """

    generate(prompt)
  end

  @doc """
  Extracts 5-10 keywords from article content via structured output.
  """
  @spec extract_keywords(String.t()) :: {:ok, list(String.t())} | {:error, term()}
  def extract_keywords(content) do
    prompt = """
    Extract 5-10 relevant keywords or key phrases from the following article.

    Article:
    #{truncate_content(content)}
    """

    schema = [keywords: [type: {:list, :string}, required: true]]

    with {:ok, response} <-
           ReqLLM.generate_object(model_spec(), prompt, schema, request_opts()),
         {:ok, object} <- ReqLLM.Response.unwrap_object(response) do
      {:ok, extract_keyword_values(object)}
    else
      {:error, reason} -> {:error, format_error(reason)}
    end
  end

  @doc false
  @spec extract_keyword_values(map()) :: [String.t()]
  def extract_keyword_values(object) do
    %{string: string_value, atom: atom_value} = normalize_keyword_values(object)
    string_value || atom_value || []
  end

  defp normalize_keyword_values(object) do
    Enum.reduce(object, %{string: nil, atom: nil}, fn
      {"keywords", value}, values -> %{values | string: value}
      {:keywords, value}, values -> %{values | atom: value}
      _field, values -> values
    end)
  end

  @doc """
  Generate text from the LLM.
  """
  @spec generate(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def generate(prompt, opts \\ []) do
    temperature = Keyword.get(opts, :temperature, 0.7)
    max_tokens = Keyword.get(opts, :max_tokens, 1000)

    case ReqLLM.generate_text(
           model_spec(),
           prompt,
           request_opts(temperature: temperature, max_tokens: max_tokens)
         ) do
      {:ok, response} ->
        {:ok, ReqLLM.Response.text(response)}

      {:error, reason} ->
        {:error, format_error(reason)}
    end
  end

  defp model_name do
    Application.get_env(:pgflow_demo, :ai_model_name) ||
      raise "AI_MODEL_NAME not configured (set :ai_model_name in :pgflow_demo config)"
  end

  defp api_key do
    Application.get_env(:pgflow_demo, :ai_api_key) ||
      raise "AI_API_KEY not configured (set :ai_api_key in :pgflow_demo config)"
  end

  defp api_base do
    Application.get_env(:pgflow_demo, :ai_api_base) ||
      raise "AI_API_BASE not configured (set :ai_api_base in :pgflow_demo config)"
  end

  defp format_error(%{message: message}) when is_binary(message), do: message
  defp format_error(reason), do: inspect(reason)

  defp truncate_content(content), do: String.slice(content, 0, @max_content_length)
end
