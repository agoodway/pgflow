defmodule PgflowDemo.LLM do
  @moduledoc """
  LLM integration for the PgFlow demo app using ReqLLM.

  Uses OpenAI's gpt-4o-mini model by default. Configure via environment variable:

      OPENAI_API_KEY=sk-...
  """

  @default_model "openai:gpt-4o-mini"
  @max_content_length 12_000

  @doc """
  Summarize article content using the LLM.

  Takes markdown content and returns a concise 2-3 paragraph summary.
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
  Extract keywords from article content using the LLM.

  Takes markdown content and returns a list of 5-10 relevant keywords.
  """
  @spec extract_keywords(String.t()) :: {:ok, list(String.t())} | {:error, term()}
  def extract_keywords(content) do
    prompt = """
    Extract 5-10 relevant keywords or key phrases from the following article.

    Article:
    #{truncate_content(content)}
    """

    schema = [keywords: [type: {:list, :string}, required: true]]

    case ReqLLM.generate_object(model(), prompt, schema) do
      {:ok, response} ->
        {:ok, ReqLLM.Response.object(response)["keywords"]}

      {:error, reason} ->
        {:error, format_error(reason)}
    end
  end

  @doc """
  Generate text from the LLM.

  Returns `{:ok, text}` on success or `{:error, reason}` on failure.
  """
  @spec generate(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def generate(prompt, opts \\ []) do
    model_spec = model()
    temperature = Keyword.get(opts, :temperature, 0.7)
    max_tokens = Keyword.get(opts, :max_tokens, 1000)

    case ReqLLM.generate_text(model_spec, prompt,
           temperature: temperature,
           max_tokens: max_tokens
         ) do
      {:ok, response} ->
        {:ok, ReqLLM.Response.text(response)}

      {:error, reason} ->
        {:error, format_error(reason)}
    end
  end

  @doc """
  Get the current model being used.
  """
  @spec model() :: String.t()
  def model do
    Application.get_env(:pgflow_demo, :llm_model, @default_model)
  end

  defp format_error(%{message: message}) when is_binary(message), do: message
  defp format_error(reason), do: inspect(reason)

  defp truncate_content(content), do: String.slice(content, 0, @max_content_length)
end
