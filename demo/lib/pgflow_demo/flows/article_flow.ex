defmodule PgflowDemo.Flows.ArticleFlow do
  @moduledoc """
  Demo flow that processes an article URL through multiple steps.

  DAG Structure:
  ```
  fetch_article → convert_to_markdown → summarize        → publish
                                      ↘ extract_keywords ↗
  ```
  """

  use PgFlow.Flow

  @flow slug: :article_flow, max_attempts: 3, base_delay: 5, timeout: 120

  # Fetch the article HTML from the URL
  step :fetch_article do
    fn input, _ctx ->
      url = input["url"]

      case Req.get(url, receive_timeout: 30_000) do
        {:ok, %{status: status, body: body}} when status in 200..299 ->
          %{
            "url" => url,
            "html" => body,
            "fetched_at" => DateTime.utc_now() |> DateTime.to_iso8601()
          }

        {:ok, %{status: status}} ->
          raise "Failed to fetch article: HTTP #{status}"

        {:error, reason} ->
          raise "Failed to fetch article: #{inspect(reason)}"
      end
    end
  end

  # Convert HTML to Markdown
  step :convert_to_markdown, depends_on: [:fetch_article] do
    fn deps, _ctx ->
      html = deps["fetch_article"]["html"]

      # Parse HTML and extract main content
      markdown = html_to_markdown(html)

      %{
        "markdown" => markdown,
        "char_count" => String.length(markdown)
      }
    end
  end

  # Summarize the article using LLM (runs in parallel with extract_keywords)
  step :summarize, depends_on: [:convert_to_markdown] do
    fn deps, _ctx ->
      markdown = deps["convert_to_markdown"]["markdown"]

      case PgflowDemo.LLM.summarize(markdown) do
        {:ok, summary} ->
          %{"summary" => summary}

        {:error, reason} ->
          raise "LLM summarization failed: #{reason}"
      end
    end
  end

  # Extract keywords using LLM (runs in parallel with summarize)
  step :extract_keywords, depends_on: [:convert_to_markdown] do
    fn deps, _ctx ->
      markdown = deps["convert_to_markdown"]["markdown"]

      case PgflowDemo.LLM.extract_keywords(markdown) do
        {:ok, keywords} ->
          %{"keywords" => keywords}

        {:error, reason} ->
          raise "LLM keyword extraction failed: #{reason}"
      end
    end
  end

  # Combine all results into final output
  step :publish, depends_on: [:summarize, :extract_keywords] do
    fn deps, _ctx ->
      %{
        "summary" => deps["summarize"]["summary"],
        "keywords" => deps["extract_keywords"]["keywords"],
        "published_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }
    end
  end

  # Private helper functions

  defp html_to_markdown(html) do
    # Parse HTML with Floki
    case Floki.parse_document(html) do
      {:ok, document} ->
        # Try to find main content areas
        content =
          document
          |> find_main_content()
          |> extract_text_content()
          |> clean_text()

        if String.trim(content) == "" do
          # Fallback: extract all text
          document
          |> Floki.text(sep: "\n\n")
          |> clean_text()
        else
          content
        end

      {:error, _} ->
        # Fallback: strip tags naively
        html
        |> String.replace(~r/<script[^>]*>.*?<\/script>/is, "")
        |> String.replace(~r/<style[^>]*>.*?<\/style>/is, "")
        |> String.replace(~r/<[^>]+>/, " ")
        |> clean_text()
    end
  end

  defp find_main_content(document) do
    # Try common content selectors in order of preference
    selectors = [
      "article",
      "main",
      "[role=main]",
      ".post-content",
      ".article-content",
      ".entry-content",
      ".content",
      "#content",
      "body"
    ]

    Enum.find_value(selectors, document, fn selector ->
      case Floki.find(document, selector) do
        [] -> nil
        found -> found
      end
    end)
  end

  defp extract_text_content(elements) do
    elements
    |> Floki.find("p, h1, h2, h3, h4, h5, h6, li, blockquote")
    |> Enum.map_join("\n\n", fn element ->
      tag = elem(element, 0)
      text = Floki.text(element)

      case tag do
        "h1" -> "# #{text}"
        "h2" -> "## #{text}"
        "h3" -> "### #{text}"
        "h4" -> "#### #{text}"
        "h5" -> "##### #{text}"
        "h6" -> "###### #{text}"
        "li" -> "- #{text}"
        "blockquote" -> "> #{text}"
        _ -> text
      end
    end)
  end

  defp clean_text(text) do
    text
    |> String.replace(~r/\s+/, " ")
    |> String.replace(~r/\n{3,}/, "\n\n")
    |> String.trim()
  end
end
