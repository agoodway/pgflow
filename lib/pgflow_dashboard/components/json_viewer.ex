defmodule PgFlowDashboard.Components.JsonViewer do
  @moduledoc """
  Server-rendered, syntax-styled JSON with an optional copy action.
  """

  use Phoenix.Component

  @doc """
  Renders JSON-compatible data with semantic syntax tokens.

  Values are emitted through HEEx so payload content remains HTML escaped.
  """
  attr(:id, :string, required: true)
  attr(:data, :any, required: true)
  attr(:class, :any, default: nil)

  def json_viewer(assigns) do
    assigns =
      assigns
      |> assign(:empty?, is_nil(assigns.data))
      |> assign(:tokens, if(is_nil(assigns.data), do: [], else: json_tokens(assigns.data)))

    ~H"""
    <div id={@id} class={["relative group", @class]}>
      <%= if @empty? do %>
        <div class="rounded-md border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-center text-xs text-slate-500 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-400">
          No data
        </div>
      <% else %>
        <button
          type="button"
          id={"#{@id}-copy"}
          phx-hook="CopyToClipboard"
          phx-update="ignore"
          data-copy-target={"#{@id}-code"}
          class="absolute right-2 top-2 z-10 inline-flex items-center gap-1.5 rounded-md border border-slate-600 bg-slate-800/90 px-2 py-1 text-[11px] font-medium text-slate-200 opacity-70 shadow-sm transition hover:border-slate-500 hover:bg-slate-700 hover:text-white hover:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-400 data-[copied]:border-emerald-500 data-[copied]:bg-emerald-600 data-[copied]:text-white data-[copied]:opacity-100"
          aria-label="Copy JSON to clipboard"
        >
          <svg data-copy-default class="size-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
            <rect x="9" y="9" width="11" height="11" rx="2" />
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
          </svg>
          <svg data-copy-success hidden class="size-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
            <path d="m5 12 4 4L19 6" />
          </svg>
          <span data-copy-default>Copy</span>
          <span data-copy-success hidden>Copied</span>
          <span data-copy-announcement class="sr-only" aria-live="polite"></span>
        </button>
        <pre
          id={"#{@id}-code"}
          class="max-h-96 overflow-auto whitespace-pre-wrap break-words rounded-md border border-slate-700 bg-slate-950 p-4 pr-16 font-mono text-xs leading-5 text-slate-200 shadow-inner"
        ><code><%= for {type, token} <- @tokens do %><span class={token_class(type)}>{token}</span><% end %></code></pre>
      <% end %>
    </div>
    """
  end

  defp json_tokens(data), do: value_tokens(data, 0)

  defp value_tokens(data, depth) when is_map(data) and not is_struct(data) do
    entries = Enum.sort_by(data, fn {key, _value} -> key_string(key) end)

    if entries == [] do
      [{:punctuation, "{}"}]
    else
      last_index = length(entries) - 1

      entry_tokens =
        entries
        |> Enum.with_index()
        |> Enum.flat_map(&map_entry_tokens(&1, last_index, depth))

      [{:punctuation, "{"}, {:whitespace, "\n"}] ++
        entry_tokens ++ [{:whitespace, indent(depth)}, {:punctuation, "}"}]
    end
  end

  defp value_tokens(data, depth) when is_list(data) do
    if data == [] do
      [{:punctuation, "[]"}]
    else
      last_index = length(data) - 1

      item_tokens =
        data
        |> Enum.with_index()
        |> Enum.flat_map(&list_item_tokens(&1, last_index, depth))

      [{:punctuation, "["}, {:whitespace, "\n"}] ++
        item_tokens ++ [{:whitespace, indent(depth)}, {:punctuation, "]"}]
    end
  end

  defp value_tokens(data, _depth) when is_binary(data), do: [{:string, encode_string(data)}]
  defp value_tokens(data, _depth) when is_number(data), do: [{:number, to_string(data)}]
  defp value_tokens(data, _depth) when is_boolean(data), do: [{:boolean, to_string(data)}]
  defp value_tokens(nil, _depth), do: [{:null, "null"}]
  defp value_tokens(data, _depth), do: [{:string, encode_string(inspect(data))}]

  defp map_entry_tokens({{key, value}, index}, last_index, depth) do
    [
      {:whitespace, indent(depth + 1)},
      {:key, encode_string(key_string(key))},
      {:punctuation, ": "}
    ] ++
      value_tokens(value, depth + 1) ++ comma_token(index, last_index) ++ [{:whitespace, "\n"}]
  end

  defp list_item_tokens({value, index}, last_index, depth) do
    [{:whitespace, indent(depth + 1)}] ++
      value_tokens(value, depth + 1) ++ comma_token(index, last_index) ++ [{:whitespace, "\n"}]
  end

  defp comma_token(index, last_index) when index < last_index, do: [{:punctuation, ","}]
  defp comma_token(_index, _last_index), do: []

  defp key_string(key) when is_binary(key), do: key
  defp key_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_string(key) when is_integer(key), do: Integer.to_string(key)
  defp key_string(key), do: inspect(key)

  defp encode_string(value), do: Jason.encode!(value)
  defp indent(depth), do: String.duplicate("  ", depth)

  defp token_class(:key), do: "json-token-key text-sky-300"
  defp token_class(:string), do: "json-token-string text-emerald-300"
  defp token_class(:number), do: "json-token-number text-amber-300"
  defp token_class(:boolean), do: "json-token-boolean text-violet-300"
  defp token_class(:null), do: "json-token-null text-rose-300 italic"
  defp token_class(:punctuation), do: "json-token-punctuation text-slate-400"
  defp token_class(:whitespace), do: nil
end
