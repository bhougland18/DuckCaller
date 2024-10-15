defmodule DuckCaller do
  @moduledoc """
  Documentation for `InforDmf`.
  """

  @doc """
  Create a new DuckDB instance with two extensions, one for Excel and the other for Json.
  """
  def create!(db_name, opts \\ [{:core_extensions, ["spatial", "json"]}]) do
    {:ok, conn} = connect!(db_name)

    if opts[:core_extensions] do
      Enum.each(opts[:core_extensions], fn e -> import_extension!(conn, e) end)
    end

    {:ok, conn}
  end

  # TODO: maybe continue on this but there is no way to destroy a prepared statement
  # defp import_extensions!(conn, extensions) do
  #   {:ok, stmt_install} = Duckdbex.prepare_statement(conn, "INSTALL $1;")
  #   {:ok, stmt_load} = Duckdbex.prepare_statement(conn, "LOAD $1;")

  #   Enum.each(extensions, fn e ->
  #     Duckbex.execute_statement(stmt_install, [e])
  #     Duckbex.execute_statement(stmt_load, [e])
  #   end)

  # TODO: does this need a prepared statement?
  defp import_extension!(conn, extension) do
    Duckdbex.query(conn, "INSTALL '#{extension}';")
  end

  @spec connect!() :: term()
  def connect!() do
    with {:ok, db} <- Duckdbex.open(),
         {:ok, conn} <- Duckdbex.connection(db) do
      conn
    else
      {:error, reason} -> raise "Connection failed: #{reason}"
    end
  end

  # TODO: have the arguement checked between string and struct (for config with in memory database)
  def connect!(db_name) do
    {:ok, db} = Duckdbex.open(db_name)
    {:ok, _conn} = Duckdbex.connection(db)
  end

  def connect!(db_name, config) do
    {:ok, db} = Duckdbex.open(db_name, config)
    {:ok, _conn} = Duckdbex.connection(db)
  end

  def query!(conn, query_string) do
    {:ok, ref} = Duckdbex.query(conn, query_string)
    rows = Duckdbex.fetch_all(ref)
    columns = Duckdbex.columns(ref) |> Enum.map(fn c -> String.to_atom(c) end)

    tabular =
      Enum.map(rows, fn row ->
        Enum.zip(columns, row) |> Map.new()
      end)

    tabular
  end

  defp transaction(conn, fun) do
    try do
      {:ok, _} = Duckdbex.query(conn, "BEGIN TRANSACTION;")
      result = fun.()

      case result do
        {:ok, _} ->
          {:ok, _} = Duckdbex.query(conn, "COMMIT;")
          result

        {:error, _} = error ->
          {:ok, _} = Duckdbex.query(conn, "ROLLBACK;")
          error
      end
    rescue
      e ->
        {:ok, _} = Duckdbex.query(conn, "ROLLBACK;")
        {:error, e}
    end
  end

  def batch_update(conn, updates, opts \\ []) when is_list(updates) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online() * 2)
    timeout = Keyword.get(opts, :timeout, 30_000)

    transaction(conn, fn ->
      results =
        Task.async_stream(
          updates,
          fn update_map ->
            with {:ok, table} <- Map.fetch(update_map, :table),
                 {:ok, field} <- Map.fetch(update_map, :field),
                 {:ok, value} <- Map.fetch(update_map, :value) do
              simple_update(conn, table, field, value)
            else
              :error -> {:error, {:missing_key, update_map}}
            end
          end,
          max_concurrency: max_concurrency,
          timeout: timeout
        )
        |> Enum.map(fn {:ok, result} -> result end)

      failures =
        Enum.filter(results, fn
          {:error, _} -> true
          _ -> false
        end)

      if Enum.empty?(failures) do
        {:ok, results}
      else
        {:error, failures}
      end
    end)
  end

  defp format_value(value) when is_binary(value), do: "'#{String.replace(value, "'", "''")}'"
  defp format_value(value), do: to_string(value)

  # TODO: trade this out with AyeSQL query so we don't have injection attack issue
  defp simple_update(conn, table, field, value) do
    sql = "UPDATE #{table} SET #{field} = #{format_value(value)};"

    case Duckdbex.query(conn, sql) do
      {:ok, result} -> {:ok, {table, field, value, result}}
      {:error, reason} -> {:error, {table, field, value, reason, sql}}
    end
  end
end
