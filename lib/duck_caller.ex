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
    Duckdbex.query(conn, "LOAD '#{extension}';")
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

  @doc """
  Performs batch updates sequentially without concurrent processing.
  This simpler version might be more reliable in some scenarios.
  """
  def batch_update(conn, updates) when is_list(updates) do
    transaction(conn, fn ->
      results =
        Enum.map(updates, fn update_map ->
          with {:ok, table} <- Map.fetch(update_map, :table),
               {:ok, field} <- Map.fetch(update_map, :field),
               {:ok, value} <- Map.fetch(update_map, :value),
               true <- valid_identifier?(table),
               true <- valid_identifier?(field) do
            simple_update(conn, table, field, value)
          else
            :error -> {:error, {:missing_key, update_map}}
            false -> {:error, {:invalid_identifier, update_map}}
          end
        end)

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

  # Updated format_value function with NULL handling
  # Handle string "NULL"
  defp format_value("NULL"), do: "NULL"
  # Handle nil value
  defp format_value(nil), do: "NULL"
  defp format_value(value) when is_binary(value), do: "'#{String.replace(value, "'", "''")}'"
  defp format_value(value), do: to_string(value)

  # Validate identifiers to prevent SQL injection
  defp valid_identifier?(name) when is_binary(name) do
    String.match?(name, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/)
  end

  defp valid_identifier?(_), do: false

  defp simple_update(conn, table, field, value) do
    sql = "UPDATE #{table} SET #{field} = #{format_value(value)};"

    case Duckdbex.query(conn, sql) do
      {:ok, result} -> {:ok, {table, field, value, result}}
      {:error, reason} -> {:error, {table, field, value, reason, sql}}
    end
  end
end
