defmodule DuckCaller do
  @moduledoc """
  Documentation for `InforDmf`.
  """

  @doc """
  Create a new DuckDB instance with two extensions, one for Excel and the other for Json.
  """

  @default_log_path "duck_caller_errors.log"

  def create!(db_name, opts \\ [{:core_extensions, ["spatial", "json", "httpfs"]}]) do
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
  Executes SQL queries from a file within a transaction.
  Returns {:ok, result} on success or {:error, reason} on failure.

  ## Examples

      iex> DuckCaller.execute_sql_file!(conn, "path/to/queries.sql")
      {:ok, result}
  """
  def execute_sql_file!(conn, file_path) do
    transaction(conn, fn ->
      case File.read(file_path) do
        {:ok, sql_content} ->
          # query! returns the result directly, not in a tuple
          result = DuckCaller.query!(conn, sql_content)
          {:ok, result}

        {:error, reason} ->
          {:error, {:file_error, reason, file_path}}
      end
    end)
  end

  @doc """
  Performs batch updates sequentially without concurrent processing.
  The updates argument takes a list of maps with the following fields:
    * :table
    * :field
    * :value
  """
  def execute_batch_update(conn, updates, opts \\ []) when is_list(updates) do
    log_path = Keyword.get(opts, :log_path, @default_log_path)
    timestamp = NaiveDateTime.local_now() |> NaiveDateTime.to_string()

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
        log_errors(failures, timestamp, log_path)
        {:error, failures}
      end
    end)
  end

  defp log_errors(failures, timestamp, log_path) do
    error_text = format_errors(failures, timestamp)

    # Ensure directory exists
    log_dir = Path.dirname(log_path)
    File.mkdir_p!(log_dir)

    # Append errors to log file
    File.write!(log_path, error_text, [:append])
  end

  defp format_errors(failures, timestamp) do
    header = """

    =====================================
    Error Report - #{timestamp}
    =====================================

    """

    error_details =
      Enum.map_join(failures, "\n", fn
        {:error, {:missing_key, update_map}} ->
          """
          Error Type: Missing Required Key
          Update Attempted: #{inspect(update_map)}
          Missing Keys: #{identify_missing_keys(update_map)}
          """

        {:error, {:invalid_identifier, update_map}} ->
          """
          Error Type: Invalid Identifier
          Update Attempted: #{inspect(update_map)}
          Invalid Fields: #{identify_invalid_identifiers(update_map)}
          """

        {:error, {table, field, value, reason, sql}} ->
          """
          Error Type: SQL Execution Error
          Table: #{table}
          Field: #{field}
          Value: #{inspect(value)}
          SQL: #{sql}
          Error Message: #{inspect(reason)}
          """

        other_error ->
          """
          Error Type: Unexpected Error
          Details: #{inspect(other_error)}
          """
      end)

    header <> error_details
  end

  defp identify_missing_keys(update_map) do
    required_keys = [:table, :field, :value]
    missing = required_keys -- Map.keys(update_map)
    Enum.join(missing, ", ")
  end

  defp identify_invalid_identifiers(update_map) do
    invalid =
      Enum.filter([:table, :field], fn key ->
        case Map.get(update_map, key) do
          nil -> false
          value -> not valid_identifier?(value)
        end
      end)

    Enum.join(invalid, ", ")
  end

  # Helper function to format identifiers with double quotes around the entire name
  defp format_identifier(name) when is_binary(name) do
    ~s("#{name}")
  end

  # Updated format_value function with NULL handling
  defp format_value("NULL"), do: "NULL"
  defp format_value(nil), do: "NULL"
  defp format_value(value) when is_binary(value), do: "'#{String.replace(value, "'", "''")}'"
  defp format_value(value), do: to_string(value)

  # Updated valid_identifier to handle period-separated identifiers
  defp valid_identifier?(name) when is_binary(name) do
    name
    |> String.split(".")
    |> Enum.all?(fn part -> String.match?(part, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/) end)
  end

  defp valid_identifier?(_), do: false

  # Updated simple_update with quoted identifiers
  defp simple_update(conn, table, field, value) do
    quoted_table = format_identifier(table)
    quoted_field = format_identifier(field)
    sql = "UPDATE #{quoted_table} SET #{quoted_field} = #{format_value(value)};"

    case Duckdbex.query(conn, sql) do
      {:ok, result} -> {:ok, {table, field, value, result}}
      {:error, reason} -> {:error, {table, field, value, reason, sql}}
    end
  end
end
