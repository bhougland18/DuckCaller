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

  def connect!() do
    {:ok, db} = Duckdbex.open()
    {:ok, _conn} = Duckdbex.connection(db)
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
end
