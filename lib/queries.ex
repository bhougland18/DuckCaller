defmodule DuckCaller.Queries do
  use AyeSQL, runner: AyeSQL.Runner.Duckdbex

  defqueries("resources/queries/core.sql")

  @doc """
  Runs an AyeSQL query with the given name and parameters.
  """
  def run_query(conn, query_name, params) when is_atom(query_name) do
    apply(__MODULE__, query_name, [conn, params])
  end
end
