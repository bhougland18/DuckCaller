# defmodule DuckCaller.Runner.Duckdb do
#   use AyeSQL.Runner

#   @impl AyeSQL
#   def run(%AyeSQL.Query{statement: stmt, arguments: args}, options) do
#     query_options = Keyword.drop(options, [:pool, :into])
#     stmt = transform_stmt(stmt)

#     with {:ok, ref} <- Duckdbex.query(conn, stmt) do
#       columns = Duckdbex.columns(ref)
#       rows = Duckdbex.fetch_all(ref)
#       result = %{columns: columns, rows: rows}
#       result = AyeSQL.Runner.handle_result(result)
#       {:ok, result}
#     end
#   end

#   @spec transform_stmt(AyeSQL.Query.statement()) :: AyeSQL.Query.statement()
#   defp transform_stmt(stmt) do
#     Regex.replace(~r/\$(\d+)/, stmt, "?")
#   end
# end
