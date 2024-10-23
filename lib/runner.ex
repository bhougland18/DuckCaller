defmodule DuckCaller.Runner.Duckdb do
  use AyeSQL.Runner

  # @impl AyeSQL.Runner
  # def run(%AyeSQL.Query{statement: stmt, arguments: args}, options) do
  #   conn = options[:conn] || raise ArgumentError, message: "Connection `:conn` cannot be `nil`"

  #   with {:ok, res} <- Duckdbex.query(conn, stmt, args) do
  #     columns = Duckdbex.columns(res)
  #     rows = Duckdbex.fetch_all(res)
  #     {:ok, AyeSQL.Runner.handle_result(%{columns: columns, rows: rows}, options)}
  #   end
  # end

  # @spec transform_stmt(AyeSQL.Query.statement()) :: AyeSQL.Query.statement()
  # defp transform_stmt(stmt) do
  #   Regex.replace(~r/\$(\d+)/, stmt, "?")
  # end
end
