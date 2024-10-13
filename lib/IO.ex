defmodule DuckCaller.IO do
  @doc """
  Creates DuckDB tables from Excel Sheets.  A table will be created for each sheet.
  """
  def from_excel!(conn, path) do
    {:ok, package} = XlsxReader.open(path)
    sheets = XlsxReader.sheet_names(package)
    excel_sheets_to_table!(conn, path, sheets)
  end

  def from_excel!(conn, path, sheets) when is_list(sheets),
    do: excel_sheets_to_table!(conn, path, sheets)

  def from_excel!(conn, path, sheets) when is_binary(sheets),
    do: excel_sheet_to_table!(conn, path, sheets)

  defp excel_sheets_to_table!(conn, path, sheets) do
    {:ok, Enum.each(sheets, fn s -> excel_sheet_to_table!(conn, path, s) end)}
  end

  defp excel_sheet_to_table!(conn, path, sheet) do
    # TODO:Make the open_options available to change
    Duckdbex.query(
      conn,
      "CREATE TABLE #{sheet} AS SELECT * FROM st_read('#{path}', layer = '#{sheet}', open_options = ['HEADERS=FORCE']);"
    )

    {:ok, IO.puts("#{sheet} has been loaded into the database!")}
  end

  # TODO: depreciate this as it is only a one table to one worksheet approach.  Look into the Exceed library instead.
  @doc """
  Create an Excel file for a given DuckDB Table
  """
  def to_excel!(conn, path, table) do
    Duckdbex.query(conn, "LOAD 'spatial';")

    Duckdbex.query(conn, """
    COPY (SELECT * FROM #{table}) TO '#{path}' WITH (FORMAT GDAL, DRIVER 'xlsx');
    """)
  end

  # def stream_to_excel!() do
  #   IO.puts("I don't do anything.")
  # end
end
