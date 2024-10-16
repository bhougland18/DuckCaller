defmodule IO.ExcelStreamer do
  @moduledoc """
  Provides functions to stream various data sources to Excel files,
  including support for multiple sheets.
  """

  @doc """
  Streams multiple sheets to a single Excel file.

  ## Parameters
  - conn: The DuckDB connection
  - sheets: A keyword list where each key is the sheet name and the value is either:
    - A query string
    - A DuckDB query reference
    - A tabular data structure implementing the Table.Reader protocol
  - output_path: The path where the Excel file will be saved

  ## Example
  ```elixir
  ExcelStreamer.stream_multi_sheet_to_excel(conn, [
    sheet1: "SELECT * FROM table1",
    sheet2: ref,
    sheet3: tabular_data
  ], "multi_sheet_output.xlsx")
  ```
  """
  def from_query_list_to_multi_sheet_excel(conn, sheets, output_path) do
    workbook = Exceed.Workbook.new("Multi-Sheet Data Export")

    sheets_data =
      Enum.map(sheets, fn {sheet_name, data} ->
        {sheet_name, prepare_sheet_data(conn, data)}
      end)

    workbook_with_sheets =
      Enum.reduce(sheets_data, workbook, fn {sheet_name, {columns, rows}}, acc ->
        worksheet = Exceed.Worksheet.new(to_string(sheet_name), columns, rows)
        Exceed.Workbook.add_worksheet(acc, worksheet)
      end)

    workbook_with_sheets
    |> Exceed.stream!()
    |> Stream.into(File.stream!(output_path))
    |> Stream.run()
  end

  # Existing functions...

  def from_query_to_excel(conn, query_string, output_path, sheet_name \\ "Query Results") do
    {:ok, ref} = Duckdbex.query(conn, query_string)
    from_ref_to_excel(ref, output_path, sheet_name)
  end

  def from_ref_to_excel(ref, output_path, sheet_name) do
    columns = Duckdbex.columns(ref)
    rows_stream = create_rows_stream(ref)
    stream_to_excel(columns, rows_stream, output_path, sheet_name)
  end

  def from_tabular_to_excel(tabular, output_path, sheet_name \\ "Tabular Data") do
    case DuckCaller.Table.Reader.init(tabular) do
      {:columns, metadata, data} ->
        columns = metadata.columns
        rows_stream = columns_to_rows_stream(data)
        stream_to_excel(columns, rows_stream, output_path, sheet_name)

      {:rows, metadata, data} ->
        columns = metadata.columns
        stream_to_excel(columns, data, output_path, sheet_name)

      :none ->
        raise ArgumentError, "Given data is not tabular"
    end
  end

  # Private helper functions

  defp prepare_sheet_data(conn, data) do
    cond do
      is_binary(data) ->
        {:ok, ref} = Duckdbex.query(conn, data)
        {Duckdbex.columns(ref), create_rows_stream(ref)}

      is_tuple(data) and elem(data, 0) == :ok ->
        ref = elem(data, 1)
        {Duckdbex.columns(ref), create_rows_stream(ref)}

      true ->
        case Table.Reader.init(data) do
          {:columns, metadata, column_data} ->
            {metadata.columns, columns_to_rows_stream(column_data)}

          {:rows, metadata, row_data} ->
            {metadata.columns, rows_to_stream(row_data)}

          :none ->
            raise ArgumentError, "Invalid data format for sheet"
        end
    end
  end

  defp create_rows_stream(ref) do
    Stream.resource(
      fn -> ref end,
      fn ref ->
        case Duckdbex.fetch_chunk(ref) do
          [] ->
            {:halt, ref}

          chunk when is_list(chunk) ->
            transformed_chunk = Enum.map(chunk, &transform_row/1)
            {transformed_chunk, ref}

          {:error, reason} ->
            raise "Error fetching chunk: #{inspect(reason)}"
        end
      end,
      fn _ref -> :ok end
    )
  end

  defp columns_to_rows_stream(column_data) do
    column_data
    |> Enum.zip()
    |> Stream.map(&Tuple.to_list/1)
    |> Stream.map(&transform_row/1)
  end

  defp rows_to_stream(row_data) do
    Stream.map(row_data, &transform_row/1)
  end

  defp transform_row(row) do
    Enum.map(row, fn cell ->
      cond do
        cell == nil -> ""
        is_atom(cell) -> Atom.to_string(cell)
        true -> cell
      end
    end)
  end

  defp stream_to_excel(columns, rows_stream, output_path, sheet_name) do
    worksheet = Exceed.Worksheet.new(sheet_name, columns, rows_stream)

    Exceed.Workbook.new("Data Export")
    |> Exceed.Workbook.add_worksheet(worksheet)
    |> Exceed.stream!()
    |> Stream.into(File.stream!(output_path))
    |> Stream.run()
  end
end
