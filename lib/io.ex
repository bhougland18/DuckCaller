defmodule DuckCaller.IO do
  @doc """
  Creates DuckDB tables from Excel Sheets. A table will be created for each sheet.
  """
  def from_excel!(conn, path) do
    {:ok, package} = XlsxReader.open(path)
    sheets = XlsxReader.sheet_names(package)
    excel_sheets_to_table!(conn, path, sheets)
  end

  def from_excel!(conn, path, sheets) when is_list(sheets),
    do: excel_sheets_to_table!(conn, path, sheets)

  def from_excel!(conn, path, sheets) when is_binary(sheets) do
    Duckdbex.query(conn, "LOAD 'spatial';")
    excel_sheet_to_table!(conn, path, sheets)
  end

  defp excel_sheets_to_table!(conn, path, sheets) do
    Duckdbex.query(conn, "LOAD 'spatial';")
    {:ok, Enum.each(sheets, fn s -> excel_sheet_to_table!(conn, path, s) end)}
  end

  defp excel_sheet_to_table!(conn, path, sheet) do
    Duckdbex.query(
      conn,
      "CREATE TABLE #{sheet} AS SELECT * FROM st_read('#{path}', layer = '#{sheet}', open_options = ['HEADERS=FORCE']);"
    )

    {:ok, IO.puts("#{sheet} has been loaded into the database!")}
  end

  @doc """
  Create an Excel file for a given DuckDB Table
  """
  def to_excel!(conn, path, table) do
    Duckdbex.query(conn, "LOAD 'spatial';")

    Duckdbex.query(conn, """
    COPY (SELECT * FROM #{table}) TO '#{path}' WITH (FORMAT GDAL, DRIVER 'xlsx');
    """)
  end

  def from_csv_to_table(conn, file_path, table_name, headers \\ nil, startrow \\ 1) do
    try do
      prepared_file_path =
        case headers do
          nil ->
            skip_rows(file_path, startrow)

          headers ->
            delimiter = find_csv_delimiter(file_path)
            prepare_csv_file(file_path, headers, delimiter, startrow)
        end

      create_table_query =
        "CREATE TABLE #{table_name} AS SELECT * FROM read_csv_auto('#{prepared_file_path}');"

      case Duckdbex.query(conn, create_table_query) do
        {:ok, _result} ->
          {:ok, conn}

        {:error, error} ->
          {:error, "Failed to execute query for #{table_name}: #{inspect(error)}"}
      end
    rescue
      File.Error -> {:error, "File operation error for #{file_path}"}
      e in RuntimeError -> {:error, "Unexpected error in from_csv_to_table: #{inspect(e)}"}
    catch
      :exit, reason ->
        {:error, "Process exit or system error in from_csv_to_table: #{inspect(reason)}"}
    end
  end

  defp skip_rows(file_path, startrow) when startrow == 1, do: file_path

  defp skip_rows(file_path, startrow) do
    prepared_path = "#{file_path}_prepared.csv"

    File.open!(prepared_path, [:write], fn file ->
      File.stream!(file_path)
      |> Stream.drop(startrow - 1)
      |> Stream.each(&IO.write(file, &1))
    end)

    prepared_path
  end

  defp find_csv_delimiter(file_path) do
    delimiters = [",", "\t", ";", "|"]

    Enum.find(delimiters, ",", fn delimiter ->
      file_path
      |> File.stream!()
      |> Stream.take(5)
      |> Enum.all?(fn line -> String.contains?(line, delimiter) end)
    end)
  end

  defp prepare_csv_file(file_path, headers, delimiter, startrow) do
    {dir, _} = file_path |> Path.split() |> Enum.split(-1)
    prepared_dir = Path.join(dir ++ ["prepared_files"])
    File.mkdir_p!(prepared_dir)

    prepared_path = Path.join(prepared_dir, Path.basename(file_path, ".csv") <> "_prepared.csv")

    File.open!(prepared_path, [:write, :utf8], fn file ->
      IO.write(file, Enum.join(headers, delimiter) <> "\n")

      File.stream!(file_path, [], :line)
      |> Stream.drop(startrow - 1)
      |> Stream.each(fn line ->
        cleaned_line = repair_unterminated_quotes(line, delimiter)
        IO.write(file, cleaned_line)
      end)
      |> Stream.run()
    end)

    prepared_path
  end

  defp repair_unterminated_quotes(line, delimiter) do
    line
    |> String.split(delimiter)
    |> Enum.map(fn field ->
      if String.starts_with?(field, "\"") do
        quote_count = String.graphemes(field) |> Enum.count(&(&1 == "\""))

        if rem(quote_count, 2) != 0 do
          field <> "\""
        else
          field
        end
      else
        field
      end
    end)
    |> Enum.join(delimiter)
  end
end
