defmodule DuckCaller.Print do
  @default_sample_nrows 5
  # TODO: change "res" to a tabular Table.Reader structure
  def to_term(res, conn, opts \\ []) do
    headers =
      res
      |> List.first()
      |> Map.keys()

    row_count = length(res)

    header_count = length(headers)

    values =
      case opts[:limit] do
        :infinity ->
          DuckCaller.Transform.map_values_to_list_ordered(res, headers)

        nrow when is_integer(nrow) and nrow >= 0 ->
          DuckCaller.Transform.map_values_to_list_ordered(res, headers) |> Enum.take(nrow)

        _ ->
          DuckCaller.Transform.map_values_to_list_ordered(res, headers)
          |> Enum.take(@default_sample_nrows)
      end

    cols =
      headers
      |> Enum.map(&"'#{&1}'")
      |> Enum.join(", ")

    # TODO: remove the datatypes, make them optional, or use the Table.Reader Elixir types instead of another DB query.
    {:ok, type_res} =
      Duckdbex.query(
        conn,
        "select distinct column_name, data_type from information_schema.columns where column_name in (#{cols});"
      )

    name_type =
      type_res
      |> Duckdbex.fetch_all()
      |> Enum.sort_by(fn [header, _type] -> Enum.find_index(headers, &(&1 == header)) end)
      |> Enum.map(fn [header, type] -> "#{header}\n<#{type}>" end)

    # types = Enum.map(df.names, &"\n<#{Shared.dtype_to_string(df.dtypes[&1])}>")
    # name_type = Enum.zip_with(headers, types, fn x, y -> x <> y end)
    # Select ProcurementGroup, Buyer, "BuyerPostalAddress.DeliveryAddress.AddressLine2" From Buyer;

    TableRex.Table.new()
    |> TableRex.Table.put_title("Duckdb Query: [rows: #{row_count}, columns: #{header_count}]")
    |> TableRex.Table.put_header(name_type)
    |> TableRex.Table.put_header_meta(0..header_count, align: :center)
    |> TableRex.Table.add_rows(values)
    |> TableRex.Table.render!(
      header_separator_symbol: "=",
      horizontal_style: :all
    )
    |> IO.puts()
  end
end
