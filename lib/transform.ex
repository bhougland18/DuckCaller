defmodule DuckCaller.Transform do
  def rows_to_maps(headers, rows) do
    headers = Enum.map(headers, &String.to_atom/1)

    Enum.map(rows, fn row ->
      Enum.zip(headers, row)
      |> Enum.into(%{})
    end)
  end

  def map_values_to_list_ordered(maps, keys) do
    Enum.map(maps, fn map ->
      Enum.map(keys, fn key -> Map.get(map, key) end)
    end)
  end
end
