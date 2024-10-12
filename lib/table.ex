defmodule Table do
  @moduledoc """
  Unified access to tabular data.

  This module provides a thin layer that unifies access to tabular data
  in different formats, supporting both row-based and column-based access.  This is an
  implementation of the Table.Reader protocol.
  """

  @type column :: term()
  @type tabular :: Table.Reader.t() | Table.Reader.row_reader() | Table.Reader.column_reader()

  @doc """
  Accesses tabular data as individual columns.

  ## Options

  * `:only` - specifies a subset of columns to include in the result.
  """
  @spec to_columns(tabular(), keyword()) :: [{column(), [term()]}]
  def to_columns(tabular, opts \\ []) do
    case Table.Reader.init(tabular) do
      {:columns, metadata, data} ->
        columns = filter_columns(metadata.columns, opts[:only])

        data
        |> Enum.zip(metadata.columns)
        |> Enum.filter(fn {_, col} -> col in columns end)
        |> Enum.map(fn {values, col} -> {col, values} end)

      {:rows, metadata, data} ->
        columns = filter_columns(metadata.columns, opts[:only])
        rows = Enum.to_list(data)

        for column <- columns do
          values = for row <- rows, do: Access.get(row, column)
          {column, values}
        end

      :none ->
        raise ArgumentError, "Given data is not tabular"
    end
  end

  @doc """
  Accesses tabular data as a sequence of rows.

  ## Options

  * `:only` - specifies a subset of columns to include in the result.
  """
  @spec to_rows(tabular(), keyword()) :: [[term()]] | Enumerable.t()
  def to_rows(tabular, opts \\ []) do
    case Table.Reader.init(tabular) do
      {:columns, metadata, data} ->
        columns = filter_columns(metadata.columns, opts[:only])
        column_data = Enum.to_list(data)

        column_indices =
          Enum.map(columns, &Enum.find_index(metadata.columns, fn col -> col == &1 end))

        for i <- 0..(length(List.first(column_data)) - 1) do
          for j <- column_indices do
            Enum.at(Enum.at(column_data, j), i)
          end
        end

      {:rows, metadata, data} ->
        columns = filter_columns(metadata.columns, opts[:only])

        column_indices =
          Enum.map(columns, &Enum.find_index(metadata.columns, fn col -> col == &1 end))

        Enum.map(data, fn row ->
          Enum.zip(columns, Enum.map(column_indices, &Enum.at(row, &1)))
          |> Enum.into(%{})
        end)

      :none ->
        raise ArgumentError, "Given data is not tabular"
    end
  end

  defp filter_columns(columns, only) do
    case only do
      nil -> columns
      only_columns when is_list(only_columns) -> Enum.filter(columns, &(&1 in only_columns))
      _ -> raise ArgumentError, "Invalid :only option. Expected a list of columns."
    end
  end
end

defprotocol Table.Reader do
  @moduledoc """
  Protocol for unified access to tabular data.
  """

  @type t :: term()
  @type column_reader :: {:columns, metadata(), Enumerable.t()}
  @type row_reader :: {:rows, metadata(), Enumerable.t()}
  @type metadata :: %{
          optional(:count) => non_neg_integer(),
          optional({term(), term()}) => any(),
          :columns => [Table.column()]
        }

  @spec init(t()) :: row_reader() | column_reader() | :none
  def init(tabular)
end

defimpl Table.Reader, for: List do
  def init(data) when is_list(data) and length(data) > 0 do
    cond do
      # List of matching key-value lists
      Enum.all?(data, &(is_list(&1) and Enum.all?(&1, fn {k, _v} -> is_binary(k) end))) ->
        columns = data |> List.first() |> Keyword.keys()
        {:rows, %{columns: columns, count: length(data)}, data}

      # List of matching maps
      Enum.all?(data, &is_map/1) ->
        columns = data |> List.first() |> Map.keys()
        {:rows, %{columns: columns, count: length(data)}, data}

      # List of column tuples
      Enum.all?(data, &(is_tuple(&1) and tuple_size(&1) == 2)) ->
        columns = Enum.map(data, &elem(&1, 0))
        column_data = Enum.map(data, &elem(&1, 1))
        {:columns, %{columns: columns, count: length(List.first(column_data))}, column_data}

      true ->
        :none
    end
  end

  def init(_), do: :none
end

defimpl Table.Reader, for: Map do
  def init(data) when is_map(data) and map_size(data) > 0 do
    if Enum.all?(data, fn {_k, v} -> is_list(v) end) do
      columns = Map.keys(data)
      column_data = Map.values(data)
      {:columns, %{columns: columns, count: length(List.first(column_data))}, column_data}
    else
      :none
    end
  end

  def init(_), do: :none
end
