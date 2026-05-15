defmodule Broker.Raft.Log do
  @moduledoc false
  # In-memory Raft log. Entries are stored as a list sorted by ascending index.
  # Each entry: %{term: integer(), index: integer(), command: term()}
  # Indexes start at 1 (Raft convention). Index 0 is reserved (no entry).

  @type entry :: %{term: non_neg_integer(), index: pos_integer(), command: term()}
  @type t :: [entry()]

  @spec new() :: t()
  def new(), do: []

  @spec append(t(), [entry()]) :: t()
  def append(log, entries) when is_list(entries), do: log ++ entries

  @spec last_index(t()) :: non_neg_integer()
  def last_index([]), do: 0
  def last_index(log), do: log |> List.last() |> Map.fetch!(:index)

  @spec last_term(t()) :: non_neg_integer()
  def last_term([]), do: 0
  def last_term(log), do: log |> List.last() |> Map.fetch!(:term)

  @spec at(t(), integer()) :: entry() | nil
  def at(_log, index) when index < 1, do: nil
  def at(log, index), do: Enum.find(log, &(&1.index == index))

  @spec slice(t(), pos_integer(), pos_integer()) :: [entry()]
  def slice(log, from_index, to_index) do
    Enum.filter(log, &(&1.index >= from_index and &1.index <= to_index))
  end

  @spec entries_from(t(), pos_integer()) :: [entry()]
  def entries_from(log, from_index) do
    Enum.filter(log, &(&1.index >= from_index))
  end

  # Keep entries with index <= at_index, drop the rest (used to truncate conflicts)
  @spec truncate_after(t(), non_neg_integer()) :: t()
  def truncate_after(log, at_index), do: Enum.filter(log, &(&1.index <= at_index))
end
