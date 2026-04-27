defmodule Broker.Log.Compactor do
  @moduledoc false
  use GenServer, restart: :permanent

  require Logger

  # Default: keep segments for 7 days
  @default_retention_ms 7 * 24 * 60 * 60 * 1000
  # Check every 5 minutes
  @default_check_interval_ms 5 * 60 * 1000

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # Trigger a compaction check immediately (useful for tests)
  @spec compact(GenServer.server()) :: :ok
  def compact(server), do: GenServer.call(server, :compact)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    dir = Keyword.fetch!(opts, :dir)
    retention_ms = Keyword.get(opts, :retention_ms, @default_retention_ms)
    check_interval_ms = Keyword.get(opts, :check_interval_ms, @default_check_interval_ms)

    schedule_check(check_interval_ms)

    {:ok,
     %{
       dir: dir,
       retention_ms: retention_ms,
       check_interval_ms: check_interval_ms
     }}
  end

  @impl GenServer
  def handle_call(:compact, _from, state) do
    deleted = do_compact(state.dir, state.retention_ms)
    {:reply, {:ok, deleted}, state}
  end

  @impl GenServer
  def handle_info(:check, state) do
    do_compact(state.dir, state.retention_ms)
    schedule_check(state.check_interval_ms)
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp schedule_check(interval_ms) do
    Process.send_after(self(), :check, interval_ms)
  end

  # Delete .log and .index files whose last-modified time is older than retention.
  # The active segment is always the one with the highest base offset — never deleted.
  defp do_compact(dir, retention_ms) do
    cutoff = System.system_time(:millisecond) - retention_ms

    log_files =
      dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".log"))
      |> Enum.sort()

    # Never delete the last (active) segment
    candidates = Enum.drop(log_files, -1)

    deleted =
      Enum.filter(candidates, fn filename ->
        path = Path.join(dir, filename)
        mtime_ms = file_mtime_ms(path)
        mtime_ms < cutoff
      end)

    Enum.each(deleted, fn filename ->
      base = String.replace_suffix(filename, ".log", "")
      log_path = Path.join(dir, "#{base}.log")
      idx_path = Path.join(dir, "#{base}.index")
      File.rm(log_path)
      File.rm(idx_path)
      Logger.info("[Compactor] Deleted expired segment #{base}")
    end)

    length(deleted)
  end

  defp file_mtime_ms(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{mtime: mtime}} -> mtime * 1_000
      _ -> 0
    end
  end
end
