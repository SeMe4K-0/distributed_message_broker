defmodule Broker.Raft.Server do
  @moduledoc false
  use GenServer

  require Logger

  alias Broker.Raft.Log

  # Timing constants. Election timeout is randomized to avoid split votes.
  @election_timeout_min 500
  @election_timeout_max 1000
  @heartbeat_interval 100

  defstruct [
    # Identity
    :name,                  # registered atom name of this server (used for peer messaging)
    :node_id,               # logical node id (atom or pid) — used in voted_for / leader_node
    :peers,                 # list of {peer_name, node()} or peer_name atoms (locally)

    # Role
    role: :follower,
    leader_node: nil,

    # Persistent (in-memory for now)
    current_term: 0,
    voted_for: nil,
    log: [],

    # Volatile (all)
    commit_index: 0,
    last_applied: 0,

    # Leader-only volatile
    next_index: %{},
    match_index: %{},

    # Internal
    votes_received: nil,
    election_timer_ref: nil,
    heartbeat_timer_ref: nil,
    apply_fn: nil,
    pending_clients: %{}
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Build a registered atom name from a topic and partition. WARNING: creates atoms — only use with bounded topic/partition sets."
  @spec name(String.t(), non_neg_integer()) :: atom()
  def name(topic, partition), do: :"raft_#{topic}_p#{partition}"

  @doc "Propose a command. Only the leader accepts; non-leaders return :not_leader."
  @spec propose(GenServer.server(), term(), timeout()) :: {:ok, term()} | {:error, term()}
  def propose(server, command, timeout \\ 5000) do
    GenServer.call(server, {:propose, command}, timeout)
  end

  @doc "Returns a snapshot of the server's role / term / leader / log size."
  def get_state(server), do: GenServer.call(server, :get_state)

  @doc "Replace the peer list at runtime (used for dynamic membership in tests)."
  def set_peers(server, peers), do: GenServer.cast(server, {:set_peers, peers})

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      node_id: Keyword.get(opts, :node_id, Keyword.fetch!(opts, :name)),
      peers: Keyword.get(opts, :peers, []),
      apply_fn: Keyword.get(opts, :apply_fn, fn _cmd -> :ok end),
      votes_received: MapSet.new()
    }

    # Single-node mode: no peers → become leader immediately (quorum = 1)
    state =
      if state.peers == [] do
        become_leader(%{state | current_term: 1, voted_for: state.node_id})
      else
        reset_election_timer(state)
      end

    {:ok, state}
  end

  @impl GenServer
  def handle_call(:get_state, _from, state) do
    snapshot = %{
      role: state.role,
      term: state.current_term,
      leader: state.leader_node,
      log_size: length(state.log),
      commit_index: state.commit_index,
      last_applied: state.last_applied,
      voted_for: state.voted_for
    }

    {:reply, snapshot, state}
  end

  # PROPOSE — leader path
  def handle_call({:propose, command}, from, %{role: :leader} = state) do
    new_index = Log.last_index(state.log) + 1
    entry = %{term: state.current_term, index: new_index, command: command}
    new_log = state.log ++ [entry]
    new_pending = Map.put(state.pending_clients, new_index, from)
    state = %{state | log: new_log, pending_clients: new_pending}

    # Send AppendEntries to all peers immediately (don't wait for next heartbeat)
    state = broadcast_append_entries(state)

    # In single-node case (no peers), commit immediately
    state = advance_commit_index(state)

    {:noreply, state}
  end

  # PROPOSE — non-leader: return current leader hint
  def handle_call({:propose, _command}, _from, state) do
    {:reply, {:error, {:not_leader, state.leader_node}}, state}
  end

  @impl GenServer
  def handle_cast({:set_peers, peers}, state) do
    {:noreply, %{state | peers: peers}}
  end

  # -------------------------- RequestVote --------------------------

  def handle_cast(
        {:request_vote, from, term, candidate_node, last_log_index, last_log_term},
        state
      ) do
    state = maybe_step_down(state, term)

    can_grant =
      term >= state.current_term and
        (state.voted_for == nil or state.voted_for == candidate_node) and
        log_up_to_date?(state.log, last_log_index, last_log_term)

    state =
      if can_grant do
        %{state | voted_for: candidate_node, current_term: term}
        |> reset_election_timer()
      else
        state
      end

    reply = {:request_vote_reply, state.node_id, state.current_term, can_grant}
    send_rpc(from, reply)
    {:noreply, state}
  end

  def handle_cast({:request_vote_reply, from_node, term, granted}, state) do
    cond do
      term > state.current_term ->
        {:noreply, step_down(state, term)}

      state.role == :candidate and granted and term == state.current_term ->
        new_votes = MapSet.put(state.votes_received, from_node)
        total = length(state.peers) + 1
        majority = div(total, 2) + 1

        if MapSet.size(new_votes) + 1 >= majority do
          # +1 for the candidate's own vote
          {:noreply, become_leader(%{state | votes_received: new_votes})}
        else
          {:noreply, %{state | votes_received: new_votes}}
        end

      true ->
        {:noreply, state}
    end
  end

  # -------------------------- AppendEntries --------------------------

  def handle_cast(
        {:append_entries, from, term, leader_node, prev_log_index, prev_log_term, entries,
         leader_commit},
        state
      ) do
    state = maybe_step_down(state, term)

    cond do
      term < state.current_term ->
        send_rpc(from, {:append_entries_reply, state.node_id, state.current_term, false, 0})
        {:noreply, state}

      true ->
        state =
          %{state | leader_node: leader_node, role: :follower}
          |> reset_election_timer()

        log_ok =
          prev_log_index == 0 or
            case Log.at(state.log, prev_log_index) do
              %{term: ^prev_log_term} -> true
              _ -> false
            end

        if log_ok do
          new_log = Log.truncate_after(state.log, prev_log_index) ++ entries
          new_commit = min(leader_commit, Log.last_index(new_log))
          state = %{state | log: new_log, commit_index: new_commit}
          state = apply_committed(state)

          match_index = prev_log_index + length(entries)

          send_rpc(
            from,
            {:append_entries_reply, state.node_id, state.current_term, true, match_index}
          )

          {:noreply, state}
        else
          send_rpc(
            from,
            {:append_entries_reply, state.node_id, state.current_term, false, 0}
          )

          {:noreply, state}
        end
    end
  end

  def handle_cast({:append_entries_reply, from_node, term, success, match_index}, state) do
    cond do
      term > state.current_term ->
        {:noreply, step_down(state, term)}

      state.role == :leader and success ->
        state = %{
          state
          | match_index: Map.put(state.match_index, from_node, match_index),
            next_index: Map.put(state.next_index, from_node, match_index + 1)
        }

        {:noreply, advance_commit_index(state)}

      state.role == :leader and not success ->
        # Decrement next_index for this follower and retry
        new_next = Map.update(state.next_index, from_node, 1, fn x -> max(1, x - 1) end)
        state = %{state | next_index: new_next}
        send_append_entries_to(from_node, state)
        {:noreply, state}

      true ->
        {:noreply, state}
    end
  end

  # -------------------------- Timeouts --------------------------

  @impl GenServer
  def handle_info(:election_timeout, %{role: role} = state) when role != :leader do
    {:noreply, start_election(state)}
  end

  def handle_info(:election_timeout, state), do: {:noreply, state}

  def handle_info(:heartbeat, %{role: :leader} = state) do
    state = broadcast_append_entries(state)
    {:noreply, schedule_heartbeat(state)}
  end

  def handle_info(:heartbeat, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Role transitions
  # ---------------------------------------------------------------------------

  defp start_election(state) do
    new_term = state.current_term + 1
    Logger.debug("[Raft #{inspect(state.name)}] Starting election for term #{new_term}")

    state = %{
      state
      | role: :candidate,
        current_term: new_term,
        voted_for: state.node_id,
        leader_node: nil,
        votes_received: MapSet.new()
    }

    last_idx = Log.last_index(state.log)
    last_trm = Log.last_term(state.log)

    for peer <- state.peers do
      send_rpc(peer, {:request_vote, state.name, new_term, state.node_id, last_idx, last_trm})
    end

    state =
      if state.peers == [] do
        become_leader(state)
      else
        reset_election_timer(state)
      end

    state
  end

  defp become_leader(state) do
    Logger.debug("[Raft #{inspect(state.name)}] BECAME LEADER for term #{state.current_term}")

    next_idx = Log.last_index(state.log) + 1
    next_index = Map.new(state.peers, fn p -> {peer_node_id(p), next_idx} end)
    match_index = Map.new(state.peers, fn p -> {peer_node_id(p), 0} end)

    state = %{
      state
      | role: :leader,
        leader_node: state.node_id,
        next_index: next_index,
        match_index: match_index,
        election_timer_ref: cancel_timer(state.election_timer_ref)
    }

    state = broadcast_append_entries(state)
    state = schedule_heartbeat(state)
    advance_commit_index(state)
  end

  defp maybe_step_down(state, incoming_term) when incoming_term > state.current_term do
    step_down(state, incoming_term)
  end

  defp maybe_step_down(state, _), do: state

  defp step_down(state, term) do
    state = %{
      state
      | role: :follower,
        current_term: term,
        voted_for: nil,
        leader_node: nil,
        heartbeat_timer_ref: cancel_timer(state.heartbeat_timer_ref)
    }

    reset_election_timer(state)
  end

  # ---------------------------------------------------------------------------
  # Replication helpers
  # ---------------------------------------------------------------------------

  defp broadcast_append_entries(state) do
    for peer <- state.peers do
      send_append_entries_to(peer_node_id(peer), state)
    end

    state
  end

  defp send_append_entries_to(peer_node_id, state) do
    peer = find_peer(state.peers, peer_node_id)
    next_idx = Map.get(state.next_index, peer_node_id, 1)
    prev_log_index = next_idx - 1
    prev_log_term =
      case Log.at(state.log, prev_log_index) do
        nil -> 0
        %{term: t} -> t
      end

    entries = Log.entries_from(state.log, next_idx)

    send_rpc(
      peer,
      {:append_entries, state.name, state.current_term, state.node_id, prev_log_index,
       prev_log_term, entries, state.commit_index}
    )
  end

  # commit_index advances when an index is replicated on a majority of nodes,
  # AND the entry's term equals current_term (Raft safety property).
  defp advance_commit_index(%{role: :leader} = state) do
    own_index = Log.last_index(state.log)
    indices = [own_index | Map.values(state.match_index)]
    sorted = Enum.sort(indices, :desc)
    total = length(state.peers) + 1
    majority = div(total, 2) + 1

    candidate = Enum.at(sorted, majority - 1, 0)

    new_commit =
      cond do
        candidate <= state.commit_index ->
          state.commit_index

        true ->
          case Log.at(state.log, candidate) do
            %{term: t} when t == state.current_term -> candidate
            _ -> state.commit_index
          end
      end

    state = %{state | commit_index: new_commit}
    apply_committed(state)
  end

  defp advance_commit_index(state), do: state

  defp apply_committed(state) do
    if state.last_applied < state.commit_index do
      next_index = state.last_applied + 1
      entry = Log.at(state.log, next_index)
      result = state.apply_fn.(entry.command)

      # Reply to pending client (if any)
      case Map.get(state.pending_clients, next_index) do
        nil ->
          :ok

        from ->
          GenServer.reply(from, {:ok, result})
      end

      state = %{
        state
        | last_applied: next_index,
          pending_clients: Map.delete(state.pending_clients, next_index)
      }

      apply_committed(state)
    else
      state
    end
  end

  # Candidate's log is at-least-as-up-to-date as ours?
  defp log_up_to_date?(log, candidate_last_index, candidate_last_term) do
    our_last_term = Log.last_term(log)
    our_last_index = Log.last_index(log)

    cond do
      candidate_last_term > our_last_term -> true
      candidate_last_term < our_last_term -> false
      true -> candidate_last_index >= our_last_index
    end
  end

  # ---------------------------------------------------------------------------
  # Timer helpers
  # ---------------------------------------------------------------------------

  defp reset_election_timer(state) do
    cancel_timer(state.election_timer_ref)
    ref = Process.send_after(self(), :election_timeout, random_election_timeout())
    %{state | election_timer_ref: ref}
  end

  defp schedule_heartbeat(state) do
    cancel_timer(state.heartbeat_timer_ref)
    ref = Process.send_after(self(), :heartbeat, @heartbeat_interval)
    %{state | heartbeat_timer_ref: ref}
  end

  defp cancel_timer(nil), do: nil

  defp cancel_timer(ref) when is_reference(ref) do
    Process.cancel_timer(ref)
    nil
  end

  defp random_election_timeout do
    @election_timeout_min + :rand.uniform(@election_timeout_max - @election_timeout_min + 1) - 1
  end

  # ---------------------------------------------------------------------------
  # Peer helpers
  # ---------------------------------------------------------------------------

  # Peer can be:
  #   - an atom name (local same-node tests)
  #   - {name, node} tuple (cross-node production)
  # peer_node_id returns the unique identifier used in next_index/match_index/voted_for.
  # For remote peers we use the node (unique across cluster); for local atoms we use the atom.
  defp peer_node_id(peer) when is_atom(peer), do: peer
  defp peer_node_id({_name, node}) when is_atom(node), do: node

  defp find_peer(peers, target_id) do
    Enum.find(peers, fn p -> peer_node_id(p) == target_id end)
  end

  defp send_rpc(peer, msg), do: GenServer.cast(peer, msg)
end
