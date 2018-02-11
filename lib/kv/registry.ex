defmodule KV.Registry do
  use GenServer

  defmodule State do
    defstruct names: nil, refs: Map.new()
  end

  ## Client API

  @doc """
    Starts the registry.
  """
  def start_link(opts) do
    server = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, server, opts)
  end

  @doc """
    Looks up the bucket pid for `name` stored in `server`.

    Returns `{:ok, pid}` if the bucket exists, `:error` otherwise.
  """
  def lookup(server, name) do
    case :ets.lookup(server, name) do
      [{^name, pid}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
    Ensures there is a bucket associated with the given `name` in `server`
  """
  def create(server, name) do
    GenServer.call(server, {:create, name})
  end

  def stop(server) do
    GenServer.stop(server)
  end

  def init(table_name) do
    names = :ets.new(table_name, [:named_table, read_concurrency: true])
    {:ok, %State{names: names}}
  end

  # def handle_call({:lookup, name}, _from, %State{names: names} = state) do
  #   {:reply, Map.fetch(names, name), state}
  # end

  def handle_call({:create, name}, _from, %State{names: names, refs: refs} = state) do
    case lookup(names, name) do
      {:ok, pid} -> {:reply, pid, state}
      :error ->
        {:ok, bucket} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
        ref = Process.monitor(bucket)
        :ets.insert(names, {name, bucket})
        {:reply, bucket, %State{state | names: names, refs: Map.put(refs, ref, name)}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{names: names, refs: refs} = state) do
    {name, refs} = Map.pop(refs, ref)
    :ets.delete(names, name)
    {:noreply, %State{state | names: names, refs: refs}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end
end