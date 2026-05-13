defmodule Broker.Protocol.Frame do
  @moduledoc false

  # Magic byte that starts every frame
  @magic 0x42
  @version 0x01

  def magic, do: @magic
  def version, do: @version

  # Client → Broker
  @produce 0x01
  @fetch 0x03
  @commit_offset 0x05
  @join_group 0x07
  @heartbeat 0x09
  @subscribe 0x0B
  @unsubscribe 0x0D

  # Broker → Client
  @produce_ack 0x02
  @fetch_response 0x04
  @commit_ack 0x06
  @join_ack 0x08
  @heartbeat_ack 0x0A
  @subscribe_ack 0x0C
  @record_push 0x0E
  @error 0xFF

  def produce, do: @produce
  def fetch, do: @fetch
  def commit_offset, do: @commit_offset
  def join_group, do: @join_group
  def heartbeat, do: @heartbeat
  def subscribe, do: @subscribe
  def unsubscribe, do: @unsubscribe
  def produce_ack, do: @produce_ack
  def fetch_response, do: @fetch_response
  def commit_ack, do: @commit_ack
  def join_ack, do: @join_ack
  def heartbeat_ack, do: @heartbeat_ack
  def subscribe_ack, do: @subscribe_ack
  def record_push, do: @record_push
  def error, do: @error

  # Error codes
  @err_none 0
  @err_unknown_topic 1
  @err_offset_out_of_range 2
  @err_leader_not_available 3
  @err_not_leader 4
  @err_replication_in_progress 5
  @err_invalid_request 6

  def err_none, do: @err_none
  def err_unknown_topic, do: @err_unknown_topic
  def err_offset_out_of_range, do: @err_offset_out_of_range
  def err_leader_not_available, do: @err_leader_not_available
  def err_not_leader, do: @err_not_leader
  def err_replication_in_progress, do: @err_replication_in_progress
  def err_invalid_request, do: @err_invalid_request
end
