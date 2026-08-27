defmodule PgFlow.AwaitSignalTransactionError do
  @moduledoc """
  Raised when `PgFlow.Context.await_signal/2` is called inside a caller-owned transaction.

  Parking exits the handler after committing its own database transition. An outer
  transaction would roll that transition back while the worker believed the task parked.
  """

  defexception message:
                 "PgFlow.Context.await_signal/2 cannot run inside Repo.transaction/1; " <>
                   "move the await outside the transaction"
end
