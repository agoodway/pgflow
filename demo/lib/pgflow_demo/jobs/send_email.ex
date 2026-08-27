defmodule PgflowDemo.Jobs.SendEmail do
  @moduledoc """
  One-off demo job. No mailer — returns a sent map so the homepage can
  teach `PgFlow.enqueue/2` and `use PgFlow.Job`.
  """
  use PgFlow.Job

  @job queue: :send_email, max_attempts: 3, timeout: 30

  perform :deliver do
    fn input, _ctx ->
      %{
        "sent" => true,
        "to" => input["to"],
        "subject" => input["subject"]
      }
    end
  end
end
