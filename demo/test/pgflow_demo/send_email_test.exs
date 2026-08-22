defmodule PgflowDemo.SendEmailTest do
  use ExUnit.Case, async: true

  alias PgflowDemo.Jobs.SendEmail

  test "is a one-step send_email job that returns a sent map" do
    defn = SendEmail.__pgflow_definition__()

    assert SendEmail.__pgflow_slug__() == :send_email
    assert defn.flow_type == :job
    assert Enum.map(defn.steps, & &1.slug) == [:deliver]

    result =
      SendEmail.perform(
        %{
          "to" => "demo@pgflow.dev",
          "subject" => "Welcome to PgFlow",
          "body" => "This email was enqueued as a Job."
        },
        nil
      )

    assert result == %{
             "sent" => true,
             "to" => "demo@pgflow.dev",
             "subject" => "Welcome to PgFlow"
           }
  end
end
