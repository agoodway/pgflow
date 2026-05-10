%Doctor.Config{
  ignore_modules: [
    # Phoenix scaffolding — generated, not authored, no @moduledoc by design.
    PgflowDemoWeb,
    PgflowDemoWeb.Endpoint,
    PgflowDemoWeb.ErrorHTML,
    PgflowDemoWeb.ErrorJSON,
    PgflowDemoWeb.Router,
    PgflowDemoWeb.Telemetry,
    PgflowDemo.Repo
  ],
  ignore_paths: [],
  min_module_doc_coverage: 40,
  min_module_spec_coverage: 0,
  min_overall_doc_coverage: 50,
  min_overall_moduledoc_coverage: 100,
  min_overall_spec_coverage: 0,
  exception_moduledoc_required: true,
  raise: false,
  reporter: Doctor.Reporters.Full,
  struct_type_spec_required: false,
  umbrella: false
}
