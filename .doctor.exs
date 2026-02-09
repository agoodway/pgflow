%Doctor.Config{
  ignore_modules: [
    # Doctor's AST traversal misanalyzes macro modules that generate functions
    # via quote blocks — it picks up `def` nodes inside quotes and counts them
    # as belonging to this module rather than the module that uses the macro.
    PgFlow.Flow,
    PgFlow.Job
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
  struct_type_spec_required: true,
  umbrella: false
}
