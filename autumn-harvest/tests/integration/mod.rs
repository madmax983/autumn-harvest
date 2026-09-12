#![allow(dead_code)]

#[cfg(feature = "db")]
mod active_workflow_gauge_tests;
mod activity_default_floor_tests;
#[cfg(feature = "db")]
mod activity_enqueue_batch_perf;
mod activity_failure_tests;
#[cfg(feature = "db")]
mod activity_info_tests;
mod activity_interceptor_tests;
mod activity_outcome_metrics_tests;
#[cfg(feature = "db")]
mod activity_pause_tests;
mod admission_gate_authoritative_tests;
mod admission_gate_tests;
mod alert_pack_docs;
mod audit_export_docs;
#[cfg(feature = "db")]
mod audit_export_tests;
mod audit_tests;
#[cfg(feature = "db")]
mod auto_heartbeat_tests;
mod awaitables_tests;
#[cfg(all(feature = "db", feature = "testing"))]
mod backup_verify_tests;
mod benchmarks_docs;
#[cfg(feature = "db")]
mod build_routing_tests;
#[cfg(feature = "testing")]
mod business_day_replay_tests;
#[cfg(feature = "db")]
mod business_day_timer_tests;
mod cache_delta_load_tests;
#[cfg(feature = "db")]
mod canary_tests;
mod cancellation_tests;
#[cfg(feature = "db")]
mod capability_miss_tests;
#[cfg(feature = "db")]
mod chain_timeout_tests;
mod chaos_catalogue_drift;
mod chaos_docs;
#[cfg(feature = "chaos")]
mod chaos_tests;
mod child_fanout_tests;
mod child_policy_tests;
#[cfg(feature = "db")]
mod child_timeout_tests;
mod ci_run_coverage;
mod circuit_breaker_wiring_tests;
mod claim_bench_support;
#[cfg(feature = "db")]
mod claim_budget_tests;
#[cfg(all(feature = "db", feature = "testing"))]
mod codec_rotation_db_tests;
mod completion_callback_tests;
#[cfg(feature = "db")]
mod completion_trigger_outbox_queue_perf;
mod concurrency_key_tests;
mod concurrency_supersede_tests;
mod context_headers_tests;
mod cross_region_dr_docs;
#[cfg(feature = "db")]
mod cross_region_dr_tests;
mod cross_shard_child_context_tests;
mod cross_shard_child_placement_unit;
#[cfg(feature = "db")]
mod cross_shard_children_tests;
#[cfg(feature = "db")]
mod cross_type_continue_as_new_tests;
mod cross_workflow_await_tests;
mod cross_workflow_cancel_tests;
#[cfg(feature = "db")]
mod cross_workflow_signal_tests;
#[cfg(feature = "db")]
mod ctx_info_tests;
mod dag_builder;
mod dag_compensation_tests;
mod dag_execution_timeout_tests;
mod dag_input_binding_tests;
mod dag_mapping_tests;
mod dag_signal_gate_tests;
#[cfg(all(feature = "testing", feature = "unified-dag-execution"))]
mod dag_unified_tests;
mod dashboard_pack_docs;
mod debounce_tests;
#[cfg(feature = "debugger")]
mod debugger_tests;
mod delayed_start_tests;
mod det_check_tests;
mod determinism_static_analysis_docs;
mod dispatch_tests;
mod e2e_bench_support;
mod event_batch_tests;
mod event_partitioning_tests;
mod executor_span_tests;
#[cfg(feature = "testing")]
mod external_completion_tests;
mod fanout_tests;
mod force_fail_tests;
mod guardrail_catalog_tests;
mod havoc_reentrancy;
mod havoc_tests;
mod history_ceiling_claim_tests;
mod hot_code_swap_docs;
#[cfg(all(feature = "hot-code-swap", feature = "testing"))]
mod hot_code_swap_tests;
#[cfg(feature = "testing")]
mod idempotency_tests;
mod integration_e2e;
mod legal_hold_tests;
mod lineage_store_tests;
mod macros_activity;
mod macros_collect;
#[cfg(feature = "testing")]
mod macros_compile_fail;
mod macros_dag;
#[cfg(feature = "testing")]
mod macros_query_handlers;
mod macros_webhook;
mod macros_workflow;
mod metrics_coverage;
mod metrics_integration;
mod metrics_rs_adapter;
#[cfg(feature = "db")]
mod migrate_tests;
mod migrating_from_temporal_docs;
mod migration_hygiene;
#[cfg(feature = "db")]
mod mixed_suspension_tests;
#[cfg(feature = "db")]
mod mutex_lease_reclaim_perf;
#[cfg(feature = "db")]
mod mutex_tests;
mod nd_block_tests;
mod panic_containment_tests;
mod partitioned_events_docs;
mod pause_tests;
#[cfg(feature = "testing")]
mod payload_cap_tests;
#[cfg(feature = "db")]
mod payload_offload_db_tests;
#[cfg(feature = "testing")]
mod payload_offload_replay_tests;
mod performance_docs;
mod poison_pill_tests;
mod priority_tests;
#[cfg(feature = "testing")]
mod publish_progress_tests;
mod query_deadlock;
mod query_terminal_tests;
mod query_tests;
mod queue_fairness_tests;
mod queue_pause_success_metric_tests;
mod queue_pause_tests;
mod quota_enforcement_tests;
mod quota_history_bytes_perf_tests;
mod quota_lock_ordering_tests;
mod quota_reconcile_tests;
mod quota_supersede_ordering_tests;
mod rate_limit_bucket_gc_tests;
mod rate_limit_key_tests;
mod redrive_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod replay_canary_tests;
#[cfg(feature = "testing")]
mod replay_drift_tests;
mod replay_tests;
#[cfg(feature = "testing")]
mod replay_verifier_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod replayer_integration_tests;
#[cfg(feature = "testing")]
mod replayer_tests;
mod retention_overrides_tests;
mod retention_reclaim_support;
mod retention_summary_tests;
mod retry_after_tests;
mod retry_chain_routing_tests;
mod retry_now_tests;
mod saga_tests;
mod scanner_liveness_tests;
mod scanner_tick_db_tests;
mod schedule_decisions;
mod schedule_runs_tests;
mod schedule_to_close_tests;
mod schedule_update_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod scheduled_time_tests;
mod scheduler_auto_pause_tests;
mod scheduler_bounded_runs_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod scheduler_carryover_tests;
mod scheduler_catchup_tests;
#[cfg(feature = "db")]
mod scheduler_ha_tests;
#[cfg(feature = "db")]
mod scheduler_overdue_pass_perf;
#[cfg(feature = "db")]
mod scheduler_overdue_tests;
#[cfg(feature = "db")]
mod scheduler_registration_tests;
mod security;
#[cfg(feature = "db")]
mod shard_placement_by_id_tests;
#[cfg(all(feature = "db", feature = "testing"))]
mod shard_rebalance_db_tests;
mod shard_rebalance_unit;
#[cfg(feature = "db")]
mod sharded_runtime_tests;
mod sharding_unit;
mod signal_tests;
#[cfg(feature = "db")]
mod signal_with_start_tests;
mod sla_breach_tests;
mod slot_tuner_tests;
mod sqlite_feasibility_docs;
#[cfg(feature = "db")]
mod start_idempotency_tests;
#[cfg(feature = "db")]
mod start_source_tests;
mod sticky_routing_tests;
mod telemetry_span_tests;
#[cfg(feature = "db")]
mod terminal_write_ownership_tests;
#[cfg(feature = "db")]
mod throttle_tests;
#[cfg(feature = "db")]
mod transactional_activity_tests;
#[cfg(feature = "db")]
mod transactional_start_tests;
#[cfg(feature = "db")]
mod triage_tests;
#[cfg(feature = "db")]
mod typed_stubs_tests;
mod typed_workflow_failure_tests;
mod updt_with_start_tests;
mod usage_report_activity_lookback_tests;
#[cfg(feature = "wasm-activities")]
mod wasm_activities_tests;
mod webhook_trigger_tests;
mod worker_session_tests;
#[cfg(feature = "db")]
mod workflow_handle_tests;
#[cfg(feature = "db")]
mod workflow_id_targeted_tests;
mod workflow_logger_tests;
mod workflow_logs_tests;
mod workflow_mutation_tests;
#[cfg(feature = "db")]
mod workflow_reachability_samples_tests;
mod workflow_retry_tests;
mod workflow_schema_contract_tests;
mod workflow_task_timeout_tests;
#[cfg(feature = "testing")]
mod workflow_test_env_tests;
