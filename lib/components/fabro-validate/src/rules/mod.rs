use fabro_llm::lithos_catalog::Catalog;
mod all_conditional_edges;
mod auto_status_deprecated;
mod backend_valid;
mod command_requires_script;
mod condition_syntax;
mod direction_valid;
mod edge_target_exists;
mod exit_no_outgoing;
mod fidelity_valid;
mod for_each_contract;
mod freeform_edge_count;
mod goal_gate_has_retry;
mod import_error;
mod inert_attribute;
mod join_policy_removed;
mod model_support;
mod node_model_known;
mod on_failure_valid;
mod orphan_custom_outcome;
mod parallel_branch;
mod parallel_branch_inert_attribute;
mod prompt_on_llm_nodes;
mod random_selection_no_conditions;
mod reachability;
mod reserved_keyword_node_id;
mod retry_target_exists;
mod script_absolute_cd;
mod selection_valid;
mod start_no_incoming;
mod start_node;
mod stdin_source_valid;
mod stylesheet_model_known;
mod stylesheet_syntax;
mod terminal_node;
#[cfg(test)]
pub(crate) mod test_support;
mod thread_id_requires_fidelity_full;
mod type_known;
mod unresolved_file_ref;

use crate::LintRule;

/// Returns all built-in lint rules.
#[must_use]
pub fn built_in_rules() -> Vec<Box<dyn LintRule>> {
    vec![
        start_node::rule(),
        terminal_node::rule(),
        reachability::rule(),
        edge_target_exists::rule(),
        start_no_incoming::rule(),
        exit_no_outgoing::rule(),
        condition_syntax::rule(),
        stylesheet_syntax::rule(),
        type_known::rule(),
        backend_valid::rule(),
        fidelity_valid::rule(),
        retry_target_exists::rule(),
        goal_gate_has_retry::rule(),
        prompt_on_llm_nodes::rule(),
        freeform_edge_count::rule(),
        for_each_contract::rule(),
        stdin_source_valid::rule(),
        direction_valid::rule(),
        reserved_keyword_node_id::rule(),
        all_conditional_edges::rule(),
        orphan_custom_outcome::rule(),
        on_failure_valid::rule(),
        auto_status_deprecated::rule(),
        script_absolute_cd::rule(),
        command_requires_script::rule(),
        import_error::rule(),
        join_policy_removed::rule(),
        unresolved_file_ref::rule(),
        thread_id_requires_fidelity_full::rule(),
        selection_valid::rule(),
        random_selection_no_conditions::rule(),
        inert_attribute::rule(),
        parallel_branch_inert_attribute::rule(),
    ]
}

/// Returns lint rules that require the caller's resolved model catalog.
#[must_use]
pub fn catalog_rules(catalog: &Catalog) -> Vec<Box<dyn LintRule + '_>> {
    vec![
        stylesheet_model_known::rule(catalog),
        node_model_known::rule(catalog),
    ]
}
