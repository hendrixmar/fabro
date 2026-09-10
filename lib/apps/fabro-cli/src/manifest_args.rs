use fabro_api::types;

use crate::args::PreflightArgs;

pub(crate) fn preflight_manifest_args(args: &PreflightArgs) -> Option<types::ManifestArgs> {
    let payload = types::ManifestArgs {
        auto_approve:     None,
        dry_run:          None,
        label:            Vec::new(),
        model:            args.model.clone(),
        preserve_sandbox: None,
        provider:         args.provider.clone(),
        environment:      args.environment.clone(),
        input:            args.inputs.values.clone(),
        verbose:          args.verbose.then_some(true),
    };
    (!fabro_manifest::manifest_args_is_empty(&payload)).then_some(payload)
}
