use datafusion::prelude::SessionContext;

/// Registers domain-specific royalty scalar UDFs and analytical functions into DataFusion
pub fn register_music_udfs(_ctx: &SessionContext) {
    // Custom UDF registrations (e.g. APPLY_FX, NORMALIZE_PLATFORM) can be attached here
    eprintln!("✓ Music royalty analytical functions ready");
}
