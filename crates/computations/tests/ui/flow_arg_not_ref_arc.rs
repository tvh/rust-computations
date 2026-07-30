use computations::Ctx;
use computations::computation;

struct NotAnArc;

#[computation]
async fn bad_flow(_ctx: &Ctx, #[flow] thing: NotAnArc) -> Result<(), computations::error::CompError> {
    let _ = thing;
    Ok(())
}

fn main() {}
