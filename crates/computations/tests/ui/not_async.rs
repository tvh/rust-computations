use computations::Ctx;
use computations::computation;

#[computation]
fn not_async(_ctx: &Ctx) -> Result<(), computations::error::CompError> {
    Ok(())
}

fn main() {}
