use computations::Ctx;
use computations::computation;

#[computation]
async fn bad_return(_ctx: &Ctx) -> u32 {
    0
}

fn main() {}
