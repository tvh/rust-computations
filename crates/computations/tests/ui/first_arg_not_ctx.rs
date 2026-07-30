use computations::computation;

#[computation]
async fn bad_first_arg(_n: u32) -> Result<(), computations::error::CompError> {
    Ok(())
}

fn main() {}
