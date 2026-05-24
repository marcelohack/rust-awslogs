use awslogs::cli;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // Match Python: Ctrl-C prints "Closing..." and exits 0.
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        println!("Closing...");
        std::process::exit(0);
    });

    let code = cli::run().await;
    std::process::exit(code);
}
