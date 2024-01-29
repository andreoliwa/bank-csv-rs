build:
	cargo build
clippy:
	cargo clippy
install:
	cargo build --release
	cargo install --path .
uninstall:
	cargo uninstall
doc:
	cargo doc --no-deps --open
test:
	cargo test
