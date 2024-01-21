build:
	cargo build
clippy:
	cargo clippy
install:
	cargo build --release
	cargo install --path .
uninstall:
	cargo uninstall
