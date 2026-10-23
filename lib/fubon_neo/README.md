# Fubon Neo SDK (Linux)

The SDK binary is not committed (gitignored). `scripts/install_colab_dependencies.py` takes it from, in order:

1. `FUBON_SDK_PATH` (a `.zip`/`.whl`, for example on Google Drive)
2. a `.zip`/`.whl` placed in this folder
3. the official download URL (v2.4.0, Linux x86_64 / aarch64)

Never put account IDs, passwords, API keys or `.pfx` certificates here.
