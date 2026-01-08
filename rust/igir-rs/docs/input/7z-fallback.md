7z listing and extraction fallback

Igir uses the system `7z` / `7za` binary when it needs to read or extract `.7z` archives. This keeps the Rust crate lightweight and avoids bundling a large native implementation.

Behavior:

- On Windows/macOS/Linux, Igir will search the PATH for `7z` or `7za` (in that order).
- If a `7z` binary is available, Igir will first run `7z l <archive>` and attempt to parse the human-readable listing. It uses a heuristics-based parser to find the `Name` column and extract filenames.
- If listing fails or parsing produces no entries, Igir falls back to extracting the archive to a temporary directory using `7z x -o<tempdir> -y` and then scans the extracted files. This fallback ensures archives with unusual listing formats or non-ASCII filenames are handled correctly.

Notes for users:

- The `7z` binary must be available in PATH for `.7z` archive support. If it is not present, Igir will skip `.7z` archives (no error), and will continue to scan other inputs.
- Because `7z` is invoked as an external process, scanning large `.7z` archives can be slower and may use more temporary disk space.
- Integration tests that exercise `.7z` behavior are conditional and will be skipped if `7z` is not available on the system running the tests.

If you need native `.7z` handling without an external binary, consider enabling or adding a Rust crate that directly supports the 7z format; this project intentionally prefers the external-binary approach for portability and to avoid large binary dependencies.
