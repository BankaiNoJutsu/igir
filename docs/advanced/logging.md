# Logging and Verbosity

Igir now treats verbosity as a progressive dial that controls both how chatty the progress bars are and how much detail is printed to stderr alongside them.

## Default (`-q`/`-v` not specified)

- Stderr shows compact `[SCAN]`, `[DAT ]`, and action bars with counts only.
- Per-record diagnostics, cache chatter, and copy/link messages stay hidden so runs remain readable.
- The execution summary at the end is still printed so you can see what happened.

## `-v` (Verbose level 1)

- Progress bars append the latest file name to give quick context.
- File-by-file actions (`Copying`, `Moving`, `Linking`, etc.) and other high-level status messages are printed once per record.
- Network/cache errors are surfaced so you know why an online lookup or cache open failed.

Use this level when you want to follow along with what Igir is doing without drowning in cache details.

## `-vv` (Verbose level 2)

- Progress bars include longer (ellipsized) paths instead of just filenames.
- Cache hits/misses, DAT/Hasheous/IGDB platform deductions, and other matching decisions are printed.
- Helpful when you need to understand why a record matched a specific DAT entry or why a cache result was used.

## `-vvv` (Verbose level 3)

- Adds trace-level information: every network lookup, cache write, and retry is logged with hashes and algorithms.
- Progress bars show full, untrimmed paths.
- Required when filing detailed bug reports because it captures the entire execution story.

## Tips

- `--quiet` still disables progress bars entirely (useful for CI logs) regardless of verbosity.
- Pair `-v`/`-vv`/`-vvv` with `--print-plan` if you want both the JSON execution plan and richer human-readable logs.
