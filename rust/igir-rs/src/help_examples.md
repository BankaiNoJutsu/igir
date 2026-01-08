Igir (Rust) — Examples and extended help

This text is embedded into the CLI `--help` output for the Rust rewrite. It mirrors
the curated examples and common usage scenarios found in the Node.js project so
users get useful, copy/paste-ready commands from `igir --help`.

Common quick examples (replace paths as needed)

- Copy new ROMs into a collection and delete unknown files:

  igir copy clean --dat "*.dat" --input "New-ROMs/" --input "ROMs/" --output "ROMs/"

- Organize and zip an existing ROM collection:

  igir move zip --dat "*.dat" --input "ROMs/" --output "ROMs/"

- Generate a report on an existing ROM collection without copying/moving files:

  igir report --dat "*.dat" --input "ROMs/"

- Produce a 1G1R set per console, preferring English ROMs from USA>WORLD>EUR>JPN:

  igir copy --dat "*.dat" --input "**/*.zip" --output "1G1R/" --dir-dat-name --single --prefer-language EN --prefer-region USA,WORLD,EUR,JPN

- Copy all Mario/Metroid/Zelda games to one directory (regex filtering):

  igir copy --input "ROMs/" --output "Nintendo/" --filter-regex "/(Mario|Metroid|Zelda)/i"

- Copy BIOS files into one directory, extracting archives when needed:

  igir copy extract --dat "*.dat" --input "**/*.zip" --output "BIOS/" --only-bios

- Re-build a MAME ROM set for a specific MAME version:

  igir copy zip --dat "MAME 0.258.dat" --input "MAME/" --output "MAME-0.258/" --merge-roms split

- Copy ROMs to an Analogue Pocket and test they were written correctly:

  igir copy extract test --dat "*.dat" --input "ROMs/" --output "/Assets/{pocket}/common/" --dir-letter

Notes on online lookups and cache

- To enable Hasheous checksum lookups: add `--enable-hasheous`.
- To enable IGDB name lookups: provide `--igdb-client-id <id>` and `--igdb-token <token>`.
- To use a persistent SQLite cache explicitly: `--cache-db <PATH>`.
- To run offline and forbid network lookups: `--cache-only` (useful with a pre-populated cache).

Filtering and one-game-per-ROM (1G1R)

- Use `--single` plus `--prefer-*` flags to produce one preferred ROM per parent set.
- Example prefer order:
  - `--prefer-language EN`
  - `--prefer-region USA,WORLD,EUR`

More help

The CLI includes many flags for output formatting, zipping, archive handling and
advanced token-based output paths. Run `igir --help` to see all flags and short
examples. The Rust help text is intentionally longer than many tools to provide
practical copy/paste snippets for common workflows.
