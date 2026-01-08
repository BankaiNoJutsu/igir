# Output Path Tokens

When specifying a ROM [writing command](../commands.md) you have to specify an `--output <path>` directory. Igir has a few replaceable "tokens" that can be referenced in the `--output <path>` directory value. This can aid in sorting ROMs into a more complicated directory structure.

See [output path tokens](./path-options.md) for other options that will further sort your ROMs into subdirectories.

For example, if you want to group all ROMs based on their region, you would specify:

=== ":fontawesome-brands-windows: Windows"

    ```batch
    igir copy extract ^
      --dat *.dat ^
      --input ROMs\ ^
      --output "ROMs-Sorted\{region}\"
    ```

=== ":fontawesome-brands-apple: macOS"

    ```shell
    igir copy extract \
      --dat *.dat \
      --input ROMs/ \
      --output "ROMs-Sorted/{region}/"
    ```

=== ":simple-linux: Linux"

    ```shell
    igir copy extract \
      --dat *.dat \
      --input ROMs/ \
      --output "ROMs-Sorted/{region}/"
    ```

This might result in an output structure such as:

```text
ROMs-Sorted/
├── AUS
│   └── Pokemon Pinball (USA, Australia) (Rumble Version) (SGB Enhanced) (GB Compatible).gbc
├── EUR
│   ├── Pokemon - Blue Version (USA, Europe) (SGB Enhanced).gb
│   ├── Pokemon - Red Version (USA, Europe) (SGB Enhanced).gb
│   └── Pokemon - Yellow Version - Special Pikachu Edition (USA, Europe) (CGB+SGB Enhanced).gb
└── USA
    ├── Pokemon - Blue Version (USA, Europe) (SGB Enhanced).gb
    ├── Pokemon - Red Version (USA, Europe) (SGB Enhanced).gb
    ├── Pokemon - Yellow Version - Special Pikachu Edition (USA, Europe) (CGB+SGB Enhanced).gb
    └── Pokemon Pinball (USA, Australia) (Rumble Version) (SGB Enhanced) (GB Compatible).gbc
```

!!! note

    Tokens can resolve to multiple values for each ROM. For example, a ROM may have multiple regions or languages. This will result in the same ROM being written to multiple locations.

## DAT information

When using [DATs](../dats/introduction.md), you can make use of console & game information contained in them:

- `{datName}` the matching DAT's name, similar to how the [`--dir-dat-name` option](./path-options.md) works
- `{datDescription}` the matching DAT's description, similar to how the [`--dir-dat-description` option](./path-options.md) works
- `{region}` the game's region token (e.g. `USA`, `EUR`, `JPN`, `WORLD`). Igir derives this from
  ROM names or metadata and falls back to `unknown-region` when no region can be detected.
- `{language}` the game's language token (e.g. `EN`, `ES`, `JA`). Igir uses the first detected
  language tag and falls back to `unknown-language` when nothing is known.
- `{type}` the game's "type," one of: `Aftermarket`, `Alpha`, `Bad`, `Beta`, `BIOS`, `Demo`, `Device`, `Fixed`, `Hacked`, `Homebrew`, `Overdump`, `Pending Dump`, `Pirated`, `Prototype`, `Retail` (most games will be this), `Sample`, `Test`, `Trained`, `Translated`, `Unlicensed`
- `{category}` the game's "category" (only some DATs provide this)
- `{genre}` the game's "genre". Igir fills this using IGDB metadata when available and
  falls back to `unknown-genre` when no genre is known. When multiple genres are present the
  first reported value is used for the directory name. Runs that include `--diag` now emit an
  `igir_unknown_genres.json` file in the current working directory which lists every ROM that still
  fell back to `unknown-genre`, along with the IGDB cache status so you can investigate stubborn
  titles quickly. Each entry also records the exact IGDB query body plus the keyword list and
  selection rules (`igdb_query.body`, `igdb_query.keywords`, `igdb_query.keyword_strategy`) so you
  can see exactly what was posted to IGDB and how the search terms were derived. When a ROM already
  has a `derived_platform`, the report includes `igdb_query.platform_hint` so you know which
  platform token Igir attempted to target when re-querying IGDB.

## File information

You can use some information about the input and output file's name & location:

- `{inputDirname}` the input file's dirname (full path minus file basename)
- `{outputBasename}` the output file's basename, equivalent to `{outputName}.{outputExt}`
- `{outputName}` the output file's filename without its extension
- `{outputExt}` the output file's extension

## Specific hardware

To help sort ROMs into unique file structures for popular frontends & hardware, Igir offers a few specific tokens:

- `{adam}` the ['Adam' image](../usage/handheld/adam.md) emulator's directory for the ROM
- `{batocera}` the [Batocera](../usage/desktop/batocera.md) emulator's directory for the ROM
- `{es}` the [EmulationStation](../usage/desktop/emulationstation.md) emulator's directory for the ROM
- `{funkeyos}` the [FunKey OS](../usage/handheld/funkeyos.md) emulator's directory for the ROM
- `{jelos}` the [JELOS](../usage/handheld/jelos.md) emulator's directory for the ROM
- `{minui}` the [MinUI](../usage/handheld/minui.md) emulator's directory for the ROM
- `{mister}` the [MiSTer FPGA](../usage/hardware/mister.md) core's directory for the ROM
- `{miyoocfw}` the [MiyooCFW](../usage/handheld/miyoocfw.md) emulator's directory for the ROM
- `{onion}` the [OnionOS / GarlicOS](../usage/handheld/onionos.md) emulator's directory for the ROM
- `{pocket}` the [Analogue Pocket](../usage/hardware/analogue-pocket.md) core's directory for the ROM
- `{retrodeck}` the [RetroDECK](../usage/desktop/retrodeck.md) emulator's directory for the ROM
- `{romm}` the [RomM](../usage/desktop/romm.md) manager directory for the ROM
- `{twmenu}` the [TWiLightMenu++](../usage/handheld/twmenu.md) emulator's directory for the ROM

!!! tip

    See the `igir --help` message for the list of all replaceable tokens.
