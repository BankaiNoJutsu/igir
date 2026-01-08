# ROM Naming Schemes

Different DAT projects adopt their own naming conventions for ROM archives. Understanding the
shorthand that appears between brackets or parentheses makes it easier to configure Igir filters,
set priorities, and interpret reports.

- **GoodTools / TOSEC style.** Releases often keep the original scene filenames and append dense
  bracket codes that describe quality, language, region, hacks, trainers, and even disc ring codes.
  The tables below summarize the tags most players encounter, sourced from the
  [GoodTools "Good codes" reference](https://emulation.gametechwiki.com/index.php/GoodTools#Good_codes).
- **No-Intro style.** Names aim to look like retail box titles with minimal metadata. Only
  parenthetical flags such as region, languages, or version numbers are used, exactly as described
  in the [No-Intro naming convention](https://wiki.no-intro.org/index.php?title=Naming_Convention).
  Quality is implied—every dump in an official No-Intro DAT should already be verified good.

## Standard GoodTools codes

Quality and verification flags use square brackets:

- `[!]` — Verified good dump.
- `[b]` — Bad dump (header mismatch, corrupted read, etc.).
- `[f]` — Fixed dump (a patched version of a previously bad dump).
- `[!p]` — Pending dump awaiting full verification.

Modification and distribution flags describe how the file differs from the original release:

- `[h]` — Hacked (could be a trainer, intro, or content change).
- `[p]` — Pirate copy distributed outside official channels.
- `[a]` — Alternate retail version (often later retail fixes).
- `[o]` — Overdump that contains useless trailing data.
- `[t]` — Trained build that launches into a cheat menu.
- `[Unl]` — Unlicensed software.

Parenthetical tokens typically communicate geography or build metadata:

- `(J)`, `(U)`, `(E)` — Japan, USA, and Europe masters respectively. Many other territories reuse
  ISO country codes or legacy GoodTools numbers such as `(4)` for USA/Brazil NTSC.
- `(VS)` — Nintendo Vs. arcade cabinet builds.
- `(Beta)`, `(Proto)`, `(Sample)` — Development snapshots.
- `(M#)` — Multilingual releases with the specified number of selectable languages.

These codes can stack: `Mario Kart 64 (U) [f][t]` would be a US version that was fixed and trained.
Igir can prefer or exclude entries using [`filtering preferences`](./filtering-preferences.md) that
look for these tokens.

## Manufacturer revisions and program numbers

Cartridge-based systems often carry manufacturer-stamped program revisions. DAT groups expose that
information in parentheses:

- `(PRG0)`, `(PRG1)` — NES/GBA program revisions (GoodNES describes them as "Program ROM #").
  Higher numbers usually signify later fixes.
- `(Rev A)`, `(Rev B)` or `(v1.01)` — Generic version increments that appear in both GoodTools and
  No-Intro sets.
- `[Set 1]`, `[Set 2]` — Arcade-focused DATs such as MAME order revisions by popularity, not by
  chronology. "Set 1" commonly references the version most cabinets shipped with; "Set 2" could be
  either an earlier prototype or a regional variant, so always read the DAT notes.

When Igir builds reports it keeps the highest-ranked revision for each normalized title but records
why lower-ranked siblings were filtered (for example: "filtered by region/language").

## Choosing a naming scheme

- **GoodTools / TOSEC collectors** typically value preservation of every variant, so the filenames
  intentionally encode dump quality and scene metadata. Their bracket-heavy strings help
  [`--allow-excess-sets`](./sets.md#allowing-inexact-sets) workflows where you only keep one archive
  per game even if it contains multiple alternates.
- **No-Intro collectors** prioritize clean, retail-style names that are easy for frontends to parse.
  Because quality is already vetted, flags such as `[!]` would be redundant; instead you will see
  `(World)` regions, `(En,Ja)` language lists, `(Beta)` statuses, and explicit `Rev 1` markers.

Igir does not force you to pick one strategy, but it is helpful to know which DAT family you feed it
so you can interpret the metadata-heavy paths it generates and configure filters appropriately.

## References

1. Emulation General Wiki — [GoodTools "Good codes"](https://emulation.gametechwiki.com/index.php/GoodTools#Good_codes) (accessed 2 Dec 2025).
2. No-Intro Wiki — [Naming Convention](https://wiki.no-intro.org/index.php?title=Naming_Convention) (accessed 2 Dec 2025).
