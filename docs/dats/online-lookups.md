# Online lookups for unmatched input ROMs

Igir can optionally search online services whenever a scanned file fails to match any entry in the provided DATs. This helps surface likely names, descriptions, or checksums to investigate before updating your collection.

## Enabling online lookups

Online lookups are disabled by default. Supply one or both of the following options to enable the integrations:

- `--enable-hasheous` — Queries [Hasheous](https://hasheous.org/) with the strongest hash found on the unmatched input file (SHA-1, then MD5, then SHA-256).
- `--igdb-client-id <id>` and `--igdb-token <token>` — Queries [IGDB](https://api-docs.igdb.com/) using the normalized filename as the search term.
- `--igdb-mode <best-effort|always|off>` — Controls when IGDB lookups run. `best-effort` (default) only queries when a platform still needs to be inferred, `always` keeps querying until genres are filled, and `off` disables IGDB even if credentials are supplied.

!!! note

    IGDB requires both a client ID and an OAuth access token generated for that client. Follow IGDB's [authentication guide](https://api-docs.igdb.com/#account-creation) to create credentials and fetch a bearer token.

## How results are used

When enabled, Igir will:

1. Parse the supplied DATs and build a list of expected ROMs.
2. Compare that list to the scanned files to find inputs that did not match by checksum or size+name.
3. For each unmatched input, query Hasheous and/or IGDB (depending on which options were provided).
4. Attach any returned metadata to the execution plan so you can review which files have online hints to help you place them in a DAT.
Online lookups are only requested for ROMs that do not already match by checksum or size+name. The rest of Igir's behavior is unchanged, so you can continue to copy, move, link, or report on your collection while seeing online matches alongside the plan output.

If you need the JSON plan on stdout for a particular run, pass `--print-plan` to opt into the verbose execution summary while still storing online hints in the optional `online_matches.json` artifact.

Need to review the DAT entries that are still unmatched? Use `--list-unmatched-dats` to include the full missing list in the execution plan JSON; otherwise only matched entries are emitted for brevity.

## IGDB search strategy

The IGDB integration only falls back to these online queries when a ROM still needs platform or genre context, but the search itself is intentionally layered to avoid brittle single-term lookups:

- Igir normalizes the filename into an `IgdbQueryPlan`, removing extensions, bracketed region tags, and obvious noise (regions, revisions, standalone years).
- The first search term uses the full normalized phrase combined with a strict `where platforms.slug = "<token>"` clause when we know the IGDB slug for the derived platform. This keeps noisy cross-platform franchises from hijacking the results.
- If that filtered query returns nothing, the same term is retried without the platform restriction, followed by progressively shorter phrases (dropping trailing keywords) and, for very short titles, single-keyword probes.
- When a RomM token does not have a known IGDB slug, we fall back to a wildcard match against the platform display name, ensuring handheld vs. console releases are still biased toward the intended hardware.

This ladder mirrors what experienced IGDB users do manually: start with the most precise search, relax it only when needed, and keep the platform constraint as long as the service can honor it.

## Handling network failures

If either service is unreachable or returns an error, Igir will continue running without failing the overall execution. Any online lookups that do succeed will still be surfaced in the final plan output.
