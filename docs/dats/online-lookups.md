# Online lookups for unmatched DAT ROMs

Igir can optionally search online services when a DAT entry does not match any of the scanned files. This helps surface likely names, descriptions, or checksums to investigate before updating your collection.

## Enabling online lookups

Online lookups are disabled by default. Supply one or both of the following options to enable the integrations:

- `--enable-hasheous` — Queries [Hasheous](https://hasheous.com/) with the strongest hash found on an unmatched DAT ROM (SHA-1, then MD5, then SHA-256).
- `--igdb-client-id <id>` and `--igdb-token <token>` — Queries [IGDB](https://api-docs.igdb.com/) using the DAT ROM's description (or name if no description) as the search term.

!!! note

    IGDB requires both a client ID and an OAuth access token generated for that client. Follow IGDB's [authentication guide](https://api-docs.igdb.com/#account-creation) to create credentials and fetch a bearer token.

## How results are used

When enabled, Igir will:

1. Parse the supplied DATs and build a list of expected ROMs.
2. Compare that list to the scanned files to identify unmatched entries.
3. For each unmatched entry, query Hasheous and/or IGDB (depending on which options were provided).
4. Attach any returned metadata to the execution plan so you can review which DAT entries have online hints.

Online lookups are only requested for ROMs that do not already match by checksum or size+name. The rest of Igir's behavior is unchanged, so you can continue to copy, move, link, or report on your collection while seeing online matches alongside the plan output.

## Handling network failures

If either service is unreachable or returns an error, Igir will continue running without failing the overall execution. Any online lookups that do succeed will still be surfaced in the final plan output.
