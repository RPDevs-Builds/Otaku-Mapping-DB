# Otaku Mapping DB (`RPDevs-Builds/Otaku-Mapping-DB`)

Automated, indexed, cryptographically verified anime cross-platform mapping database forked from [Goldenfreddy0703/Otaku-Mappings](https://github.com/Goldenfreddy0703/Otaku-Mappings) for **Otaku-RPDev**.

Rather than committing static SQLite binaries into Git history, a daily GitHub Actions pipeline (`.github/workflows/update-mappings.yml`) rebuilds, validates, indexes, compresses (`.xz` ~5.8 MB), and publishes `anime_mappings.db` cleanly to rolling `latest` and dated `vYYYY.MM.DD` GitHub Releases.

---

## Automated Sources & Resolution Pipeline

1. **Seed Continuity & Shell Pruning**: Preserves historical `trakt_id`, legacy curations (e.g. `thetvdb_season = 'a'`), and baseline fallback metadata while stripping dirty sentinels (`''`, non-numeric strings in `thetvdb_id`) and pruning empty orphaned seed shells (`>64000` phantom IDs after deduplication).
2. **Manami Anime Offline Database** (`manami-project/anime-offline-database`): Base catalog (`mal_id`, `anilist_id`, `kitsu_id`, `anidb_id`, `simkl_id`), canonical titles, relative MAL picture suffixes, duration (seconds), median score, status, episode counts, and franchise `relatedAnime` graph.
3. **kawaiioverflow/arm** (`kawaiioverflow/arm`): Additional `mal_id <-> anilist_id` cross-reference backfill prior to downstream merges.
4. **Fribb Anime Lists** (`Fribb/anime-lists`): Cross-references `thetvdb_id`, `themoviedb_id` (`tv` / `movie`), `imdb_id`, seasons, and episode offsets.
5. **Anime-Lists XML** (`Anime-Lists/anime-lists`): `anidbid`-keyed TVDB/TMDB season/offset defaults and `<mapping-list>` episode split rules into `tvdb_rules`.
6. **Kometa Anime-IDs** (`Kometa-Team/Anime-IDs`), **PlexAniBridge-Mappings** (`eliasbenb/PlexAniBridge-Mappings` v2), **Wikidata SPARQL** (`P4086` <-> IMDb/TVDB/TMDb), **AniDB Official Titles** (`anime-titles.xml.gz`), & **MAL-Dubs** (`MAL-Dubs/MAL-Dubs`): Backfills missing cross-platform IDs, missing TVDB seasons, missing titles, and `mal_dub_id`.
7. **Incremental Cached Live API Hydration** (`actions/cache` delta-only):
   - **AniList GraphQL** (`graphql.anilist.co` batches of 50): Fills airing/recent/untitled metadata (`format`, `status`, `title`, `episodes`, `season`, `seasonYear`) and `PREQUEL` relation edges.
   - **AniZip** (`api.ani.zip/mappings?mal_id=...`): Direct `thetvdb_id`, `themoviedb_id`, `imdb_id`, episode count, and `seasonNumber` for newly airing shows.
   - **MALSync Rules** (`api.malsync.moe/tvdb/rules/cache-key/...`): Populates split-cour and `absoluteStart` episode rules into `tvdb_rules` across MAL and AniList franchise entries.
   - **Simkl Extended API** (`api.simkl.com`) & **Trakt Search API** (`TRAKT_CLIENT_ID`): Fills remaining missing `simkl_id`, `thetvdb_id`, `themoviedb_id`, `imdb_id`, and media-kind-scoped `trakt_id`.
   - **AnimeFillerList** (`animefillerlist.com/shows`): Pre-computes non-canon episode intervals (`Filler`, `Mixed Canon/Filler`, `Recap`) mapped across `TV`/`ONA` `SEQUEL` and shared-TVDB chains into `filler_ranges`.
   - **Transitive Prequel Inheritance**: Propagates unambiguous franchise `thetvdb_id` / `themoviedb_id` / `imdb_id` down direct TV sequel chains.

---

## Schema & Performance Enhancements

- **Backward-Compatible Extended `anime` Table (`v2.4`–`v2.6`)**: Preserves all 21 core columns (`mal_id`, `mal_dub_id`, `anilist_id`, `kitsu_id`, `anidb_id`, `simkl_id`, `thetvdb_id`, `themoviedb_id`, `imdb_id`, `trakt_id`, `mal_title`, `mal_picture`, `thetvdb_season`, `thetvdb_part`, `duration`, `score`, `status`, `anime_media_type`, `global_media_type`, `anime_media_episodes`, `global_media_episodes`), adds indexed `year` (`INTEGER`) and `anime_season` (`WINTER`/`SPRING`/`SUMMER`/`FALL`), and in **`v2.6`** fixes `imdb_id` to `TEXT`, adds `episodes` (`INTEGER`), `title_english`, `genres`, `studios`, `start_date`, and `trakt_slug`, preserves shared `anidb_id`/`simkl_id`/`anilist_id`/`kitsu_id` across multi-cour siblings, and eliminates same-kind Trakt IMDb/TMDb collisions.
- **Zero Full-Table Scans**: Composite indexes on `(year, anime_season, anime_media_type, score DESC, mal_id DESC)`, `(thetvdb_id, thetvdb_season, thetvdb_part)`, `(anidb_id, mal_id)`, `(anilist_id, mal_id)`, `(kitsu_id, mal_id)`, `(simkl_id, mal_id)`, `themoviedb_id`, `imdb_id`, `trakt_id`, `mal_dub_id`, and `mal_title` (`ANALYZE` + `VACUUM` for `file:...db?mode=ro` readers).
- **Additive Tables (`v2.2`–`v2.6`)**:
  - `anime_fts` (`v2.6`): SQLite FTS5 (`unicode61 remove_diacritics 2`) virtual table indexed by `rowid = mal_id` over `mal_title`, `title_english`, and `aliases_text` for instant sub-millisecond full-text & diacritic-insensitive offline search.
  - `tvdb_rules`: Split-cour, special, and `absolute_start` episode mapping rules from `Anime-Lists` XML, `PlexAniBridge`, and `MALSync`.
  - `anime_aliases`: Compact `WITHOUT ROWID` indexed multi-language alternate titles (`Manami` synonyms, `AniDB` `x-jat`/`en`/`short`, `AniList` English/Romaji/synonyms) (`ix_aliases_title`).
  - `franchise_links`: Compact `WITHOUT ROWID` bidirectional franchise graph (`PREQUEL`, `SEQUEL`, `SIDE_STORY`, `SPIN_OFF`, `PARENT`, `ALTERNATIVE`, `SUMMARY`, `RELATED`) (`ix_franchise_rel`).
  - `schedule_ids`: `syobocal_tid` & `annict_id` Japanese broadcast schedule cross-references from `kawaiioverflow/arm`.
  - `filler_ranges`: Compact `WITHOUT ROWID` non-canon episode ranges (`(mal_id, ep_start, ep_end, filler_type)`) (`v2.3`).
  - `build_meta`: Schema version, UTC build timestamp, row/rule/alias/franchise/filler totals, and JSON coverage summary.

---

## Release Assets (`releases/latest`)

Each scheduled run uploads:
- `anime_mappings.db.xz` (~5.8 MB LZMA compressed)
- `anime_mappings.db` (uncompressed SQLite DB)
- `manifest.json` (timestamps, row counts, coverage metrics, SHA-256 digests)
- `SHA256SUMS.txt` / `MD5SUMS.txt`

Direct rolling URLs:
- `https://github.com/RPDevs-Builds/Otaku-Mapping-DB/releases/download/latest/anime_mappings.db.xz`
- `https://github.com/RPDevs-Builds/Otaku-Mapping-DB/releases/download/latest/manifest.json`

---

## Local Development & Verification

```bash
# Run hermetic offline unit tests
python3 -W default -m unittest discover -s tests -p "test_*.py" -v

# Build locally using cached/live feeds + seed preservation
python3 scripts/build_mappings.py --seed .seed/v23_baseline.db --output dist/anime_mappings.db --compress-xz --manifest dist/manifest.json --cache-dir .cache

# Run strict integrity, index query-plan, hygiene & baseline monotone validation
python3 scripts/validate_mappings.py --db dist/anime_mappings.db --baseline .seed/v23_baseline.db --xz dist/anime_mappings.db.xz --manifest dist/manifest.json --min-rows 30000 --require-canonical-spots
```
