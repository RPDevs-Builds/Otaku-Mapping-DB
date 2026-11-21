# Otaku Mapping DB (`RPDevs-Builds/Otaku-Mapping-DB`)

Automated, indexed, cryptographically verified anime cross-platform mapping database forked from [Goldenfreddy0703/Otaku-Mappings](https://github.com/Goldenfreddy0703/Otaku-Mappings) for **Otaku-RPDev**.

Rather than committing static 6 MB SQLite binaries into Git history, a daily GitHub Actions pipeline (`.github/workflows/update-mappings.yml`) rebuilds, validates, indexes, compresses (`.xz` ~1.8 MB), and publishes `anime_mappings.db` cleanly to rolling `latest` and dated `vYYYY.MM.DD` GitHub Releases.

---

## Automated Sources & Resolution Pipeline

1. **Seed Continuity & Shell Pruning**: Preserves historical `trakt_id`, legacy curations (e.g. `thetvdb_season = 'a'`), and baseline fallback metadata while stripping dirty sentinels (`''`, non-numeric strings in `thetvdb_id`) and pruning empty orphaned seed shells (`>64000` phantom IDs after deduplication).
2. **Manami Anime Offline Database** (`manami-project/anime-offline-database`): Base catalog (`mal_id`, `anilist_id`, `kitsu_id`, `anidb_id`, `simkl_id`), canonical titles, relative MAL picture suffixes, duration (seconds), median score, status, episode counts, and franchise `relatedAnime` graph.
3. **kawaiioverflow/arm** (`kawaiioverflow/arm`): Additional `mal_id <-> anilist_id` cross-reference backfill prior to downstream merges.
4. **Fribb Anime Lists** (`Fribb/anime-lists`): Cross-references `thetvdb_id`, `themoviedb_id` (`tv` / `movie`), `imdb_id`, seasons, and episode offsets.
5. **Anime-Lists XML** (`Anime-Lists/anime-lists`): `anidbid`-keyed TVDB/TMDB season/offset defaults and `<mapping-list>` episode split rules into `tvdb_rules`.
6. **Kometa Anime-IDs** (`Kometa-Team/Anime-IDs`), **Wikidata SPARQL** (`P4086` <-> IMDb/TVDB/TMDb), **AniDB Official Titles** (`anime-titles.xml.gz`), & **MAL-Dubs** (`MAL-Dubs/MAL-Dubs`): Backfills missing cross-platform IDs, missing titles, and `mal_dub_id`.
7. **Incremental Cached Live API Hydration** (`actions/cache` delta-only):
   - **AniList GraphQL** (`graphql.anilist.co` batches of 50): Fills airing/recent/untitled metadata (`format`, `status`, `title`, `episodes`) and `PREQUEL` relation edges.
   - **AniZip** (`api.ani.zip/mappings?mal_id=...`): Direct `thetvdb_id`, `themoviedb_id`, `imdb_id`, episode count, and `seasonNumber` for newly airing shows.
   - **MALSync Rules** (`api.malsync.moe/tvdb/rules/cache-key/...`): Populates split-cour and `absoluteStart` episode rules into `tvdb_rules`.
   - **Simkl Extended API** (`api.simkl.com`): Fills remaining missing `simkl_id`, `thetvdb_id`, `themoviedb_id`, and `imdb_id` for recent/airing titles (guarded by `ids.mal == mal_id`).
   - **Transitive Prequel Inheritance**: Propagates unambiguous franchise `thetvdb_id` / `themoviedb_id` / `imdb_id` down direct TV sequel chains.

---

## Schema & Performance Enhancements

- **100% Backward Compatible `anime` Table**: Preserves all 21 columns (`mal_id`, `mal_dub_id`, `anilist_id`, `kitsu_id`, `anidb_id`, `simkl_id`, `thetvdb_id`, `themoviedb_id`, `imdb_id`, `trakt_id`, `mal_title`, `mal_picture`, `thetvdb_season`, `thetvdb_part`, `duration`, `score`, `status`, `anime_media_type`, `global_media_type`, `anime_media_episodes`, `global_media_episodes`).
- **Zero Full-Table Scans**: Adds B-tree indexes on `thetvdb_id`, `themoviedb_id`, `imdb_id`, `trakt_id`, `mal_dub_id`, and `mal_title`, plus `ANALYZE` statistics and `VACUUM` optimization for `file:...db?mode=ro` readers.
- **Additive Tables**:
  - `tvdb_rules`: Split-cour, special, and `absolute_start` episode mapping rules from `Anime-Lists` XML and `MALSync`.
  - `build_meta`: Schema version, UTC build timestamp, row/rule totals, and JSON coverage summary.

---

## Release Assets (`releases/latest`)

Each scheduled run uploads:
- `anime_mappings.db.xz` (~1.8 MB LZMA compressed)
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
python3 -m unittest discover -s tests -p "test_*.py" -v

# Build locally using cached/live feeds + seed preservation
python3 scripts/build_mappings.py --seed anime_mappings.db --output dist/anime_mappings.db --compress-xz --manifest dist/manifest.json --cache-dir .cache

# Run strict integrity, index query-plan, hygiene & baseline monotone validation
python3 scripts/validate_mappings.py --db dist/anime_mappings.db --baseline anime_mappings.db --xz dist/anime_mappings.db.xz --manifest dist/manifest.json --min-rows 30000 --require-canonical-spots
```
