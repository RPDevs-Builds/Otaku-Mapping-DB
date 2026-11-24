# Otaku-Mapping-DB Project Rules & Architectural Invariants

## 1. Primary Purpose & Logic Integration with `Otaku-RPDev`
- **Primary Purpose**:
  - `Otaku-Mapping-DB` is the offline, zero-latency cross-platform service-interconnect, seasonal/episode routing, and metadata correlation engine for `/mnt/sharedroot/projects/RPDevs-Builds/Otaku-RPDev/` (`plugin.video.otaku.rpdev`).
  - Its primary artifact (`dist/anime_mappings.db.xz`) is synced into `Otaku-RPDev` as `control.mappingDB` (`resources/data/mappings.db`), and its correlated metadata artifact (`dist/anime_metadata.db.xz`) maps to `control.metadataDB` (`resources/data/anime_metadata.db`), eliminating runtime network round-trips across 12+ external anime and media APIs (`MAL`, `AniList`, `Kitsu`, `AniDB`, `Simkl`, `TVDB`, `TMDB`, `IMDb`, `Trakt`, `MAL-Dubs`, `Syobocal`, `Annict`, `AnimeFillerList`) during Kodi directory rendering, source scraping, playback initialization, skip-time resolution, and watchlist scrobbling on low-RAM streaming devices.
- **Distribution, Manifest Polling & Read-Only SQLite URI Contract**:
  - `Otaku-RPDev` (`service.py` and `routes/tools.py`) polls `https://github.com/RPDevs-Builds/Otaku-Mapping-DB/releases/download/latest/manifest.json` (`sha256`, `generated_at_utc`, `schema_version`), downloads `anime_mappings.db.xz` (and optionally `anime_metadata.db.xz`), verifies the SHA-256 digest, and atomically decompresses to `control.mappingDB` / `control.metadataDB`.
  - All runtime queries in `Otaku-RPDev` (`resources/lib/ui/database.py`) open `control.mappingDB` and `control.metadataDB` strictly in read-only URI mode (`file:...db?mode=ro` via `SQL(path, is_readonly=True)` with isolated `:ro` connection cache keys). Therefore, all published SQLite databases must be finalized with `PRAGMA journal_mode = DELETE`, `ANALYZE`, and `VACUUM` so read-only URI readers never require `-wal`/`-shm` sidecars or trigger table scans.
- **`Otaku-RPDev` Subsystem Consumers & Schema Contracts**:
  1. **Cross-Platform ID Interconnect (`database.get_mappings(anime_id, send_id)`, `get_mal_ids()`, `resolve_mappings_with_fallback()`)**:
     - Translates any primary ID (`mal_id`, `anilist_id`, `kitsu_id`, `anidb_id`, `simkl_id`, `thetvdb_id`, `themoviedb_id`, `imdb_id`, `trakt_id`, `mal_dub_id`) across `AniListBrowser.py`, `MalBrowser.py`, `OtakuBrowser.py`, `MetaBrowser.py`, `WatchlistFlavor/*` (`AniList`, `MAL`, `Simkl`, `Kitsu`, `Trakt`), `indexers/simkl.py`, `indexers/tmdb.py`, `indexers/tvdb.py`, `indexers/fanart.py`, and `subtitles/meta.py`.
     - Multi-cour siblings sharing an external ID (`anidb_id`, `simkl_id`, `anilist_id`, `kitsu_id`) rely on composite `(col, mal_id)` indexes so `ORDER BY mal_id ASC LIMIT 1` deterministically resolves the primary entry first.
  2. **Bidirectional Seasonal & Split-Cour Episode Routing (`tvdb_rules`, `thetvdb_season`, `thetvdb_part`, `anime_media_episodes`, `global_media_episodes`)**:
     - **Forward (`MAL Episode E -> TVDB/Trakt Season S, Episode T`)**: `resources/lib/ui/player.py:resolve_trakt_season_episode()` and `database.py:get_episode_offset_from_mappings()` query `tvdb_rules` (`WHERE mal_id=? ORDER BY season, range_start`) to translate canonical MAL episode numbers into TVDB/TMDB/Trakt `(season, episode)` coordinates for 1-to-1 seasons (`range_start == ep_start`), split-cour Part 2 seasons (`range_start > ep_start`), and multi-season long-running single MAL IDs (`ep_start > range_start`, e.g. *One Piece* `mal_id=21`).
     - **Reverse (`TMDBHelper / External S, E -> MAL ID & Episode E`)**: `resources/lib/routes/playback.py` queries `tvdb_rules` (`WHERE season=? AND range_start <= ? <= range_end`) to map external `S{season}E{episode}` playback requests back to `(mal_id, ep_start + episode - range_start)`.
     - **Part & Cour Filtering**: `resources/lib/ui/divide_flavors.py` and `resources/lib/ui/part_filter.py` consume `thetvdb_id`, `thetvdb_season`, `thetvdb_part`, `anime_media_episodes`, and `global_media_episodes` to disambiguate multi-part seasons and filter multi-season torrent packs.
  3. **Trakt Scrobbling & Media-Kind Namespace Routing (`player.py:build_trakt_ids()`, `WatchlistFlavor/trakt.py`)**:
     - Populates `script.trakt.ids` window property and Trakt scrobble/watchlist payloads directly from `trakt_id`, `trakt_slug`, `imdb_id`, `themoviedb_id`, `thetvdb_id`, and `global_media_type` (`'movie'` vs `'show'`). Requires `global_media_type` to be 100% populated and `(global_media_type, trakt_id)` to be free of disjoint IMDb/TMDB collisions.
  4. **Scraper Title Isolation, Candidate Scoring & Dub Gating (`BrowserBase.py`, `source_utils.py`, `pages/*`)**:
     - `database.get_mapping_aliases(mal_id)` queries `control.metadataDB` / `control.mappingDB` (`anime.mal_title`, `anime.title_english`, and `anime_aliases`) to supply multi-language synonyms to `source_utils.isolate_show_title()`, `score_title_candidate()`, and `expand_sequel_season_aliases()`.
     - `anime.mal_dub_id` drives English Dub / Dual-Audio badge rendering in `divide_flavors.py` and dub scraper eligibility across `nyaa.py`, `animetosho.py`, `anikoto.py`, `watchnixtoons2.py`, `bitsearch.py`, and `piratebay.py`.
  5. **Franchise Graph, Series Profile & SmartPlay Sequel Resolution (`franchise_links`)**:
     - `database.get_franchise_entries(mal_id)`, `get_franchise_chain(mal_id)`, and `get_sequel_mal_id(mal_id)` traverse typed `PREQUEL`/`SEQUEL`/`SIDE_STORY`/`PARENT`/`SPIN_OFF` edges in `franchise_links` for Related Anime directories, `windows/series_profile.py`, and SmartPlay end-of-season sequel transitions.
  6. **Filler Episode Tagging, Skip Times & Broadcast Schedules (`filler_ranges`, `schedule_ids`)**:
     - `endpoints/anime_filler.py` and `database.get_mapping_filler_map(mal_id)` read `filler_ranges` (`ep_start`, `ep_end`, `filler_type`) offline before any AnimeFillerList HTTP fallback, powering episode filler badges and player filler auto-skip.
     - `endpoints/introdb.py` and `endpoints/anime_skip.py` resolve cross-platform IDs (`thetvdb_id`, `themoviedb_id`, `imdb_id`, `anidb_id`) from `mappings.db` for 3-tier intro/outro skip timestamp lookups.
     - `AnimeSchedule.py` and `routes/tools.py` query `schedule_ids` (`syobocal_tid`, `annict_id`) for Japanese broadcast schedule correlation.
  7. **Offline FTS5 Search & Seasonal/Year Catalogue Browsing (`anime_fts`, `year`, `anime_season`, `genres`, `studios`, `start_date`)**:
     - `database.search_mappings_offline()`, `browse_mappings_by_year_season()`, and `get_mapping_years()` query `anime_fts` (`MATCH`) and composite indexes on `anime` to power offline title search and `routes/search.py:BROWSE_YEAR_SEASON`.

## 2. Active-Only Upstream Source Vetting Protocol
- **Mandatory Upstream Liveness Audit**: Only integrate or query upstream data sources that are actively maintained and updated. Before adding any aggregator or dataset, verify via the GitHub API or upstream headers that `archived == false` and commits/releases are current.
- **Permanently Retired Sources (Never Re-Introduce)**:
  - `manami-project/anime-offline-database` — Archived on `2026-07-04` (read-only at release `2026-27`). Never use as a primary source for catalog discovery or volatile metadata.
  - `eliasbenb/PlexAniBridge-Mappings` — Archived on `2026-04-05`. Replaced by its active official v3 successor **`anibridge/anibridge-mappings`** (`https://github.com/anibridge/anibridge-mappings/releases/tag/v3`).
  - `MALSync/MAL-Sync-Backup` (GitHub raw) — Dead (`HTTP 451` DMCA takedown). Use `anibridge/anibridge-mappings` v3, `Anime-Lists/anime-lists` XML, and live `api.ani.zip` / `api.malsync.moe`.
- **Approved Active Sources (14 Per-Service Databases)**:
  1. **AniList GraphQL API** (`https://graphql.anilist.co` — 50/page x 5-alias batching via `id_in` and `idMal_in`) -> `data/anime_metadata_anilist_catalog.db`
  2. **Kitsu Edge API** (`https://kitsu.app/api/edge/anime?page[limit]=20&include=mappings`) -> `data/anime_metadata_kitsu_catalog.db`
  3. **AniBridge Mappings v3** (`https://github.com/anibridge/anibridge-mappings/releases/download/v3/mappings.min.json`) -> `data/anime_metadata_anibridge_mappings.db`
  4. **AniDB Official Daily Titles** (`https://anidb.net/api/anime-titles.xml.gz`) -> `data/anime_metadata_anidb_titles.db`
  5. **Fribb Anime-Lists** (`https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-full.json`) -> `data/anime_metadata_fribb_ids.db`
  6. **ARM (`kawaiioverflow/arm`)** (`https://raw.githubusercontent.com/kawaiioverflow/arm/master/arm.json`) -> `data/anime_metadata_arm_ids.db`
  7. **Kometa Anime-IDs** (`https://raw.githubusercontent.com/Kometa-Team/Anime-IDs/master/anime_ids.json`) -> `data/anime_metadata_kometa_ids.db`
  8. **ScudLee Anime-Lists XML** (`https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-full.xml`) -> `data/anime_metadata_animelists_xml.db`
  9. **Wikidata Live SPARQL** (`https://query.wikidata.org/sparql`) -> `data/anime_metadata_wikidata_ids.db`
  10. **MAL-Dubs** (`https://raw.githubusercontent.com/MAL-Dubs/MAL-Dubs/main/data/dubInfo.json`) -> `data/anime_metadata_mal_dubs.db`
  11. **Simkl API** (`https://api.simkl.com`) -> `data/anime_metadata_simkl_catalog.db`
  12. **Trakt API** (`https://api.trakt.tv`) -> `data/anime_metadata_trakt_ids.db`
  13. **AnimeFillerList** (`https://www.animefillerlist.com/shows`) -> `data/anime_metadata_filler_ranges.db`
  14. **AniZip API** (`https://api.ani.zip/mappings`) -> `data/anime_metadata_anizip_mappings.db`

## 3. Three-Tier Database Separation & Provenance Architecture
- **Tier 1 — Standalone Per-Service Databases (`data/anime_metadata_<service>_<data>.db`)**:
  - Always harvest 100% of each active upstream service or aggregator into its own standalone SQLite database in `data/` via `scripts/harvest_services.py` before performing any cross-source correlation.
  - Preserve raw service schemas, localized titles, external ID mappings, episode range descriptors, and `service_meta` harvest timestamps so cross-service disagreements can be audited without re-querying remote APIs.
- **Tier 2 — Authority-Weighted Cross-Service Correlation (`scripts/correlate_mappings.py`)**:
  - Correlate all 14 per-service databases using `_pick_consensus()` authority weights and emit `dist/correlation_report.json` recording per-service row counts, consensus matches, and resolved conflicts.
- **Tier 3 — Separated Interconnect vs. Metadata Databases (`dist/`)**:
  - `dist/anime_mappings.db` (+ `.xz`): Lean service-interconnect and seasonal/episode routing database consumed by `Otaku-RPDev`. Focus on cross-platform primary IDs, `thetvdb_season`, `thetvdb_part`, `anime_media_type`, `global_media_type`, `anime_media_episodes`, `global_media_episodes`, `tvdb_rules`, `franchise_links`, and `schedule_ids`. Keep volatile metadata minimal.
  - `dist/anime_metadata.db` (+ `.xz`): Comprehensive correlated metadata database (`anime_metadata` with per-field `provenance_json`, full `anime_aliases`, and `filler_ranges`) for rich metadata consumption alongside `anime_mappings.db`.
- **Git Binary Exclusion Invariant**: Never commit binary SQLite databases or cache blobs (`data/`, `dist/`, `.cache/`, `.seed/`, `*.db`, `*.xz`) into Git history. Publish built artifacts exclusively via GitHub Releases (`latest` and dated `vYYYY.MM.DD` tags).

## 4. Full Historical & Current Catalog Completeness
- **No Historical Era Starvation (`mal_id < 55000`)**: Never hard-exclude pre-2023 or finished historical anime (`mal_id < 55000`) from API hydration or correlation. Use tiered priority queues (airing/recent missing IDs first, followed by historical catalog entries missing IDs, dates, or titles) so 100% of past and present anime are populated.
- **Cache Payload Schema Validation**: Always validate that cached `.cache/*.json` payloads contain all current schema fields (`_is_complete_anilist_payload` verifying `genres`, `studios`, and `startDate.month`) before treating them as complete cache hits.
- **Transitive Secondary-ID Bridging**: When an entry on AniList has `idMal: null` or a feed entry lacks a direct `mal_id`, bridge it via unambiguous 1-to-1 `(anilist_id, anidb_id, kitsu_id, simkl_id)` cross-links established by active ID aggregators (`Fribb`, `ARM`, `AniBridge v3`, `Kometa`, `Kitsu mappings`, `Wikidata`).

## 5. Cross-Service Data-Integrity Invariants
- **Zero Seed-Echo Triple-Voting**: When seeding `anime_metadata_trakt_ids.db` or `anime_metadata_simkl_catalog.db` from a baseline DB (`source = 'seed'`), store only `trakt_id`/`trakt_slug` or `simkl_id`. Never echo baseline `thetvdb_id`, `themoviedb_id`, or `imdb_id` into `trakt_mappings` or `simkl_entries`, and only allow `source == 'trakt_api'` / `'simkl_api'` rows to vote on external IDs.
- **Primary `anilist_id` Retention on Shared `mal_id`s**: When multiple AniList entries share the same `idMal` (e.g. split-season AniList entries mapping to a single long-running MAL entry), retain the primary (lowest) `anilist_id` in `anilist_by_mal` and select `al_rec` via the consensus-winning `anilist_id`.
- **1-to-1 Regular-Scope (`:R`) `anidb_id` Guard**: In AniDB, multiple MAL TV Specials are folded into a parent TV series's `anidb_id` under Special scope (`:S`). Only assign `anidb_id` from unclaimed 1-to-1 mappings and Regular (`:R`) scope (`anibridge_v3`), never from multi-MAL lists or `:S` scope.
- **Strict `thetvdb_id` <-> `thetvdb_season` / `ep_off` Coupling**: Filter `thetvdb_season` and `episode_offset` votes strictly to sources whose `thetvdb_id` matches the consensus-winning `anime.thetvdb_id`. Enforce `thetvdb_id = NULL` on `MOVIE` entries so movies never collide with parent TV series Season 0 TVDB IDs.
- **Canonical Bidirectional `tvdb_rules` Coordinates**:
  - Every `tvdb_rules` row across `anibridge_v3` and `anime_lists_xml` must store `ep_start` as the 1-based **local MAL episode start** and `range_start..range_end` as the 1-based **target TVDB season episode range**.
  - Forward mapping (`MAL ep E -> TVDB S, ep T`): when `ep_start <= E <= ep_start + (range_end - range_start)`, `T = range_start + (E - ep_start)`.
  - Reverse mapping (`TVDB S, ep T -> MAL ep E`): when `range_start <= T <= range_end`, `E = ep_start + (T - range_start)`.
- **100% Deterministic `global_media_type` & Typed `franchise_links`**: Every row in `anime` must have a non-null `global_media_type` (`'movie'` or `'show'`). `franchise_links` must contain only typed relation edges (`PREQUEL`, `SEQUEL`, `SIDE_STORY`, `PARENT`, `SPIN_OFF`, `ALTERNATIVE`, `SUMMARY`) with symmetric `PREQUEL <-> SEQUEL` inverses.

## 6. Verification, Testing & Release Invariants
- **Pre-Release Verification Gate**: Always run both hermetic unit tests and the strict SQLite validator before publishing or pushing pipeline changes:
  1. `python3 -W default -m unittest discover -s tests -p "test_*.py" -v`
  2. `python3 scripts/validate_mappings.py --db dist/anime_mappings.db --baseline .seed/anime_mappings_original.db --xz dist/anime_mappings.db.xz --manifest dist/manifest.json --min-rows 30000 --require-canonical-spots`
- **Index & Query-Plan Invariant**: Every hot lookup column (`mal_id`, `anilist_id`, `kitsu_id`, `anidb_id`, `simkl_id`, `thetvdb_id`, `themoviedb_id`, `imdb_id`, `trakt_id`, `mal_dub_id`, `mal_title LIKE 'Prefix%'`, seasonal composite browse, TVDB sibling ordering, `anime_fts`, `anime_aliases`, `franchise_links`, `filler_ranges`) must resolve via an index `SEARCH` with zero full-table `SCAN` or `TEMP B-TREE` sort in `EXPLAIN QUERY PLAN`, followed by `ANALYZE` and `VACUUM` for read-only URI (`file:...db?mode=ro`) consumers.
- **Git Commit Invariant**: All automated Git commits must specify `--no-gpg-sign` (`git commit --no-gpg-sign -m "..."`).
