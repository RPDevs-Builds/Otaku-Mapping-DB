#!/usr/bin/env python3
"""Strict integrity, query-plan, and monotone-coverage validator for anime_mappings.db."""

import argparse
import hashlib
import json
import lzma
import os
import re
import sqlite3
import sys

REQUIRED_TABLES = ("anime", "activities", "tvdb_rules", "anime_aliases", "franchise_links", "schedule_ids", "filler_ranges", "anime_fts", "build_meta")
_VALID_FILLER_TYPES = ("Filler", "Mixed Canon/Filler", "Recap")
REQUIRED_COLUMNS = (
    "mal_id", "mal_dub_id", "anilist_id", "kitsu_id", "anidb_id", "simkl_id",
    "thetvdb_id", "themoviedb_id", "imdb_id", "trakt_id", "mal_title", "mal_picture",
    "thetvdb_season", "thetvdb_part", "duration", "score", "status",
    "anime_media_type", "global_media_type", "anime_media_episodes", "global_media_episodes",
    "year", "anime_season", "episodes", "title_english", "genres", "studios",
    "start_date", "trakt_slug",
)
INDEXED_LOOKUP_COLUMNS = (
    ("mal_id", 1),
    ("anilist_id", 1),
    ("kitsu_id", 1),
    ("anidb_id", 1),
    ("simkl_id", 1),
    ("thetvdb_id", 1),
    ("themoviedb_id", 1),
    ("imdb_id", "tt0000001"),
    ("trakt_id", 1),
    ("mal_dub_id", 1),
    ("mal_title", "Cowboy Bebop"),
    ("year", 2024),
)
INTEGER_ID_COLS = frozenset({
    "mal_id", "mal_dub_id", "anilist_id", "kitsu_id", "anidb_id",
    "simkl_id", "thetvdb_id", "themoviedb_id", "trakt_id",
})
_RE_IMDB = re.compile(r"^tt\d{5,10}$")


def _col_count(conn, col):
    if col in INTEGER_ID_COLS:
        return conn.execute(
            f'SELECT COUNT(*) FROM anime WHERE typeof("{col}") = "integer" AND "{col}" > 0'
        ).fetchone()[0]
    return conn.execute(
        f'SELECT COUNT(*) FROM anime WHERE "{col}" IS NOT NULL AND TRIM(CAST("{col}" AS TEXT)) != ""'
    ).fetchone()[0]


def validate_database(
    db_path,
    baseline_path=None,
    xz_path=None,
    manifest_path=None,
    min_rows=0,
    max_regression_ratio=0.005,
    require_canonical_spots=False,
):
    """Run full schema, hygiene, query-plan, regression, and compression verification.

    Returns:
        list[str]: Empty list on success, or list of human-readable error strings.
    """
    errors = []
    if not os.path.isfile(db_path):
        return [f"Missing database file: {db_path}"]

    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True)
    try:
        for pragma_name in ("quick_check", "integrity_check"):
            res = conn.execute(f"PRAGMA {pragma_name}").fetchone()[0]
            if res != "ok":
                errors.append(f"PRAGMA {pragma_name} failed: {res}")

        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing_tables = [t for t in REQUIRED_TABLES if t not in tables]
        if missing_tables:
            errors.append(f"Missing required tables: {missing_tables}")

        cols = [r[1] for r in conn.execute("PRAGMA table_info(anime)")]
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in cols]
        if missing_cols:
            errors.append(f"Missing required anime columns: {missing_cols}")
            return errors

        total_rows = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        if total_rows < min_rows:
            errors.append(f"Row count {total_rows} is below min_rows floor {min_rows}")

        # Hygiene checks: zero empty strings in ID columns, numeric integer IDs, valid tt-prefixed IMDb IDs, valid seasons
        for id_col in ("mal_id", "anilist_id", "kitsu_id", "anidb_id", "simkl_id", "thetvdb_id", "themoviedb_id", "imdb_id", "trakt_id", "mal_dub_id"):
            dirty_empty = conn.execute(
                f'SELECT COUNT(*) FROM anime WHERE "{id_col}" = "" OR "{id_col}" = 0 OR "{id_col}" IN ("None", "null")'
            ).fetchone()[0]
            if dirty_empty:
                errors.append(f"Column {id_col} contains {dirty_empty} empty/zero sentinel values")
            if id_col in INTEGER_ID_COLS:
                dirty_str = conn.execute(
                    f'SELECT COUNT(*) FROM anime WHERE "{id_col}" IS NOT NULL AND typeof("{id_col}") != "integer"'
                ).fetchone()[0]
                if dirty_str:
                    errors.append(f"Column {id_col} contains {dirty_str} non-integer values")

        bad_imdb = 0
        for (val,) in conn.execute("SELECT imdb_id FROM anime WHERE imdb_id IS NOT NULL"):
            if not _RE_IMDB.match(str(val)):
                bad_imdb += 1
        if bad_imdb:
            errors.append(f"Column imdb_id contains {bad_imdb} malformed non-ttNNNNN values")

        bad_season = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE thetvdb_season IS NOT NULL AND typeof(thetvdb_season) != 'integer' AND thetvdb_season != 'a'"
        ).fetchone()[0]
        if bad_season:
            errors.append(f"Column thetvdb_season contains {bad_season} invalid non-integer/non-'a' values")

        bad_year = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE year IS NOT NULL AND (typeof(year) != 'integer' OR year < 1900 OR year > 2035)"
        ).fetchone()[0]
        if bad_year:
            errors.append(f"Column year contains {bad_year} out-of-range/non-integer values")

        bad_anime_season = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE anime_season IS NOT NULL AND anime_season NOT IN ('WINTER', 'SPRING', 'SUMMER', 'FALL')"
        ).fetchone()[0]
        if bad_anime_season:
            errors.append(f"Column anime_season contains {bad_anime_season} invalid seasonal values")

        empty_shells = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE mal_title IS NULL AND status IS NULL "
            "AND mal_dub_id IS NULL AND anilist_id IS NULL AND kitsu_id IS NULL "
            "AND anidb_id IS NULL AND simkl_id IS NULL AND thetvdb_id IS NULL "
            "AND themoviedb_id IS NULL AND imdb_id IS NULL AND trakt_id IS NULL"
        ).fetchone()[0]
        if empty_shells:
            errors.append(f"Database contains {empty_shells} empty orphan rows with no title, status, or external IDs")

        # Verify query plans never degrade to SCAN anime on hot ID columns
        for col, sample in INDEXED_LOOKUP_COLUMNS:
            plan_rows = conn.execute(
                f'EXPLAIN QUERY PLAN SELECT mal_id FROM anime WHERE "{col}" = ?', (sample,)
            ).fetchall()
            plan_txt = " | ".join(str(r[-1]) for r in plan_rows)
            if "SCAN anime" in plan_txt:
                errors.append(f"Missing/unused index for {col}: query plan = '{plan_txt}'")

        # Ensure prefix LIKE searches on mal_title use NOCASE index SEARCH instead of full table SCAN
        like_plan = " | ".join(
            str(r[-1])
            for r in conn.execute("EXPLAIN QUERY PLAN SELECT mal_id FROM anime WHERE mal_title LIKE 'Cowboy%'").fetchall()
        )
        if "SCAN anime" in like_plan or "SEARCH anime" not in like_plan:
            errors.append(f"mal_title prefix LIKE does not use NOCASE index SEARCH: query plan = '{like_plan}'")

        # Ensure seasonal composite browse + TVDB ordered sibling queries avoid full scans / TEMP B-TREE sorts
        season_plan = " | ".join(
            str(r[-1])
            for r in conn.execute(
                "EXPLAIN QUERY PLAN SELECT mal_id FROM anime "
                "WHERE year = 2024 AND anime_season = 'FALL' AND anime_media_type = 'TV' "
                "ORDER BY score DESC, mal_id DESC"
            ).fetchall()
        )
        if "SCAN anime" in season_plan or "TEMP B-TREE" in season_plan:
            errors.append(f"Seasonal browse query uses scan/temp sort: '{season_plan}'")

        tvdb_plan = " | ".join(
            str(r[-1])
            for r in conn.execute(
                "EXPLAIN QUERY PLAN SELECT mal_id FROM anime "
                "WHERE thetvdb_id = 1 ORDER BY thetvdb_season, thetvdb_part"
            ).fetchall()
        )
        if "SCAN anime" in tvdb_plan or "TEMP B-TREE" in tvdb_plan:
            errors.append(f"TVDB sibling query uses scan/temp sort: '{tvdb_plan}'")

        # Verify zero disjoint IMDb/TMDB Trakt collisions within a namespace and zero shared-ID cross-kind leaks
        trakt_same_coll = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT trakt_id, (CASE WHEN anime_media_type='MOVIE' OR global_media_type='movie' THEN 'movie' ELSE 'show' END) AS kind "
            "  FROM anime WHERE trakt_id IS NOT NULL "
            "  GROUP BY trakt_id, kind "
            "  HAVING COUNT(DISTINCT imdb_id) > 1 OR COUNT(DISTINCT themoviedb_id) > 1"
            ")"
        ).fetchone()[0]
        if trakt_same_coll:
            errors.append(f"Found {trakt_same_coll} same-kind trakt_id disjoint IMDb/TMDb collisions")

        trakt_cross_leak = conn.execute(
            "SELECT COUNT(*) FROM anime a JOIN anime b ON a.trakt_id = b.trakt_id AND a.mal_id < b.mal_id "
            "WHERE a.trakt_id IS NOT NULL "
            "AND (CASE WHEN a.anime_media_type='MOVIE' OR a.global_media_type='movie' THEN 'movie' ELSE 'show' END) != "
            "    (CASE WHEN b.anime_media_type='MOVIE' OR b.global_media_type='movie' THEN 'movie' ELSE 'show' END) "
            "AND ((a.imdb_id IS NOT NULL AND a.imdb_id = b.imdb_id) "
            "  OR (a.themoviedb_id IS NOT NULL AND a.themoviedb_id = b.themoviedb_id) "
            "  OR (a.thetvdb_id IS NOT NULL AND a.thetvdb_id = b.thetvdb_id))"
        ).fetchone()[0]
        if trakt_cross_leak:
            errors.append(f"Found {trakt_cross_leak} cross-kind trakt_id leaks sharing IMDb/TMDb/TVDB")

        if "anime_fts" in tables:
            fts_plan = " | ".join(
                str(r[-1])
                for r in conn.execute("EXPLAIN QUERY PLAN SELECT rowid FROM anime_fts WHERE anime_fts MATCH 'Bebop'").fetchall()
            )
            if "SCAN anime_fts" not in fts_plan and "VIRTUAL TABLE INDEX" not in fts_plan:
                errors.append(f"Unexpected anime_fts query plan: '{fts_plan}'")
            doc_tbl = "anime_fts_docsize" if "anime_fts_docsize" in tables else "anime_fts"
            fts_cnt = conn.execute(f'SELECT COUNT(*) FROM "{doc_tbl}"').fetchone()[0]
            if fts_cnt != total_rows:
                errors.append(f"anime_fts row count mismatch: fts={fts_cnt} vs anime={total_rows}")

        idx_names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}

        # Verify v2.2 relational index existence, query plan, & zero dangling foreign IDs
        if "anime_aliases" in tables:
            if "ix_aliases_title" not in idx_names:
                errors.append("Missing required index 'ix_aliases_title' on anime_aliases")
            for sql, arg, label in (
                ("EXPLAIN QUERY PLAN SELECT title FROM anime_aliases WHERE mal_id = ?", 1, "anime_aliases.mal_id"),
                ("EXPLAIN QUERY PLAN SELECT mal_id FROM anime_aliases WHERE title = ?", "Cowboy Bebop", "anime_aliases.title"),
            ):
                p_txt = " | ".join(str(r[-1]) for r in conn.execute(sql, (arg,)).fetchall())
                if "SCAN anime_aliases" in p_txt and "USING " not in p_txt:
                    errors.append(f"Missing/unused index for {label}: query plan = '{p_txt}'")
            dangling_al = conn.execute(
                "SELECT COUNT(*) FROM anime_aliases a WHERE NOT EXISTS (SELECT 1 FROM anime m WHERE m.mal_id = a.mal_id)"
            ).fetchone()[0]
            if dangling_al:
                errors.append(f"anime_aliases contains {dangling_al} dangling mal_id references")

        if "franchise_links" in tables:
            if "ix_franchise_rel" not in idx_names:
                errors.append("Missing required index 'ix_franchise_rel' on franchise_links")
            for sql, arg, label in (
                ("EXPLAIN QUERY PLAN SELECT related_mal_id FROM franchise_links WHERE mal_id = ?", 1, "franchise_links.mal_id"),
                ("EXPLAIN QUERY PLAN SELECT mal_id FROM franchise_links WHERE related_mal_id = ?", 1, "franchise_links.related_mal_id"),
            ):
                p_txt = " | ".join(str(r[-1]) for r in conn.execute(sql, (arg,)).fetchall())
                if "SCAN franchise_links" in p_txt and "USING " not in p_txt:
                    errors.append(f"Missing/unused index for {label}: query plan = '{p_txt}'")
            dangling_fr = conn.execute(
                "SELECT COUNT(*) FROM franchise_links fl "
                "WHERE NOT EXISTS (SELECT 1 FROM anime m1 WHERE m1.mal_id = fl.mal_id) "
                "OR NOT EXISTS (SELECT 1 FROM anime m2 WHERE m2.mal_id = fl.related_mal_id)"
            ).fetchone()[0]
            if dangling_fr:
                errors.append(f"franchise_links contains {dangling_fr} dangling mal_id references")

        if "schedule_ids" in tables:
            dangling_sc = conn.execute(
                "SELECT COUNT(*) FROM schedule_ids s WHERE NOT EXISTS (SELECT 1 FROM anime m WHERE m.mal_id = s.mal_id)"
            ).fetchone()[0]
            if dangling_sc:
                errors.append(f"schedule_ids contains {dangling_sc} dangling mal_id references")

        if "filler_ranges" in tables:
            p_txt = " | ".join(
                str(r[-1])
                for r in conn.execute(
                    "EXPLAIN QUERY PLAN SELECT ep_start, ep_end, filler_type FROM filler_ranges WHERE mal_id = ?",
                    (1,),
                ).fetchall()
            )
            if "SCAN filler_ranges" in p_txt and "USING " not in p_txt:
                errors.append(f"Missing/unused primary key index for filler_ranges.mal_id: query plan = '{p_txt}'")
            dangling_fr_r = conn.execute(
                "SELECT COUNT(*) FROM filler_ranges f WHERE NOT EXISTS (SELECT 1 FROM anime m WHERE m.mal_id = f.mal_id)"
            ).fetchone()[0]
            if dangling_fr_r:
                errors.append(f"filler_ranges contains {dangling_fr_r} dangling mal_id references")
            bad_bounds = conn.execute(
                "SELECT COUNT(*) FROM filler_ranges WHERE ep_start < 1 OR ep_end < ep_start"
            ).fetchone()[0]
            if bad_bounds:
                errors.append(f"filler_ranges contains {bad_bounds} invalid episode bounds")
            bad_types = conn.execute(
                "SELECT COUNT(*) FROM filler_ranges WHERE filler_type NOT IN ('Filler', 'Mixed Canon/Filler', 'Recap')"
            ).fetchone()[0]
            if bad_types:
                errors.append(f"filler_ranges contains {bad_types} invalid filler_type values")
            bad_ep_overrun = 0
            for ep_end, ep_rng in conn.execute(
                "SELECT f.ep_end, a.anime_media_episodes FROM filler_ranges f "
                "JOIN anime a ON a.mal_id = f.mal_id "
                "WHERE a.status = 'FINISHED' AND a.anime_media_type IN ('TV', 'ONA') AND a.anime_media_episodes IS NOT NULL"
            ).fetchall():
                m_end = re.match(r"^\s*\d+\s*-\s*(\d+)\s*$", str(ep_rng or ""))
                if m_end and ep_end > int(m_end.group(1)) + 2:
                    bad_ep_overrun += 1
            if bad_ep_overrun:
                errors.append(f"filler_ranges contains {bad_ep_overrun} finished-show episode overrun rows")

        if "tvdb_rules" in tables:
            bad_rules = conn.execute(
                "SELECT COUNT(*) FROM tvdb_rules WHERE season < 0 OR range_start < 1 OR ep_start < 1"
            ).fetchone()[0]
            if bad_rules:
                errors.append(f"tvdb_rules contains {bad_rules} invalid rule bounds (<1)")
            mismatched_tvdb = conn.execute(
                "SELECT COUNT(*) FROM tvdb_rules r JOIN anime a ON a.mal_id = r.mal_id "
                "WHERE r.tvdb_id IS NOT NULL AND a.thetvdb_id IS NOT NULL AND r.tvdb_id != a.thetvdb_id"
            ).fetchone()[0]
            if mismatched_tvdb:
                errors.append(f"tvdb_rules contains {mismatched_tvdb} rows disagreeing with anime.thetvdb_id")

        # Baseline monotone coverage comparison (ignoring empty phantom seed shells > 64000)
        if baseline_path and os.path.isfile(baseline_path):
            bconn = sqlite3.connect(f"file:{os.path.abspath(baseline_path)}?mode=ro", uri=True)
            try:
                base_rows = bconn.execute(
                    "SELECT COUNT(*) FROM anime WHERE "
                    "mal_title IS NOT NULL OR status IS NOT NULL OR mal_id <= 64000 "
                    "OR mal_dub_id IS NOT NULL OR anilist_id IS NOT NULL OR kitsu_id IS NOT NULL "
                    "OR anidb_id IS NOT NULL OR simkl_id IS NOT NULL OR thetvdb_id IS NOT NULL "
                    "OR themoviedb_id IS NOT NULL OR imdb_id IS NOT NULL OR trakt_id IS NOT NULL"
                ).fetchone()[0]
                if total_rows < base_rows * (1.0 - max_regression_ratio):
                    errors.append(f"Total rows regressed from {base_rows} to {total_rows}")
                base_schema = ""
                try:
                    row_sv = bconn.execute("SELECT value FROM build_meta WHERE key='schema_version'").fetchone()
                    base_schema = str(row_sv[0] or "") if row_sv else ""
                except sqlite3.Error:
                    base_schema = ""
                for col in (
                    "anilist_id", "kitsu_id", "anidb_id", "simkl_id",
                    "thetvdb_id", "themoviedb_id", "imdb_id", "trakt_id",
                    "mal_dub_id", "mal_title",
                ):
                    cur_c = _col_count(conn, col)
                    base_c = _col_count(bconn, col)
                    col_tol = max_regression_ratio
                    # Allow up to 1.5% one-time Trakt collision pruning when upgrading from pre-2.6 seeds
                    if col == "trakt_id" and (not base_schema or tuple(int(p) for p in base_schema.split(".")[:2] if p.isdigit()) < (2, 6)):
                        col_tol = max(max_regression_ratio, 0.015)
                    if cur_c < base_c * (1.0 - col_tol):
                        errors.append(f"Coverage regression in '{col}': baseline={base_c}, candidate={cur_c}")
            finally:
                bconn.close()

        # Optional live canonical spot checks (used in CI / full builds)
        if require_canonical_spots:
            checks = (
                (1, {"thetvdb_id": 76885, "themoviedb_id": 30991}),
                (21, {"thetvdb_season": "a", "thetvdb_id": 81797}),
                (52991, {"thetvdb_id": 424536, "themoviedb_id": 209867}),
                (58516, {"thetvdb_id": 248035, "thetvdb_season": 4}),
                (59193, {"thetvdb_id": 371310, "themoviedb_id": 94664}),
                (63752, {"thetvdb_id": 475631}),
            )
            conn.row_factory = sqlite3.Row
            for mid, expected in checks:
                row = conn.execute("SELECT * FROM anime WHERE mal_id=?", (mid,)).fetchone()
                if not row:
                    errors.append(f"Missing canonical spot-check mal_id={mid}")
                    continue
                for k, v in expected.items():
                    if row[k] != v:
                        errors.append(f"Spot check mal_id={mid} col={k}: expected {v!r}, got {row[k]!r}")
    finally:
        conn.close()

    # Verify .xz decompression & manifest SHA-256 parity
    if xz_path:
        if not os.path.isfile(xz_path):
            errors.append(f"Missing xz archive: {xz_path}")
        else:
            with open(db_path, "rb") as rf:
                raw_sha = hashlib.sha256(rf.read()).hexdigest()
            with lzma.open(xz_path, "rb") as xf:
                decomp_sha = hashlib.sha256(xf.read()).hexdigest()
            if decomp_sha != raw_sha:
                errors.append(f"xz decompression SHA-256 mismatch: raw={raw_sha} vs xz={decomp_sha}")

    if manifest_path:
        if not os.path.isfile(manifest_path):
            errors.append(f"Missing manifest: {manifest_path}")
        else:
            with open(manifest_path, "r", encoding="utf-8") as mf:
                manifest = json.load(mf)
            with open(db_path, "rb") as rf:
                raw_sha = hashlib.sha256(rf.read()).hexdigest()
            man_raw_sha = ((manifest.get("files") or {}).get("raw") or {}).get("sha256")
            if man_raw_sha != raw_sha:
                errors.append(f"Manifest raw sha256 mismatch: manifest={man_raw_sha} vs actual={raw_sha}")

    return errors


def main():
    parser = argparse.ArgumentParser(description="Validate generated anime_mappings.db")
    parser.add_argument("--db", default="dist/anime_mappings.db", help="Candidate SQLite DB")
    parser.add_argument("--baseline", default="", help="Optional baseline DB for monotone coverage gate")
    parser.add_argument("--xz", default="", help="Optional compressed .xz archive to verify")
    parser.add_argument("--manifest", default="", help="Optional manifest.json to verify")
    parser.add_argument("--min-rows", type=int, default=30000, help="Minimum required row count")
    parser.add_argument("--require-canonical-spots", action="store_true", help="Assert canonical anime spot checks")
    args = parser.parse_args()

    errs = validate_database(
        args.db,
        baseline_path=args.baseline or None,
        xz_path=args.xz or None,
        manifest_path=args.manifest or None,
        min_rows=args.min_rows,
        require_canonical_spots=args.require_canonical_spots,
    )
    if errs:
        for e in errs:
            print(f"[FAIL] {e}", file=sys.stderr)
        return 1
    print(f"[OK] Validated {args.db} cleanly (integrity, indexes, hygiene, baseline & archives).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
