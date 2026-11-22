#!/usr/bin/env python3
"""Monitored local Trakt ID prewarm utility for Otaku-Mapping-DB.

Reads a baseline anime_mappings.db (preserving all v2.3 tables cleanly), applies
unambiguous sibling IMDb/TMDb Trakt IDs and resumable .cache lookups, queries
remaining unique Trakt IDs at a safe rate limit with periodic SQLite commits,
rebuilds dist/.xz/manifest/RELEASE_NOTES/checksums, and runs strict validation.
"""

import argparse
import datetime
import json
import lzma
import os
import shutil
import sqlite3
import sys
import time

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import build_mappings  # noqa: E402
import validate_mappings  # noqa: E402


def _flush_trakt_ids(db_path, records):
    updates = [
        (int(r["trakt_id"]), int(mid))
        for mid, r in records.items()
        if build_mappings.parse_pos_int(r.get("trakt_id"))
    ]
    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            "UPDATE anime SET trakt_id=? WHERE mal_id=? AND (trakt_id IS NULL OR trakt_id != ?)",
            [(tid, mid, tid) for tid, mid in updates],
        )
        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Prewarm trakt_id locally onto anime_mappings.db")
    parser.add_argument("--seed", default=".seed/anime_mappings.db", help="Baseline v2.3 DB")
    parser.add_argument("--output", default="dist/anime_mappings.db", help="Output SQLite DB")
    parser.add_argument("--cache-dir", default=".cache", help="Persistent ApiCache dir")
    parser.add_argument("--max-unique", type=int, default=5000, help="Max unique HTTP lookups")
    parser.add_argument("--delay", type=float, default=0.30, help="Delay between Trakt HTTP GETs (seconds)")
    args = parser.parse_args()

    client_id = os.environ.get("TRAKT_CLIENT_ID", "").strip()
    if not client_id:
        print("[ERROR] Set TRAKT_CLIENT_ID in environment before running prewarm.", file=sys.stderr)
        return 1
    if not os.path.isfile(args.seed):
        print(f"[ERROR] Missing seed DB: {args.seed}", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    shutil.copy2(args.seed, args.output)

    records = build_mappings.load_seed_database(args.seed)
    cache = build_mappings.ApiCache(args.cache_dir)
    before_trakt = sum(1 for r in records.values() if r.get("trakt_id"))
    print(f"Starting Trakt prewarm: rows={len(records):,}, initial_trakt={before_trakt:,}", flush=True)

    summary = build_mappings.hydrate_trakt_ids(
        records,
        cache,
        max_items=args.max_unique,
        client_id=client_id,
        delay_seconds=args.delay,
        progress_every=50,
        on_batch=lambda recs: _flush_trakt_ids(args.output, recs),
    )

    stats = build_mappings.compute_stats(records)
    now_epoch = str(int(time.time()))
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = sqlite3.connect(args.output)
    try:
        conn.execute("UPDATE activities SET last_updated=? WHERE sync_id=1", (now_epoch,))
        conn.execute("INSERT OR REPLACE INTO build_meta (key, value) VALUES ('built_at_utc', ?)", (now_iso,))
        conn.execute("INSERT OR REPLACE INTO build_meta (key, value) VALUES ('coverage_json', ?)", (json.dumps(stats),))
        counts = {}
        for tbl in ("anime", "tvdb_rules", "anime_aliases", "franchise_links", "schedule_ids", "filler_ranges"):
            counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        filler_shows = conn.execute("SELECT COUNT(DISTINCT mal_id) FROM filler_ranges").fetchone()[0]
        schema_ver = (conn.execute("SELECT value FROM build_meta WHERE key='schema_version'").fetchone() or [build_mappings.SCHEMA_VERSION])[0]
        conn.commit()
        conn.execute("ANALYZE")
        conn.execute("VACUUM")
    finally:
        conn.close()

    xz_path = f"{args.output}.xz"
    with open(args.output, "rb") as src, lzma.open(xz_path, "wb", preset=6) as dst:
        shutil.copyfileobj(src, dst, 65536)

    raw_size = os.path.getsize(args.output)
    xz_size = os.path.getsize(xz_path)
    raw_sha = build_mappings.sha256_file(args.output)
    xz_sha = build_mappings.sha256_file(xz_path)

    out_dir = os.path.dirname(os.path.abspath(args.output))
    manifest = {
        "schema_version": schema_ver,
        "generated_at_utc": now_iso,
        "seed_rows": counts["anime"],
        "pruned_seed_shells": 0,
        "total_rows": counts["anime"],
        "rule_count": counts["tvdb_rules"],
        "alias_count": counts["anime_aliases"],
        "franchise_link_count": counts["franchise_links"],
        "schedule_id_count": counts["schedule_ids"],
        "filler_range_count": counts["filler_ranges"],
        "filler_show_count": filler_shows,
        "files": {
            "raw": {"filename": "anime_mappings.db", "size_bytes": raw_size, "sha256": raw_sha},
            "xz": {"filename": "anime_mappings.db.xz", "size_bytes": xz_size, "sha256": xz_sha},
        },
        "coverage": stats["columns"],
    }
    manifest_p = os.path.join(out_dir, "manifest.json")
    with open(manifest_p, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)

    c = manifest["coverage"]
    notes_p = os.path.join(out_dir, "RELEASE_NOTES.md")
    with open(notes_p, "w", encoding="utf-8") as nf:
        nf.write(
            f"## Otaku Mapping Database ({now_iso})\n\n"
            f"- **Rows**: `{manifest['total_rows']:,}` (seed: `{manifest['seed_rows']:,}`) | "
            f"**TVDB/Cour Rules**: `{manifest['rule_count']:,}` | "
            f"**Aliases**: `{manifest['alias_count']:,}` | "
            f"**Franchise Links**: `{manifest['franchise_link_count']:,}` | "
            f"**Schedule IDs**: `{manifest['schedule_id_count']:,}` | "
            f"**Filler Ranges**: `{manifest['filler_range_count']:,}` (`{manifest['filler_show_count']:,}` shows)\n"
            f"- **Archives**: `anime_mappings.db.xz` (`{xz_size:,}` bytes) | `anime_mappings.db` (`{raw_size:,}` bytes)\n"
            f"- **Coverage**: `anilist_id={c['anilist_id']:,}`, `kitsu_id={c['kitsu_id']:,}`, "
            f"`anidb_id={c['anidb_id']:,}`, `simkl_id={c['simkl_id']:,}`, `thetvdb_id={c['thetvdb_id']:,}`, "
            f"`themoviedb_id={c['themoviedb_id']:,}`, `imdb_id={c['imdb_id']:,}`, `trakt_id={c['trakt_id']:,}`, `mal_dub_id={c['mal_dub_id']:,}`\n"
            f"- **Recent (`mal_id >= 55000`, n=`{c['recent_total']:,}`)**: "
            f"`thetvdb_id={c['recent_thetvdb_id']:,}`, `themoviedb_id={c['recent_themoviedb_id']:,}`, "
            f"`anidb_id={c['recent_anidb_id']:,}`, `trakt_id={c['recent_trakt_id']:,}`\n"
        )

    errs = validate_mappings.validate_database(
        args.output,
        baseline_path=args.seed,
        xz_path=xz_path,
        manifest_path=manifest_p,
        min_rows=30000,
        require_canonical_spots=True,
    )
    if errs:
        for e in errs:
            print(f"[FAIL] {e}", file=sys.stderr)
        return 1

    print(
        f"[OK] Prewarm complete & validated: trakt_id {before_trakt:,} -> {c['trakt_id']:,} "
        f"(siblings=+{summary['sibling_filled']:,}, http={summary['http_requests']:,}, resolved_http=+{summary['resolved_http']:,}; "
        f"recent trakt={c['recent_trakt_id']:,})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
