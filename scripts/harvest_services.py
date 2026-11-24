#!/usr/bin/env python3
"""Harvest 100% of active anime mapping and metadata sources into standalone per-service SQLite databases.

Strictly uses ACTIVE, vetted data sources (zero archived manami-project, zero archived
eliasbenb/PlexAniBridge-Mappings, zero dead MAL-Sync-Backup GitHub raw URLs):

Per-service databases created in data/:
  1. anime_metadata_fribb_ids.db          <- Fribb/anime-lists (active weekly)
  2. anime_metadata_arm_ids.db            <- kawaiioverflow/arm (active weekly)
  3. anime_metadata_kometa_ids.db         <- Kometa-Team/Anime-IDs (active daily)
  4. anime_metadata_animelists_xml.db     <- Anime-Lists/anime-lists XML (active daily)
  5. anime_metadata_anibridge_mappings.db <- anibridge/anibridge-mappings v3 (active daily/weekly)
  6. anime_metadata_wikidata_ids.db       <- Wikidata Live SPARQL
  7. anime_metadata_mal_dubs.db           <- MAL-Dubs/MAL-Dubs (active daily)
  8. anime_metadata_anidb_titles.db       <- AniDB Official Daily anime-titles.xml.gz
  9. anime_metadata_trakt_ids.db          <- Trakt API & verified seed/cache Trakt mappings
 10. anime_metadata_anizip_mappings.db    <- api.ani.zip & api.malsync.moe live API mappings
 11. anime_metadata_simkl_catalog.db      <- Simkl API & verified Simkl mappings
 12. anime_metadata_filler_ranges.db      <- AnimeFillerList show index & episode filler ranges
 13. anime_metadata_anilist_catalog.db    <- AniList GraphQL full historical + current catalog
"""

import argparse
import datetime
import glob
import gzip
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import build_mappings as bm  # noqa: E402

ACTIVE_SOURCE_URLS = {
    "fribb": "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-full.json",
    "arm": "https://raw.githubusercontent.com/kawaiioverflow/arm/master/arm.json",
    "kometa": "https://raw.githubusercontent.com/Kometa-Team/Anime-IDs/master/anime_ids.json",
    "anime_lists": "https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-full.xml",
    "anibridge_v3": "https://github.com/anibridge/anibridge-mappings/releases/download/v3/mappings.min.json",
    "mal_dubs": "https://raw.githubusercontent.com/MAL-Dubs/MAL-Dubs/main/data/dubInfo.json",
    "anidb_titles": "https://anidb.net/api/anime-titles.xml.gz",
}

ANILIST_GRAPHQL_URL = "https://graphql.anilist.co"
ANILIST_STATUS_MAP = {
    "FINISHED": "FINISHED",
    "RELEASING": "ONGOING",
    "NOT_YET_RELEASED": "UPCOMING",
    "CANCELLED": "UNKNOWN",
    "HIATUS": "ONGOING",
}
ANILIST_MEDIA_FIELDS = (
    "id idMal format status episodes duration season seasonYear "
    "startDate { year month day } endDate { year month day } "
    "title { romaji english native userPreferred } synonyms "
    "genres studios(isMain: true) { nodes { name isAnimationStudio } } "
    "coverImage { large medium } "
    "relations { edges { relationType node { id idMal type format } } }"
)


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_int(val):
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if s.lstrip("-").isdigit():
        return int(s)
    return None


def normalize_media_type(val):
    if not val:
        return None
    u = str(val).strip().upper()
    if u in ("TV", "MOVIE", "OVA", "ONA", "SPECIAL", "MUSIC"):
        return u
    if u in ("TV_SHORT", "TV SHORT", "SHOW"):
        return "TV"
    if u in ("FILM", "THEATRICAL"):
        return "MOVIE"
    return None


def _extract_genres(raw_list):
    if not isinstance(raw_list, list):
        return None
    out = [str(g).strip() for g in raw_list if isinstance(g, str) and str(g).strip()]
    return ",".join(dict.fromkeys(out)) if out else None






def _init_service_db(db_path, source_name, source_url):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("CREATE TABLE service_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.executemany(
        "INSERT INTO service_meta (key, value) VALUES (?, ?)",
        [
            ("source_name", source_name),
            ("source_url", source_url),
            ("harvested_at_utc", _utc_now()),
        ],
    )
    return conn


def _finalize_service_db(conn, row_count):
    conn.execute(
        "INSERT OR REPLACE INTO service_meta (key, value) VALUES ('row_count', ?)",
        (str(row_count),),
    )
    conn.commit()
    conn.execute("ANALYZE")
    conn.execute("VACUUM")
    conn.close()


def harvest_fribb(data_dir, cache_dir):
    """Harvest 100% of Fribb/anime-lists into anime_metadata_fribb_ids.db."""
    raw = bm.load_source_blob("fribb", ACTIVE_SOURCE_URLS["fribb"], cache_dir, max_cache_age=3600)
    items = json.loads(raw.decode("utf-8"))
    db_path = os.path.join(data_dir, "anime_metadata_fribb_ids.db")
    conn = _init_service_db(db_path, "Fribb/anime-lists", ACTIVE_SOURCE_URLS["fribb"])
    conn.execute(
        """
        CREATE TABLE fribb_entries (
            entry_idx INTEGER PRIMARY KEY,
            mal_id INTEGER,
            anilist_id INTEGER,
            anidb_id INTEGER,
            kitsu_id INTEGER,
            simkl_id INTEGER,
            thetvdb_id INTEGER,
            themoviedb_id INTEGER,
            imdb_id TEXT,
            livechart_id INTEGER,
            anisearch_id INTEGER,
            anime_planet_id TEXT,
            animenewsnetwork_id INTEGER,
            animecountdown_id INTEGER,
            season_tvdb INTEGER,
            season_tmdb INTEGER,
            episode_offset INTEGER,
            media_type TEXT
        )
        """
    )
    rows = []
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        season_obj = item.get("season") if isinstance(item.get("season"), dict) else {}
        ap_id = item.get("anime-planet_id")
        rows.append(
            (
                idx,
                bm.parse_pos_int(item.get("mal_id")),
                bm.parse_pos_int(item.get("anilist_id")),
                bm.parse_pos_int(item.get("anidb_id")),
                bm.parse_pos_int(item.get("kitsu_id")),
                bm.parse_pos_int(item.get("simkl_id")),
                bm.parse_pos_int(item.get("tvdb_id") or item.get("thetvdb_id")),
                bm.parse_pos_int(item.get("themoviedb_id")),
                bm.clean_imdb_id(item.get("imdb_id")),
                bm.parse_pos_int(item.get("livechart_id")),
                bm.parse_pos_int(item.get("anisearch_id")),
                str(ap_id).strip() if ap_id is not None and str(ap_id).strip() else None,
                bm.parse_pos_int(item.get("animenewsnetwork_id")),
                bm.parse_pos_int(item.get("animecountdown_id")),
                parse_int(season_obj.get("tvdb")),
                parse_int(season_obj.get("tmdb")),
                parse_int(item.get("episode_offset")),
                normalize_media_type(item.get("type")),
            )
        )
    conn.executemany(
        "INSERT INTO fribb_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.execute("CREATE INDEX idx_fribb_mal ON fribb_entries(mal_id) WHERE mal_id IS NOT NULL")
    conn.execute("CREATE INDEX idx_fribb_anilist ON fribb_entries(anilist_id) WHERE anilist_id IS NOT NULL")
    conn.execute("CREATE INDEX idx_fribb_anidb ON fribb_entries(anidb_id) WHERE anidb_id IS NOT NULL")
    _finalize_service_db(conn, len(rows))
    print(f"[OK] anime_metadata_fribb_ids.db: {len(rows):,} rows")
    return len(rows)


def harvest_arm(data_dir, cache_dir):
    """Harvest 100% of kawaiioverflow/arm into anime_metadata_arm_ids.db."""
    raw = bm.load_source_blob("arm", ACTIVE_SOURCE_URLS["arm"], cache_dir, max_cache_age=3600)
    items = json.loads(raw.decode("utf-8"))
    db_path = os.path.join(data_dir, "anime_metadata_arm_ids.db")
    conn = _init_service_db(db_path, "kawaiioverflow/arm", ACTIVE_SOURCE_URLS["arm"])
    conn.execute(
        """
        CREATE TABLE arm_entries (
            entry_idx INTEGER PRIMARY KEY,
            mal_id INTEGER,
            anilist_id INTEGER,
            annict_id INTEGER,
            syobocal_tid INTEGER
        )
        """
    )
    rows = []
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                idx,
                bm.parse_pos_int(item.get("mal_id")),
                bm.parse_pos_int(item.get("anilist_id")),
                bm.parse_pos_int(item.get("annict_id")),
                bm.parse_pos_int(item.get("syobocal_tid")),
            )
        )
    conn.executemany("INSERT INTO arm_entries VALUES (?,?,?,?,?)", rows)
    conn.execute("CREATE INDEX idx_arm_mal ON arm_entries(mal_id) WHERE mal_id IS NOT NULL")
    conn.execute("CREATE INDEX idx_arm_anilist ON arm_entries(anilist_id) WHERE anilist_id IS NOT NULL")
    _finalize_service_db(conn, len(rows))
    print(f"[OK] anime_metadata_arm_ids.db: {len(rows):,} rows")
    return len(rows)


def harvest_kometa(data_dir, cache_dir):
    """Harvest 100% of Kometa-Team/Anime-IDs into anime_metadata_kometa_ids.db."""
    raw = bm.load_source_blob("kometa", ACTIVE_SOURCE_URLS["kometa"], cache_dir, max_cache_age=3600)
    data = json.loads(raw.decode("utf-8"))
    db_path = os.path.join(data_dir, "anime_metadata_kometa_ids.db")
    conn = _init_service_db(db_path, "Kometa-Team/Anime-IDs", ACTIVE_SOURCE_URLS["kometa"])
    conn.execute(
        """
        CREATE TABLE kometa_entries (
            anidb_id INTEGER PRIMARY KEY,
            mal_ids_csv TEXT,
            mal_id_single INTEGER,
            mal_id_count INTEGER NOT NULL,
            anilist_id INTEGER,
            thetvdb_id INTEGER,
            tvdb_season INTEGER,
            tvdb_epoffset INTEGER,
            imdb_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE kometa_mal_links (
            anidb_id INTEGER NOT NULL,
            mal_id INTEGER NOT NULL,
            is_1to1 INTEGER NOT NULL,
            PRIMARY KEY (anidb_id, mal_id)
        )
        """
    )
    entries = []
    links = []
    for k, item in data.items():
        aid = bm.parse_pos_int(k)
        if not aid or not isinstance(item, dict):
            continue
        raw_mal = item.get("mal_id")
        mids = []
        if isinstance(raw_mal, int):
            if raw_mal > 0:
                mids.append(raw_mal)
        elif isinstance(raw_mal, str):
            for tok in raw_mal.split(","):
                m = bm.parse_pos_int(tok)
                if m and m not in mids:
                    mids.append(m)
        ts = parse_int(item.get("tvdb_season"))
        if ts is not None and ts < 0:
            ts = None
        imdb_raw = item.get("imdb_id")
        imdb_first = None
        if isinstance(imdb_raw, str):
            for tok in imdb_raw.split(","):
                c = bm.clean_imdb_id(tok)
                if c:
                    imdb_first = c
                    break
        entries.append(
            (
                aid,
                ",".join(str(x) for x in mids) if mids else None,
                mids[0] if len(mids) == 1 else None,
                len(mids),
                bm.parse_pos_int(item.get("anilist_id")),
                bm.parse_pos_int(item.get("tvdb_id")),
                ts,
                parse_int(item.get("tvdb_epoffset")) or 0,
                imdb_first,
            )
        )
        is_1to1 = 1 if len(mids) == 1 else 0
        for mid in mids:
            links.append((aid, mid, is_1to1))
    conn.executemany("INSERT INTO kometa_entries VALUES (?,?,?,?,?,?,?,?,?)", entries)
    conn.executemany("INSERT OR IGNORE INTO kometa_mal_links VALUES (?,?,?)", links)
    conn.execute("CREATE INDEX idx_kometa_mal_single ON kometa_entries(mal_id_single) WHERE mal_id_single IS NOT NULL")
    conn.execute("CREATE INDEX idx_kometa_links_mal ON kometa_mal_links(mal_id)")
    _finalize_service_db(conn, len(entries))
    print(f"[OK] anime_metadata_kometa_ids.db: {len(entries):,} entries, {len(links):,} MAL links")
    return len(entries)


def harvest_animelists_xml(data_dir, cache_dir):
    """Harvest 100% of Anime-Lists/anime-lists XML into anime_metadata_animelists_xml.db."""
    raw = bm.load_source_blob("anime_lists", ACTIVE_SOURCE_URLS["anime_lists"], cache_dir, max_cache_age=3600)
    root = ET.fromstring(raw)
    db_path = os.path.join(data_dir, "anime_metadata_animelists_xml.db")
    conn = _init_service_db(db_path, "Anime-Lists/anime-lists", ACTIVE_SOURCE_URLS["anime_lists"])
    conn.execute(
        """
        CREATE TABLE animelists_entries (
            anidb_id INTEGER PRIMARY KEY,
            tvdb_id_raw TEXT,
            thetvdb_id INTEGER,
            default_tvdb_season_raw TEXT,
            default_tvdb_season INTEGER,
            episode_offset INTEGER,
            tmdbtv INTEGER,
            tmdbid_raw TEXT,
            themoviedb_id INTEGER,
            tmdb_season_raw TEXT,
            tmdb_season INTEGER,
            tmdb_offset INTEGER,
            imdb_id_raw TEXT,
            imdb_id TEXT,
            name TEXT,
            before_text TEXT,
            supplemental_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE animelists_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anidb_id INTEGER NOT NULL,
            anidb_season INTEGER,
            tvdb_season INTEGER,
            tmdb_season INTEGER,
            start_ep INTEGER,
            end_ep INTEGER,
            offset_ep INTEGER,
            mapping_text TEXT
        )
        """
    )
    entries = []
    mappings = []
    for elem in root.findall("anime"):
        aid = bm.parse_pos_int(elem.attrib.get("anidbid"))
        if not aid:
            continue
        tvdb_raw = elem.attrib.get("tvdbid")
        tvdb_id = bm.parse_pos_int(tvdb_raw)
        dts_raw = elem.attrib.get("defaulttvdbseason")
        dts = parse_int(dts_raw) if dts_raw and dts_raw != "a" else None
        if dts is not None and dts < 0:
            dts = None
        ep_off = parse_int(elem.attrib.get("episodeoffset")) or 0
        tmdbtv = bm.parse_pos_int(elem.attrib.get("tmdbtv"))
        tmdbid_raw = elem.attrib.get("tmdbid")
        tmdbid_first = bm.parse_pos_int(str(tmdbid_raw).split(",")[0]) if tmdbid_raw else None
        tmdb_id = tmdbtv or tmdbid_first
        tms_raw = elem.attrib.get("tmdbseason")
        tms = parse_int(tms_raw) if tms_raw and tms_raw != "a" else None
        if tms is not None and tms < 0:
            tms = None
        tmo = parse_int(elem.attrib.get("tmdboffset")) or 0
        imdb_raw = elem.attrib.get("imdbid")
        imdb_clean = None
        if imdb_raw:
            for tok in imdb_raw.split(","):
                c = bm.clean_imdb_id(tok)
                if c:
                    imdb_clean = c
                    break
        name_el = elem.find("name")
        before_el = elem.find("before")
        supp_el = elem.find("supplemental-info")
        supp_dict = {}
        if supp_el is not None:
            for child in supp_el:
                supp_dict.setdefault(child.tag, []).append((child.text or "").strip())
        entries.append(
            (
                aid,
                tvdb_raw,
                tvdb_id,
                dts_raw,
                dts,
                ep_off,
                tmdbtv,
                tmdbid_raw,
                tmdb_id,
                tms_raw,
                tms,
                tmo,
                imdb_raw,
                imdb_clean,
                (name_el.text or "").strip() if name_el is not None else None,
                (before_el.text or "").strip() if before_el is not None else None,
                json.dumps(supp_dict, ensure_ascii=False) if supp_dict else None,
            )
        )
        mlist = elem.find("mapping-list")
        if mlist is not None:
            for m in mlist.findall("mapping"):
                mappings.append(
                    (
                        aid,
                        parse_int(m.attrib.get("anidbseason")),
                        parse_int(m.attrib.get("tvdbseason")),
                        parse_int(m.attrib.get("tmdbseason")),
                        bm.parse_pos_int(m.attrib.get("start")),
                        bm.parse_pos_int(m.attrib.get("end")),
                        parse_int(m.attrib.get("offset")) or 0,
                        (m.text or "").strip() or None,
                    )
                )
    conn.executemany("INSERT OR REPLACE INTO animelists_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", entries)
    conn.executemany(
        "INSERT INTO animelists_mappings (anidb_id, anidb_season, tvdb_season, tmdb_season, start_ep, end_ep, offset_ep, mapping_text) VALUES (?,?,?,?,?,?,?,?)",
        mappings,
    )
    conn.execute("CREATE INDEX idx_animelists_mappings_aid ON animelists_mappings(anidb_id)")
    _finalize_service_db(conn, len(entries))
    print(f"[OK] anime_metadata_animelists_xml.db: {len(entries):,} entries, {len(mappings):,} <mapping> elements")
    return len(entries)


def _parse_anibridge_descriptor(desc_str):
    parts = str(desc_str).split(":")
    prov = parts[0]
    raw_id = parts[1] if len(parts) > 1 else ""
    scope = parts[2] if len(parts) > 2 else None
    return prov, raw_id, scope


def _parse_ep_range_spec(spec_str):
    """Parse AniBridge v3 range string like '1-26', '1', '1156-', '1-12,14-24' into list of (start, end)."""
    out = []
    for chunk in str(spec_str or "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            a, b = chunk.split("-", 1)
            sa = bm.parse_pos_int(a)
            sb = bm.parse_pos_int(b) if b.strip() else None
            if sa:
                out.append((sa, sb))
        else:
            sa = bm.parse_pos_int(chunk)
            if sa:
                out.append((sa, sa))
    return out


def harvest_anibridge_v3(data_dir, cache_dir):
    """Harvest 100% of anibridge/anibridge-mappings v3 into anime_metadata_anibridge_mappings.db."""
    raw = bm.load_source_blob("anibridge_v3", ACTIVE_SOURCE_URLS["anibridge_v3"], cache_dir, max_cache_age=3600)
    data = json.loads(raw.decode("utf-8"))
    db_path = os.path.join(data_dir, "anime_metadata_anibridge_mappings.db")
    conn = _init_service_db(db_path, "anibridge/anibridge-mappings v3", ACTIVE_SOURCE_URLS["anibridge_v3"])
    meta_info = data.get("$meta") if isinstance(data.get("$meta"), dict) else {}
    for k, v in meta_info.items():
        conn.execute("INSERT OR REPLACE INTO service_meta (key, value) VALUES (?, ?)", (f"upstream_{k}", str(v)))

    conn.execute(
        """
        CREATE TABLE anibridge_edges (
            source_descriptor TEXT NOT NULL,
            source_provider TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_scope TEXT,
            target_descriptor TEXT NOT NULL,
            target_provider TEXT NOT NULL,
            target_id TEXT NOT NULL,
            target_scope TEXT,
            source_range TEXT NOT NULL,
            target_range TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE anibridge_mal_summary (
            mal_id INTEGER PRIMARY KEY,
            anilist_id INTEGER,
            anidb_id INTEGER,
            anidb_scope TEXT,
            tvdb_show_id INTEGER,
            tvdb_season INTEGER,
            tvdb_part INTEGER,
            tvdb_movie_id INTEGER,
            tmdb_show_id INTEGER,
            tmdb_season INTEGER,
            tmdb_movie_id INTEGER,
            imdb_movie_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE anibridge_tvdb_rules (
            mal_id INTEGER NOT NULL,
            tvdb_id INTEGER NOT NULL,
            season INTEGER NOT NULL,
            part INTEGER NOT NULL,
            rule_type TEXT NOT NULL,
            range_start INTEGER NOT NULL,
            range_end INTEGER,
            ep_start INTEGER NOT NULL,
            source_provider TEXT NOT NULL
        )
        """
    )

    edge_rows = []
    mal_summaries = {}
    rule_rows = []

    for src_desc, targets in data.items():
        if src_desc.startswith("$") or not isinstance(targets, dict):
            continue
        s_prov, s_id, s_scope = _parse_anibridge_descriptor(src_desc)
        for tgt_desc, ranges_dict in targets.items():
            if not isinstance(ranges_dict, dict):
                continue
            t_prov, t_id, t_scope = _parse_anibridge_descriptor(tgt_desc)
            for s_rng, t_rng in ranges_dict.items():
                edge_rows.append((src_desc, s_prov, s_id, s_scope, tgt_desc, t_prov, t_id, t_scope, str(s_rng), str(t_rng)))

        if s_prov == "mal":
            mid = bm.parse_pos_int(s_id)
            if not mid:
                continue
            anilist_id = None
            anidb_r_id = None
            anidb_s_id = None
            tvdb_show_candidates = []  # (season, ep_start, tvdb_id, r_start, r_end)
            tvdb_movie_id = None
            tmdb_show_candidates = []
            tmdb_movie_id = None
            imdb_movie_id = None

            for tgt_desc, ranges_dict in targets.items():
                if not isinstance(ranges_dict, dict):
                    continue
                t_prov, t_id, t_scope = _parse_anibridge_descriptor(tgt_desc)
                if t_prov == "anilist" and not anilist_id:
                    anilist_id = bm.parse_pos_int(t_id)
                elif t_prov == "anidb":
                    aid = bm.parse_pos_int(t_id)
                    if aid:
                        if t_scope == "R" and not anidb_r_id:
                            anidb_r_id = aid
                        elif not anidb_s_id:
                            anidb_s_id = aid
                elif t_prov == "tvdb_movie" and not tvdb_movie_id:
                    tvdb_movie_id = bm.parse_pos_int(t_id)
                elif t_prov == "tmdb_movie" and not tmdb_movie_id:
                    tmdb_movie_id = bm.parse_pos_int(t_id)
                elif t_prov == "imdb_movie" and not imdb_movie_id:
                    imdb_movie_id = bm.clean_imdb_id(t_id)
                elif t_prov in ("tvdb_show", "tmdb_show"):
                    show_id = bm.parse_pos_int(t_id)
                    season_num = parse_int(t_scope[1:]) if t_scope and t_scope.startswith("s") else None
                    if not show_id or season_num is None or season_num < 0:
                        continue
                    part_idx = 0
                    for s_rng_str, t_rng_str in ranges_dict.items():
                        s_ranges = _parse_ep_range_spec(s_rng_str)
                        t_ranges = _parse_ep_range_spec(t_rng_str)
                        if not s_ranges or not t_ranges:
                            continue
                        ep_start = s_ranges[0][0]
                        r_start, r_end = t_ranges[0]
                        part_idx += 1
                        if t_prov == "tvdb_show":
                            tvdb_show_candidates.append((1 if season_num == 0 else 0, ep_start, season_num, show_id, r_start, r_end))
                            rule_rows.append((mid, show_id, season_num, part_idx, "explicit", r_start, r_end, ep_start, "anibridge_v3_tvdb"))
                        else:
                            tmdb_show_candidates.append((1 if season_num == 0 else 0, ep_start, season_num, show_id, r_start, r_end))

            tvdb_show_id = None
            tvdb_season = None
            tvdb_part = None
            if tvdb_show_candidates:
                tvdb_show_candidates.sort(key=lambda x: (x[0], x[1], x[2]))
                _, _, tvdb_season, tvdb_show_id, r_start0, _ = tvdb_show_candidates[0]
                tvdb_part = 2 if (r_start0 and r_start0 > 1) else 1

            tmdb_show_id = None
            tmdb_season = None
            if tmdb_show_candidates:
                tmdb_show_candidates.sort(key=lambda x: (x[0], x[1], x[2]))
                _, _, tmdb_season, tmdb_show_id, _, _ = tmdb_show_candidates[0]

            mal_summaries[mid] = (
                mid,
                anilist_id,
                anidb_r_id or anidb_s_id,
                "R" if anidb_r_id else ("S" if anidb_s_id else None),
                tvdb_show_id,
                tvdb_season,
                tvdb_part,
                tvdb_movie_id,
                tmdb_show_id,
                tmdb_season,
                tmdb_movie_id,
                imdb_movie_id,
            )

    conn.executemany("INSERT INTO anibridge_edges VALUES (?,?,?,?,?,?,?,?,?,?)", edge_rows)
    conn.executemany("INSERT INTO anibridge_mal_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", list(mal_summaries.values()))
    conn.executemany("INSERT INTO anibridge_tvdb_rules VALUES (?,?,?,?,?,?,?,?,?)", rule_rows)
    conn.execute("CREATE INDEX idx_anibridge_src ON anibridge_edges(source_provider, source_id)")
    conn.execute("CREATE INDEX idx_anibridge_rules_mal ON anibridge_tvdb_rules(mal_id)")
    _finalize_service_db(conn, len(mal_summaries))
    print(
        f"[OK] anime_metadata_anibridge_mappings.db: {len(mal_summaries):,} MAL summaries, "
        f"{len(edge_rows):,} descriptor edges, {len(rule_rows):,} TVDB episode rules"
    )
    return len(mal_summaries)


def harvest_wikidata(data_dir, cache_dir):
    """Harvest Wikidata SPARQL cross-IDs into anime_metadata_wikidata_ids.db."""
    url = bm._build_wikidata_url()
    raw = bm.load_source_blob(
        "wikidata",
        url,
        cache_dir,
        max_cache_age=24 * 3600,
        extra_headers={"Accept": "application/sparql-results+json"},
        optional=True,
    )
    data = json.loads(raw.decode("utf-8")) if raw else {}
    bindings = (data.get("results") or {}).get("bindings") or []
    db_path = os.path.join(data_dir, "anime_metadata_wikidata_ids.db")
    conn = _init_service_db(db_path, "Wikidata Live SPARQL", bm.WIKIDATA_SPARQL_URL)
    conn.execute(
        """
        CREATE TABLE wikidata_entries (
            entry_idx INTEGER PRIMARY KEY,
            mal_id INTEGER NOT NULL,
            anilist_id INTEGER,
            anidb_id INTEGER,
            kitsu_id INTEGER,
            syobocal_tid INTEGER,
            annict_id INTEGER,
            thetvdb_id INTEGER,
            tmdb_tv_id INTEGER,
            tmdb_movie_id INTEGER,
            imdb_id TEXT
        )
        """
    )
    rows = []
    for idx, b in enumerate(bindings, start=1):
        if not isinstance(b, dict):
            continue
        mid = bm.parse_pos_int((b.get("mal") or {}).get("value"))
        if not mid:
            continue
        rows.append(
            (
                idx,
                mid,
                bm.parse_pos_int((b.get("anilist") or {}).get("value")),
                bm.parse_pos_int((b.get("anidb") or {}).get("value")),
                bm.parse_pos_int((b.get("kitsu") or {}).get("value")),
                bm.parse_pos_int((b.get("syobocal") or {}).get("value")),
                bm.parse_pos_int((b.get("annict") or {}).get("value")),
                bm.parse_pos_int((b.get("tvdb") or {}).get("value")),
                bm.parse_pos_int((b.get("tmdbTv") or {}).get("value")),
                bm.parse_pos_int((b.get("tmdbMovie") or {}).get("value")),
                bm.clean_imdb_id((b.get("imdb") or {}).get("value")),
            )
        )
    conn.executemany("INSERT INTO wikidata_entries VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.execute("CREATE INDEX idx_wikidata_mal ON wikidata_entries(mal_id)")
    _finalize_service_db(conn, len(rows))
    print(f"[OK] anime_metadata_wikidata_ids.db: {len(rows):,} SPARQL bindings")
    return len(rows)


def harvest_mal_dubs(data_dir, cache_dir):
    """Harvest 100% of MAL-Dubs/MAL-Dubs into anime_metadata_mal_dubs.db."""
    raw = bm.load_source_blob("mal_dubs", ACTIVE_SOURCE_URLS["mal_dubs"], cache_dir, max_cache_age=3600)
    data = json.loads(raw.decode("utf-8"))
    db_path = os.path.join(data_dir, "anime_metadata_mal_dubs.db")
    conn = _init_service_db(db_path, "MAL-Dubs/MAL-Dubs", ACTIVE_SOURCE_URLS["mal_dubs"])
    conn.execute(
        """
        CREATE TABLE mal_dubs (
            mal_id INTEGER PRIMARY KEY,
            is_complete_dub INTEGER NOT NULL,
            is_incomplete_dub INTEGER NOT NULL
        )
        """
    )
    dubbed = {bm.parse_pos_int(x) for x in (data.get("dubbed") or []) if bm.parse_pos_int(x)}
    incomplete = {bm.parse_pos_int(x) for x in (data.get("incomplete") or []) if bm.parse_pos_int(x)}
    all_ids = sorted(dubbed | incomplete)
    rows = [(mid, 1 if mid in dubbed else 0, 1 if mid in incomplete else 0) for mid in all_ids]
    conn.executemany("INSERT INTO mal_dubs VALUES (?,?,?)", rows)
    _finalize_service_db(conn, len(rows))
    print(f"[OK] anime_metadata_mal_dubs.db: {len(rows):,} dub IDs")
    return len(rows)


def harvest_anidb_titles(data_dir, cache_dir):
    """Harvest 100% of AniDB daily anime-titles.xml.gz into anime_metadata_anidb_titles.db."""
    raw = bm.load_source_blob("anidb_titles", ACTIVE_SOURCE_URLS["anidb_titles"], cache_dir, max_cache_age=24 * 3600)
    if raw[:2] == b"\x1f\x8b":
        xml_bytes = gzip.decompress(raw)
    else:
        xml_bytes = raw
    root = ET.fromstring(xml_bytes)
    db_path = os.path.join(data_dir, "anime_metadata_anidb_titles.db")
    conn = _init_service_db(db_path, "AniDB Daily anime-titles.xml.gz", ACTIVE_SOURCE_URLS["anidb_titles"])
    conn.execute(
        """
        CREATE TABLE anidb_titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anidb_id INTEGER NOT NULL,
            title_type TEXT NOT NULL,
            xml_lang TEXT NOT NULL,
            title TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE anidb_anime (
            anidb_id INTEGER PRIMARY KEY,
            main_title TEXT,
            official_en TEXT,
            official_ja TEXT,
            synonyms_json TEXT
        )
        """
    )
    xml_lang_attr = "{http://www.w3.org/XML/1998/namespace}lang"
    title_rows = []
    anime_rows = []
    for elem in root.findall("anime"):
        aid = bm.parse_pos_int(elem.attrib.get("aid"))
        if not aid:
            continue
        main_title = None
        official_en = None
        syn_en = None
        official_ja = None
        syns = []
        for t_el in elem.findall("title"):
            txt = (t_el.text or "").strip()
            if not txt:
                continue
            ttype = (t_el.attrib.get("type") or "syn").strip().lower()
            tlang = (t_el.attrib.get(xml_lang_attr) or t_el.attrib.get("lang") or "").strip().lower()
            title_rows.append((aid, ttype, tlang, txt))
            if ttype == "main" and not main_title:
                main_title = txt
            elif ttype == "official" and tlang == "en" and not official_en:
                official_en = txt
            elif ttype == "official" and tlang == "ja" and not official_ja:
                official_ja = txt
            elif ttype in ("syn", "synonym") and tlang == "en" and not syn_en:
                syn_en = txt
            if tlang in ("en", "x-jat") and txt not in syns:
                syns.append(txt)
        anime_rows.append((aid, main_title, official_en or syn_en, official_ja, json.dumps(syns, ensure_ascii=False)))

    conn.executemany("INSERT INTO anidb_titles (anidb_id, title_type, xml_lang, title) VALUES (?,?,?,?)", title_rows)
    conn.executemany("INSERT INTO anidb_anime VALUES (?,?,?,?,?)", anime_rows)
    conn.execute("CREATE INDEX idx_anidb_titles_aid ON anidb_titles(anidb_id)")
    _finalize_service_db(conn, len(anime_rows))
    print(f"[OK] anime_metadata_anidb_titles.db: {len(anime_rows):,} anime, {len(title_rows):,} total title rows")
    return len(anime_rows)


def _is_complete_anilist_payload(d):
    if not isinstance(d, dict) or not d.get("id"):
        return False
    if "genres" not in d or "studios" not in d or "startDate" not in d:
        return False
    sd = d.get("startDate")
    if isinstance(sd, dict) and sd.get("year") is not None and "month" not in sd:
        return False
    return True


def _fetch_anilist_5page_batch(id_chunks, var_field="id_in"):
    """Fetch up to 5 Page(perPage: 50) blocks (250 anime IDs) in a single AniList GraphQL request."""
    active_chunks = [c for c in id_chunks if c]
    if not active_chunks:
        return [], 30
    var_defs = ", ".join(f"$ids{i}: [Int]" for i in range(len(active_chunks)))
    pages = "\n".join(
        f"  p{i}: Page(page: 1, perPage: 50) {{ media({var_field}: $ids{i}, type: ANIME) {{ {ANILIST_MEDIA_FIELDS} }} }}"
        for i in range(len(active_chunks))
    )
    query = f"query ({var_defs}) {{\n{pages}\n}}"
    variables = {f"ids{i}": c for i, c in enumerate(active_chunks)}
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")

    for attempt in range(4):
        req = urllib.request.Request(
            ANILIST_GRAPHQL_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Otaku-Mapping-DB/2.7 (+https://github.com/RPDev88/Otaku-Mapping-DB)",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                rem = parse_int(resp.headers.get("X-RateLimit-Remaining"))
                payload = json.loads(resp.read().decode("utf-8"))
                out = []
                for k, page_obj in (payload.get("data") or {}).items():
                    for m in (page_obj or {}).get("media") or []:
                        if isinstance(m, dict) and m.get("id"):
                            out.append(m)
                return out, (rem if rem is not None else 15)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = min(65.0, float(parse_int(e.headers.get("Retry-After")) or 20))
                print(f"  [AniList 429] sleeping {retry_after:.1f}s (attempt {attempt + 1}/4)...", flush=True)
                time.sleep(retry_after + 1.0)
                continue
            print(f"  [AniList HTTP {e.code}] {e}", flush=True)
            break
        except Exception as e:
            print(f"  [AniList Error] {e}", flush=True)
            time.sleep(2.0)
    return [], 15


def harvest_anilist_catalog(data_dir, cache_dir, seed_db_path, max_http_batches=200):
    """Harvest the complete historical & modern AniList catalog into anime_metadata_anilist_catalog.db."""
    db_path = os.path.join(data_dir, "anime_metadata_anilist_catalog.db")

    # 1. Collect all known anilist_id and mal_id across our active per-service DBs + seed
    known_anilist_ids = set()
    known_mal_ids = set()
    mal_by_anilist = {}

    for svc_db, sql_q in [
        ("anime_metadata_fribb_ids.db", "SELECT mal_id, anilist_id FROM fribb_entries"),
        ("anime_metadata_arm_ids.db", "SELECT mal_id, anilist_id FROM arm_entries"),
        ("anime_metadata_anibridge_mappings.db", "SELECT mal_id, anilist_id FROM anibridge_mal_summary"),
        ("anime_metadata_kometa_ids.db", "SELECT mal_id_single, anilist_id FROM kometa_entries"),
        ("anime_metadata_wikidata_ids.db", "SELECT mal_id, anilist_id FROM wikidata_entries"),
    ]:
        p = os.path.join(data_dir, svc_db)
        if os.path.isfile(p):
            c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
            for mid, alid in c.execute(sql_q):
                if mid and mid > 0:
                    known_mal_ids.add(int(mid))
                if alid and alid > 0:
                    known_anilist_ids.add(int(alid))
                    if mid and mid > 0 and int(alid) not in mal_by_anilist:
                        mal_by_anilist[int(alid)] = int(mid)
            c.close()

    if seed_db_path and os.path.isfile(seed_db_path):
        c = sqlite3.connect(f"file:{seed_db_path}?mode=ro", uri=True)
        for mid, alid in c.execute("SELECT mal_id, anilist_id FROM anime"):
            if mid and mid > 0:
                known_mal_ids.add(int(mid))
            if alid and alid > 0:
                known_anilist_ids.add(int(alid))
                if mid and mid > 0 and int(alid) not in mal_by_anilist:
                    mal_by_anilist[int(alid)] = int(mid)
        c.close()

    # 2. Load all existing non-empty payloads from .cache/anilist__*.json or existing anime_metadata_anilist_catalog.db
    media_by_alid = {}
    complete_alids = set()
    if os.path.isfile(db_path):
        try:
            c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            for (raw_json,) in c.execute("SELECT raw_json FROM anilist_media WHERE raw_json IS NOT NULL"):
                d = json.loads(raw_json)
                if isinstance(d, dict) and d.get("id"):
                    alid = int(d["id"])
                    media_by_alid[alid] = d
                    if _is_complete_anilist_payload(d):
                        complete_alids.add(alid)
            c.close()
        except Exception:
            pass

    for p in glob.glob(os.path.join(cache_dir, "anilist__*.json")) + glob.glob(os.path.join(cache_dir, "anilist_by_id__*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = (json.load(f) or {}).get("data")
            if isinstance(d, dict) and d.get("id"):
                alid = int(d["id"])
                if alid not in complete_alids:
                    media_by_alid[alid] = d
                    if _is_complete_anilist_payload(d):
                        complete_alids.add(alid)
        except Exception:
            pass

    unfetched_alids = sorted(known_anilist_ids - set(media_by_alid.keys()))
    upgrade_alids = sorted((known_anilist_ids & set(media_by_alid.keys())) - complete_alids)
    missing_alids = unfetched_alids + upgrade_alids
    print(
        f"[AniList Catalog] Known anilist_ids={len(known_anilist_ids):,}, "
        f"in_cache={len(media_by_alid):,} (v2.7_complete={len(complete_alids):,}), "
        f"unfetched={len(unfetched_alids):,}, to_upgrade={len(upgrade_alids):,}",
        flush=True,
    )

    # 3. Fetch missing anilist_ids in 5-page batches (250 IDs per request)
    cache = bm.ApiCache(cache_dir)
    chunks_50 = [missing_alids[i : i + 50] for i in range(0, len(missing_alids), 50)]
    batches_250 = [chunks_50[i : i + 5] for i in range(0, len(chunks_50), 5)]

    for b_idx, batch in enumerate(batches_250[:max_http_batches], start=1):
        t_req = time.time()
        fetched_items, rem = _fetch_anilist_5page_batch(batch, var_field="id_in")
        for m in fetched_items:
            alid = int(m["id"])
            media_by_alid[alid] = m
            cache.set("anilist_by_id", alid, m)
            mid = bm.parse_pos_int(m.get("idMal")) or mal_by_anilist.get(alid)
            if mid:
                cache.set("anilist", mid, m)
        if b_idx % 5 == 0 or b_idx == len(batches_250):
            print(
                f"  [AniList id_in batch {b_idx}/{len(batches_250)}] "
                f"collected={len(media_by_alid):,}, rate_rem={rem}",
                flush=True,
            )
        if b_idx % 15 == 0:
            _write_anilist_catalog_db(db_path, media_by_alid, mal_by_anilist)
        # Pace cleanly at <= 28 Pages/minute (5 pages = ~10.8s window)
        elapsed = time.time() - t_req
        sleep_s = max(1.5, 10.8 - elapsed) if rem > 8 else max(12.0, 20.0 - elapsed)
        if b_idx < len(batches_250[:max_http_batches]):
            time.sleep(sleep_s)

    # 4. Also check any known_mal_ids not yet covered by media_by_alid
    covered_mids = set()
    for alid, m in media_by_alid.items():
        mid = bm.parse_pos_int(m.get("idMal")) or mal_by_anilist.get(alid)
        if mid:
            covered_mids.add(mid)
    unlinked_mids = sorted(
        mid for mid in (known_mal_ids - covered_mids)
        if cache.get("anilist", mid, max_age_seconds=30 * 86400) != {}
    )
    if unlinked_mids and max_http_batches > len(batches_250):
        rem_budget = max_http_batches - len(batches_250)
        m_chunks_50 = [unlinked_mids[i : i + 50] for i in range(0, len(unlinked_mids), 50)]
        m_batches_250 = [m_chunks_50[i : i + 5] for i in range(0, len(m_chunks_50), 5)]
        print(f"[AniList Catalog] Querying {len(unlinked_mids):,} unlinked mal_ids via idMal_in ({len(m_batches_250)} batches)...", flush=True)
        for b_idx, batch in enumerate(m_batches_250[:rem_budget], start=1):
            t_req = time.time()
            fetched_items, rem = _fetch_anilist_5page_batch(batch, var_field="idMal_in")
            found_mids = set()
            for m in fetched_items:
                alid = int(m["id"])
                media_by_alid[alid] = m
                cache.set("anilist_by_id", alid, m)
                mid = bm.parse_pos_int(m.get("idMal"))
                if mid:
                    found_mids.add(mid)
                    cache.set("anilist", mid, m)
            for chunk in batch:
                for mid in chunk:
                    if mid not in found_mids:
                        cache.set("anilist", mid, {})
            if b_idx % 15 == 0:
                _write_anilist_catalog_db(db_path, media_by_alid, mal_by_anilist)
            elapsed = time.time() - t_req
            sleep_s = max(1.5, 10.8 - elapsed) if rem > 8 else max(12.0, 20.0 - elapsed)
            if b_idx < len(m_batches_250[:rem_budget]):
                time.sleep(sleep_s)

    return _write_anilist_catalog_db(db_path, media_by_alid, mal_by_anilist)


def _write_anilist_catalog_db(db_path, media_by_alid, mal_by_anilist):
    # 5. Write anime_metadata_anilist_catalog.db
    tmp_path = f"{db_path}.tmp"
    conn = _init_service_db(tmp_path, "AniList GraphQL API", ANILIST_GRAPHQL_URL)
    conn.execute(
        """
        CREATE TABLE anilist_media (
            anilist_id INTEGER PRIMARY KEY,
            mal_id INTEGER,
            format TEXT,
            status TEXT,
            episodes INTEGER,
            duration_seconds INTEGER,
            season TEXT,
            season_year INTEGER,
            year INTEGER,
            start_date TEXT,
            end_date TEXT,
            title_romaji TEXT,
            title_english TEXT,
            title_native TEXT,
            title_user_preferred TEXT,
            synonyms_json TEXT,
            genres_csv TEXT,
            studios_csv TEXT,
            cover_image_large TEXT,
            cover_image_medium TEXT,
            raw_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE anilist_relations (
            anilist_id INTEGER NOT NULL,
            mal_id INTEGER,
            related_anilist_id INTEGER NOT NULL,
            related_mal_id INTEGER,
            relation_type TEXT NOT NULL,
            related_format TEXT,
            PRIMARY KEY (anilist_id, related_anilist_id, relation_type)
        )
        """
    )

    media_rows = []
    rel_rows = []
    for alid, m in sorted(media_by_alid.items()):
        mid = bm.parse_pos_int(m.get("idMal")) or mal_by_anilist.get(alid)
        fmt = normalize_media_type(m.get("format"))
        status = ANILIST_STATUS_MAP.get(str(m.get("status") or "").upper())
        eps = bm.parse_pos_int(m.get("episodes"))
        dur_m = bm.parse_pos_int(m.get("duration"))
        dur_s = dur_m * 60 if dur_m else None
        season = str(m.get("season") or "").strip().upper() or None
        s_year = bm.parse_pos_int(m.get("seasonYear"))
        sd = m.get("startDate") if isinstance(m.get("startDate"), dict) else {}
        ed = m.get("endDate") if isinstance(m.get("endDate"), dict) else {}
        sd_y = bm.parse_pos_int(sd.get("year"))
        year = s_year if (s_year and 1900 <= s_year <= 2100) else (sd_y if (sd_y and 1900 <= sd_y <= 2100) else None)
        start_date = None
        if sd_y and 1900 <= sd_y <= 2100:
            sm = max(1, min(12, bm.parse_pos_int(sd.get("month")) or 1))
            sday = max(1, min(31, bm.parse_pos_int(sd.get("day")) or 1))
            start_date = f"{sd_y:04d}-{sm:02d}-{sday:02d}"
        end_date = None
        ed_y = bm.parse_pos_int(ed.get("year"))
        if ed_y and 1900 <= ed_y <= 2100:
            em = max(1, min(12, bm.parse_pos_int(ed.get("month")) or 1))
            eday = max(1, min(31, bm.parse_pos_int(ed.get("day")) or 1))
            end_date = f"{ed_y:04d}-{em:02d}-{eday:02d}"
        if not season and isinstance(sd.get("month"), int) and 1 <= sd["month"] <= 12:
            m_num = sd["month"]
            season = "WINTER" if m_num in (1, 2, 3) else ("SPRING" if m_num in (4, 5, 6) else ("SUMMER" if m_num in (7, 8, 9) else "FALL"))

        t_obj = m.get("title") if isinstance(m.get("title"), dict) else {}
        syns = [str(s).strip() for s in (m.get("synonyms") or []) if isinstance(s, str) and str(s).strip()]
        genres = _extract_genres(m.get("genres"))
        st_nodes = ((m.get("studios") or {}).get("nodes") or []) if isinstance(m.get("studios"), dict) else []
        anim_st = [str(n.get("name")).strip() for n in st_nodes if isinstance(n, dict) and n.get("name") and n.get("isAnimationStudio")]
        if not anim_st:
            anim_st = [str(n.get("name")).strip() for n in st_nodes if isinstance(n, dict) and n.get("name")]
        studios_csv = ",".join(dict.fromkeys(anim_st)) if anim_st else None
        cov = m.get("coverImage") if isinstance(m.get("coverImage"), dict) else {}

        media_rows.append(
            (
                alid,
                mid,
                fmt,
                status,
                eps,
                dur_s,
                season,
                s_year,
                year,
                start_date,
                end_date,
                (t_obj.get("romaji") or "").strip() or None,
                (t_obj.get("english") or "").strip() or None,
                (t_obj.get("native") or "").strip() or None,
                (t_obj.get("userPreferred") or "").strip() or None,
                json.dumps(syns, ensure_ascii=False) if syns else None,
                genres,
                studios_csv,
                (cov.get("large") or "").strip() or None,
                (cov.get("medium") or "").strip() or None,
                json.dumps(m, ensure_ascii=False),
            )
        )

        edges = ((m.get("relations") or {}).get("edges") or []) if isinstance(m.get("relations"), dict) else []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            rtype = str(edge.get("relationType") or "").strip().upper()
            node = edge.get("node") if isinstance(edge.get("node"), dict) else {}
            if not rtype or node.get("type") not in (None, "ANIME"):
                continue
            rel_alid = bm.parse_pos_int(node.get("id"))
            if not rel_alid:
                continue
            rel_mid = bm.parse_pos_int(node.get("idMal")) or mal_by_anilist.get(rel_alid)
            rel_rows.append((alid, mid, rel_alid, rel_mid, rtype, normalize_media_type(node.get("format"))))

    conn.executemany("INSERT OR REPLACE INTO anilist_media VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", media_rows)
    conn.executemany("INSERT OR IGNORE INTO anilist_relations VALUES (?,?,?,?,?,?)", rel_rows)
    conn.execute("CREATE INDEX idx_anilist_mal ON anilist_media(mal_id) WHERE mal_id IS NOT NULL")
    conn.execute("CREATE INDEX idx_anilist_rel_mal ON anilist_relations(mal_id, related_mal_id)")
    _finalize_service_db(conn, len(media_rows))
    os.replace(tmp_path, db_path)
    print(f"[OK] anime_metadata_anilist_catalog.db: {len(media_rows):,} media rows, {len(rel_rows):,} typed relation edges", flush=True)
    return len(media_rows)


def harvest_trakt_anizip_simkl_filler(data_dir, cache_dir, seed_db_path):
    """Harvest Trakt, AniZip, Simkl, and AnimeFillerList into their dedicated per-service databases."""
    seed_rows = bm.load_seed_database(seed_db_path) if (seed_db_path and os.path.isfile(seed_db_path)) else {}

    # 1. Trakt IDs DB
    trakt_db = os.path.join(data_dir, "anime_metadata_trakt_ids.db")
    conn = _init_service_db(trakt_db, "Trakt API & Verified Mappings", "https://api.trakt.tv")
    conn.execute(
        """
        CREATE TABLE trakt_mappings (
            mal_id INTEGER PRIMARY KEY,
            trakt_id INTEGER NOT NULL,
            trakt_slug TEXT,
            media_type TEXT,
            thetvdb_id INTEGER,
            themoviedb_id INTEGER,
            imdb_id TEXT,
            source TEXT NOT NULL
        )
        """
    )
    trakt_map = {}
    for mid, r in seed_rows.items():
        tid = bm.parse_pos_int(r.get("trakt_id"))
        if tid:
            trakt_map[mid] = (
                mid,
                tid,
                r.get("trakt_slug"),
                r.get("global_media_type"),
                bm.parse_pos_int(r.get("thetvdb_id")),
                bm.parse_pos_int(r.get("themoviedb_id")),
                bm.clean_imdb_id(r.get("imdb_id")),
                "seed",
            )
    for p in glob.glob(os.path.join(cache_dir, "trakt__*.json")):
        try:
            mid = bm.parse_pos_int(os.path.basename(p).split("__")[1].split(".")[0])
            if not mid:
                continue
            with open(p, "r", encoding="utf-8") as f:
                d = (json.load(f) or {}).get("data")
            if isinstance(d, int) and d > 0:
                prev = trakt_map.get(mid)
                trakt_map[mid] = (
                    mid,
                    d,
                    prev[2] if prev else None,
                    prev[3] if prev else None,
                    prev[4] if prev else None,
                    prev[5] if prev else None,
                    prev[6] if prev else None,
                    "trakt_cache",
                )
            elif isinstance(d, dict) and bm.parse_pos_int(d.get("trakt_id")):
                trakt_map[mid] = (
                    mid,
                    bm.parse_pos_int(d.get("trakt_id")),
                    d.get("trakt_slug"),
                    d.get("media_type"),
                    bm.parse_pos_int(d.get("thetvdb_id")),
                    bm.parse_pos_int(d.get("themoviedb_id")),
                    bm.clean_imdb_id(d.get("imdb_id")),
                    "trakt_api",
                )
        except Exception:
            pass
    conn.executemany("INSERT OR REPLACE INTO trakt_mappings VALUES (?,?,?,?,?,?,?,?)", list(trakt_map.values()))
    _finalize_service_db(conn, len(trakt_map))
    print(f"[OK] anime_metadata_trakt_ids.db: {len(trakt_map):,} Trakt mappings")

    # 2. AniZip Mappings DB
    anizip_db = os.path.join(data_dir, "anime_metadata_anizip_mappings.db")
    conn = _init_service_db(anizip_db, "AniZip API", "https://api.ani.zip/mappings")
    conn.execute(
        """
        CREATE TABLE anizip_entries (
            mal_id INTEGER PRIMARY KEY,
            anidb_id INTEGER,
            anilist_id INTEGER,
            kitsu_id INTEGER,
            thetvdb_id INTEGER,
            themoviedb_id INTEGER,
            imdb_id TEXT,
            simkl_id INTEGER,
            livechart_id INTEGER,
            notifymoe_id TEXT,
            episode_count INTEGER,
            special_count INTEGER,
            titles_json TEXT,
            raw_json TEXT NOT NULL
        )
        """
    )
    az_rows = []
    for p in glob.glob(os.path.join(cache_dir, "anizip__*.json")):
        try:
            mid = bm.parse_pos_int(os.path.basename(p).split("__")[1].split(".")[0])
            with open(p, "r", encoding="utf-8") as f:
                d = (json.load(f) or {}).get("data")
            if not mid or not isinstance(d, dict) or not d:
                continue
            mappings = d.get("mappings") if isinstance(d.get("mappings"), dict) else {}
            az_rows.append(
                (
                    mid,
                    bm.parse_pos_int(mappings.get("anidb_id")),
                    bm.parse_pos_int(mappings.get("anilist_id")),
                    bm.parse_pos_int(mappings.get("kitsu_id")),
                    bm.parse_pos_int(mappings.get("thetvdb_id")),
                    bm.parse_pos_int(mappings.get("themoviedb_id")),
                    bm.clean_imdb_id(mappings.get("imdb_id")),
                    bm.parse_pos_int(mappings.get("simkl_id")),
                    bm.parse_pos_int(mappings.get("livechart_id")),
                    str(mappings.get("notifymoe_id") or "").strip() or None,
                    bm.parse_pos_int(d.get("episodeCount")),
                    bm.parse_pos_int(d.get("specialCount")),
                    json.dumps(d.get("titles") or {}, ensure_ascii=False),
                    json.dumps(d, ensure_ascii=False),
                )
            )
        except Exception:
            pass
    conn.executemany("INSERT OR REPLACE INTO anizip_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", az_rows)
    _finalize_service_db(conn, len(az_rows))
    print(f"[OK] anime_metadata_anizip_mappings.db: {len(az_rows):,} cached AniZip records")

    # 3. Simkl Catalog DB
    simkl_db = os.path.join(data_dir, "anime_metadata_simkl_catalog.db")
    conn = _init_service_db(simkl_db, "Simkl API", "https://api.simkl.com")
    conn.execute(
        """
        CREATE TABLE simkl_entries (
            mal_id INTEGER PRIMARY KEY,
            simkl_id INTEGER NOT NULL,
            anidb_id INTEGER,
            thetvdb_id INTEGER,
            themoviedb_id INTEGER,
            imdb_id TEXT,
            title TEXT,
            title_en TEXT,
            year INTEGER,
            media_type TEXT,
            total_episodes INTEGER,
            status TEXT,
            genres_csv TEXT,
            studios_csv TEXT,
            source TEXT NOT NULL
        )
        """
    )
    simkl_map = {}
    for mid, r in seed_rows.items():
        sid = bm.parse_pos_int(r.get("simkl_id"))
        if sid:
            simkl_map[mid] = (
                mid,
                sid,
                bm.parse_pos_int(r.get("anidb_id")),
                bm.parse_pos_int(r.get("thetvdb_id")),
                bm.parse_pos_int(r.get("themoviedb_id")),
                bm.clean_imdb_id(r.get("imdb_id")),
                r.get("mal_title"),
                r.get("title_english"),
                bm.parse_pos_int(r.get("year")),
                r.get("anime_media_type"),
                bm.parse_pos_int(r.get("episodes")),
                r.get("status"),
                r.get("genres"),
                r.get("studios"),
                "seed",
            )
    for p in glob.glob(os.path.join(cache_dir, "simkl__*.json")):
        try:
            mid = bm.parse_pos_int(os.path.basename(p).split("__")[1].split(".")[0])
            with open(p, "r", encoding="utf-8") as f:
                d = (json.load(f) or {}).get("data")
            if not mid or not isinstance(d, dict) or not d:
                continue
            ids = d.get("ids") if isinstance(d.get("ids"), dict) else {}
            sid = bm.parse_pos_int(ids.get("simkl") or ids.get("simkl_id") or d.get("simkl_id"))
            if not sid:
                continue
            st_list = [str(s.get("name")).strip() for s in (d.get("studios") or []) if isinstance(s, dict) and s.get("name")]
            simkl_map[mid] = (
                mid,
                sid,
                bm.parse_pos_int(ids.get("anidb")),
                bm.parse_pos_int(ids.get("tvdb")),
                bm.parse_pos_int(ids.get("tmdb")),
                bm.clean_imdb_id(ids.get("imdb")),
                (d.get("title") or "").strip() or None,
                (d.get("en_title") or "").strip() or None,
                bm.parse_pos_int(d.get("year")),
                normalize_media_type(d.get("anime_type") or d.get("type")),
                bm.parse_pos_int(d.get("total_episodes")),
                str(d.get("status") or "").upper() or None,
                _extract_genres(d.get("genres")),
                ",".join(dict.fromkeys(st_list)) if st_list else None,
                "simkl_api",
            )
        except Exception:
            pass
    conn.executemany("INSERT OR REPLACE INTO simkl_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", list(simkl_map.values()))
    _finalize_service_db(conn, len(simkl_map))
    print(f"[OK] anime_metadata_simkl_catalog.db: {len(simkl_map):,} Simkl entries")

    # 4. Filler Ranges DB
    filler_db = os.path.join(data_dir, "anime_metadata_filler_ranges.db")
    conn = _init_service_db(filler_db, "AnimeFillerList", "https://www.animefillerlist.com/shows")
    conn.execute(
        """
        CREATE TABLE filler_shows (
            slug TEXT PRIMARY KEY,
            title TEXT,
            max_ep INTEGER,
            episodes_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE filler_ranges (
            mal_id INTEGER NOT NULL,
            ep_start INTEGER NOT NULL,
            ep_end INTEGER NOT NULL,
            filler_type TEXT NOT NULL,
            PRIMARY KEY (mal_id, ep_start)
        )
        """
    )
    show_rows = []
    for p in glob.glob(os.path.join(cache_dir, "afl__show_*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = (json.load(f) or {}).get("data")
            if isinstance(d, dict) and d.get("episodes"):
                slug = d.get("slug") or os.path.basename(p).split("__show_", 1)[1].rsplit(".", 1)[0]
                show_rows.append((slug, d.get("title"), bm.parse_pos_int(d.get("max_ep")), json.dumps(d.get("episodes"), ensure_ascii=False)))
        except Exception:
            pass
    range_rows = []
    if seed_db_path and os.path.isfile(seed_db_path):
        try:
            sc = sqlite3.connect(f"file:{seed_db_path}?mode=ro", uri=True)
            range_rows = sc.execute("SELECT mal_id, ep_start, ep_end, filler_type FROM filler_ranges").fetchall()
            sc.close()
        except Exception:
            pass
    conn.executemany("INSERT OR REPLACE INTO filler_shows VALUES (?,?,?,?)", show_rows)
    conn.executemany("INSERT OR REPLACE INTO filler_ranges VALUES (?,?,?,?)", range_rows)
    _finalize_service_db(conn, len(range_rows) or len(show_rows))
    print(f"[OK] anime_metadata_filler_ranges.db: {len(show_rows):,} cached AnimeFillerList shows, {len(range_rows):,} mapped filler ranges")


def main():
    parser = argparse.ArgumentParser(description="Harvest active anime sources into data/anime_metadata_<service>_<data>.db")
    parser.add_argument("--data-dir", default="data", help="Output directory for per-service SQLite DBs")
    parser.add_argument("--cache-dir", default=".cache", help="Persistent download & API cache directory")
    parser.add_argument("--seed", default="dist/anime_mappings.db", help="Seed DB for verified historical Trakt/Simkl IDs")
    parser.add_argument("--max-anilist-batches", type=int, default=200, help="Max 250-ID AniList GraphQL batches per run")
    parser.add_argument("--only-anilist", action="store_true", help="Only run AniList catalog harvester")
    args = parser.parse_args()

    if not args.only_anilist:
        harvest_fribb(args.data_dir, args.cache_dir)
        harvest_arm(args.data_dir, args.cache_dir)
        harvest_kometa(args.data_dir, args.cache_dir)
        harvest_animelists_xml(args.data_dir, args.cache_dir)
        harvest_anibridge_v3(args.data_dir, args.cache_dir)
        harvest_wikidata(args.data_dir, args.cache_dir)
        harvest_mal_dubs(args.data_dir, args.cache_dir)
        harvest_anidb_titles(args.data_dir, args.cache_dir)
        harvest_trakt_anizip_simkl_filler(args.data_dir, args.cache_dir, args.seed)
    harvest_anilist_catalog(args.data_dir, args.cache_dir, args.seed, max_http_batches=args.max_anilist_batches)
    return 0


if __name__ == "__main__":
    sys.exit(main())
