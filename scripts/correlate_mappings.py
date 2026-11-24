#!/usr/bin/env python3
"""Correlate all active per-service anime_metadata_<service>_<data>.db databases.

Performs multi-source consensus voting, transitive 1-to-1 ID bridging, conflict auditing,
and canonical tvdb_rules unification across strictly active sources:
  - anime_metadata_anilist_catalog.db
  - anime_metadata_anibridge_mappings.db (anibridge/anibridge-mappings v3)
  - anime_metadata_fribb_ids.db
  - anime_metadata_arm_ids.db
  - anime_metadata_kometa_ids.db
  - anime_metadata_animelists_xml.db
  - anime_metadata_wikidata_ids.db
  - anime_metadata_mal_dubs.db
  - anime_metadata_anidb_titles.db
  - anime_metadata_trakt_ids.db
  - anime_metadata_anizip_mappings.db
  - anime_metadata_simkl_catalog.db
  - anime_metadata_filler_ranges.db

Produces:
  1. dist/correlation_report.json — Full data-integrity & cross-source conflict audit report
  2. dist/anime_mappings.db (+ .xz) — Lean service-interconnect & routing DB for Otaku-RPDev
  3. dist/anime_metadata.db (+ .xz) — Comprehensive correlated metadata DB across all services
"""

import argparse
import collections
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

import build_mappings as bm  # noqa: E402


def _ro_conn(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _pick_consensus(claims_with_weight):
    """Given [(source_name, weight, value), ...], pick the highest total weight value and report conflicts."""
    valid = [(s, w, v) for s, w, v in claims_with_weight if v is not None and v != ""]
    if not valid:
        return None, None, False
    scores = collections.defaultdict(float)
    top_source = {}
    for s, w, v in valid:
        scores[v] += w
        if v not in top_source or w > top_source[v][1]:
            top_source[v] = (s, w)
    ranked = sorted(scores.items(), key=lambda kv: (kv[1], top_source[kv[0]][1]), reverse=True)
    winner_val = ranked[0][0]
    winner_src = top_source[winner_val][0]
    had_conflict = len(ranked) > 1
    return winner_val, winner_src, had_conflict


def correlate_all_services(data_dir, seed_path, output_mappings_path, output_metadata_path, report_path, compress_xz=True):
    t0 = time.perf_counter()

    # 1. Load baseline seed records (preserving verified legacy IDs/titles from .seed/anime_mappings_original.db)
    seed_records = bm.load_seed_database(seed_path) if (seed_path and os.path.isfile(seed_path)) else {}
    seed_ids = set(seed_records.keys())

    # Per-service claims indexed by mal_id
    fribb_by_mal = {}
    fribb_no_mal = []
    arm_by_mal = {}
    kometa_by_mal_1to1 = {}
    kometa_by_mal_multi = collections.defaultdict(list)
    kometa_by_anidb = {}
    anibridge_by_mal = {}
    anibridge_rules = []
    wikidata_by_mal = {}
    dubs_set = set()
    trakt_by_mal = {}
    anizip_by_mal = {}
    simkl_by_mal = {}
    anilist_by_mal = {}
    anilist_by_alid = {}
    anilist_relations = []
    anidb_titles_by_aid = {}
    animelists_by_aid = {}
    animelists_mappings_by_aid = collections.defaultdict(list)

    service_counts = {}

    # Load Fribb
    p = os.path.join(data_dir, "anime_metadata_fribb_ids.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM fribb_entries")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["fribb"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            mid = d.get("mal_id")
            if mid:
                fribb_by_mal[mid] = d
            else:
                fribb_no_mal.append(d)
        c.close()

    # Load ARM
    p = os.path.join(data_dir, "anime_metadata_arm_ids.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM arm_entries")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["arm"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            if d.get("mal_id"):
                arm_by_mal[d["mal_id"]] = d
        c.close()

    # Load Kometa
    p = os.path.join(data_dir, "anime_metadata_kometa_ids.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM kometa_entries")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["kometa"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            aid = d["anidb_id"]
            kometa_by_anidb[aid] = d
            if d.get("mal_id_single"):
                kometa_by_mal_1to1[d["mal_id_single"]] = d
            elif d.get("mal_ids_csv"):
                for tok in d["mal_ids_csv"].split(","):
                    m = bm.parse_pos_int(tok)
                    if m:
                        kometa_by_mal_multi[m].append(d)
        c.close()

    # Load AniBridge v3
    p = os.path.join(data_dir, "anime_metadata_anibridge_mappings.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM anibridge_mal_summary")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["anibridge_v3"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            anibridge_by_mal[d["mal_id"]] = d
        cur2 = c.execute("SELECT * FROM anibridge_tvdb_rules")
        rcols = [d[0] for d in cur2.description]
        for r in cur2.fetchall():
            anibridge_rules.append(dict(zip(rcols, r)))
        c.close()

    # Load Wikidata
    p = os.path.join(data_dir, "anime_metadata_wikidata_ids.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM wikidata_entries")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["wikidata"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            mid = d["mal_id"]
            prev = wikidata_by_mal.setdefault(mid, {})
            for k, v in d.items():
                if v is not None and not prev.get(k):
                    prev[k] = v
        c.close()

    # Load MAL-Dubs
    p = os.path.join(data_dir, "anime_metadata_mal_dubs.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        rows = c.execute("SELECT mal_id FROM mal_dubs").fetchall()
        service_counts["mal_dubs"] = len(rows)
        dubs_set = {r[0] for r in rows}
        c.close()

    # Load Anime-Lists XML
    p = os.path.join(data_dir, "anime_metadata_animelists_xml.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM animelists_entries")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["animelists_xml"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            animelists_by_aid[d["anidb_id"]] = d
        cur2 = c.execute("SELECT * FROM animelists_mappings")
        mcols = [d[0] for d in cur2.description]
        for r in cur2.fetchall():
            md = dict(zip(mcols, r))
            animelists_mappings_by_aid[md["anidb_id"]].append(md)
        c.close()

    # Load AniDB Titles
    p = os.path.join(data_dir, "anime_metadata_anidb_titles.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM anidb_anime")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["anidb_titles"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            anidb_titles_by_aid[d["anidb_id"]] = d
        c.close()

    # Load Trakt, AniZip, Simkl
    for key, fname, tbl in [
        ("trakt", "anime_metadata_trakt_ids.db", "trakt_mappings"),
        ("anizip", "anime_metadata_anizip_mappings.db", "anizip_entries"),
        ("simkl", "anime_metadata_simkl_catalog.db", "simkl_entries"),
    ]:
        p = os.path.join(data_dir, fname)
        if os.path.isfile(p):
            c = _ro_conn(p)
            cur = c.execute(f"SELECT * FROM {tbl}")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            service_counts[key] = len(rows)
            target = trakt_by_mal if key == "trakt" else (anizip_by_mal if key == "anizip" else simkl_by_mal)
            for r in rows:
                d = dict(zip(cols, r))
                target[d["mal_id"]] = d
            c.close()

    # Load AniList Catalog
    p = os.path.join(data_dir, "anime_metadata_anilist_catalog.db")
    if os.path.isfile(p):
        c = _ro_conn(p)
        cur = c.execute("SELECT * FROM anilist_media")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        service_counts["anilist_catalog"] = len(rows)
        for r in rows:
            d = dict(zip(cols, r))
            anilist_by_alid[d["anilist_id"]] = d
            if d.get("mal_id"):
                anilist_by_mal[d["mal_id"]] = d
        cur2 = c.execute("SELECT * FROM anilist_relations")
        rcols = [d[0] for d in cur2.description]
        for r in cur2.fetchall():
            anilist_relations.append(dict(zip(rcols, r)))
        c.close()

    # 2. Build the complete universe of MAL IDs from active sources + seed
    all_mal_ids = sorted(
        seed_ids
        | set(fribb_by_mal.keys())
        | set(arm_by_mal.keys())
        | set(kometa_by_mal_1to1.keys())
        | set(anibridge_by_mal.keys())
        | set(wikidata_by_mal.keys())
        | set(anilist_by_mal.keys())
        | set(trakt_by_mal.keys())
        | set(anizip_by_mal.keys())
        | set(simkl_by_mal.keys())
    )

    records = {}
    provenance = collections.defaultdict(dict)
    conflict_counters = collections.Counter()

    # Pass 1: Correlate primary IDs (anilist_id, anidb_id, kitsu_id, simkl_id, annict_id, syobocal_tid)
    claimed_anidb = set()
    for mid in all_mal_ids:
        rec = dict(seed_records.get(mid) or bm.empty_record(mid))
        rec["mal_id"] = mid
        # Drop volatile score from active correlation mutations; keep existing seed fields as lowest-weight fallback
        s_rec = seed_records.get(mid) or {}
        f_rec = fribb_by_mal.get(mid) or {}
        a_rec = arm_by_mal.get(mid) or {}
        k_rec = kometa_by_mal_1to1.get(mid) or {}
        k_multi = (kometa_by_mal_multi.get(mid) or [None])[0] or {}
        ab_rec = anibridge_by_mal.get(mid) or {}
        w_rec = wikidata_by_mal.get(mid) or {}
        al_rec = anilist_by_mal.get(mid) or {}
        az_rec = anizip_by_mal.get(mid) or {}
        sm_rec = simkl_by_mal.get(mid) or {}
        tr_rec = trakt_by_mal.get(mid) or {}

        # Correlate anilist_id
        al_val, al_src, al_conf = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("anilist_id")),
            ("anibridge_v3", 4.0, ab_rec.get("anilist_id")),
            ("arm", 3.5, a_rec.get("anilist_id")),
            ("fribb", 3.5, f_rec.get("anilist_id")),
            ("kometa", 3.0, k_rec.get("anilist_id")),
            ("wikidata", 2.5, w_rec.get("anilist_id")),
            ("anizip", 2.5, az_rec.get("anilist_id")),
            ("seed", 1.0, s_rec.get("anilist_id")),
        ])
        rec["anilist_id"] = al_val
        if al_src:
            provenance[mid]["anilist_id"] = al_src
        if al_conf:
            conflict_counters["anilist_id"] += 1

        # If anilist_by_mal didn't match directly (idMal was null on AniList), link via correlated anilist_id!
        if not al_rec and al_val and al_val in anilist_by_alid:
            al_rec = anilist_by_alid[al_val]

        # Correlate anidb_id (strictly 1-to-1 Regular scope preferred over Special scope)
        ab_anidb_r = ab_rec.get("anidb_id") if ab_rec.get("anidb_scope") == "R" else None
        aid_val, aid_src, aid_conf = _pick_consensus([
            ("fribb", 4.5, f_rec.get("anidb_id")),
            ("anibridge_v3_R", 4.0, ab_anidb_r),
            ("kometa_1to1", 3.5, k_rec.get("anidb_id")),
            ("anizip", 3.0, az_rec.get("anidb_id")),
            ("wikidata", 2.5, w_rec.get("anidb_id")),
            ("simkl", 2.0, sm_rec.get("anidb_id")),
            ("seed", 1.5, s_rec.get("anidb_id")),
        ])
        if aid_val and aid_val not in claimed_anidb:
            rec["anidb_id"] = aid_val
            claimed_anidb.add(aid_val)
            provenance[mid]["anidb_id"] = aid_src
        elif not aid_val:
            rec["anidb_id"] = None
        if aid_conf:
            conflict_counters["anidb_id"] += 1

        # Correlate kitsu_id
        kit_val, kit_src, kit_conf = _pick_consensus([
            ("fribb", 4.5, f_rec.get("kitsu_id")),
            ("anizip", 3.5, az_rec.get("kitsu_id")),
            ("wikidata", 3.0, w_rec.get("kitsu_id")),
            ("seed", 1.5, s_rec.get("kitsu_id")),
        ])
        rec["kitsu_id"] = kit_val
        if kit_src:
            provenance[mid]["kitsu_id"] = kit_src
        if kit_conf:
            conflict_counters["kitsu_id"] += 1

        # Correlate simkl_id
        sim_val, sim_src, sim_conf = _pick_consensus([
            ("simkl_api", 5.0, sm_rec.get("simkl_id") if sm_rec.get("source") == "simkl_api" else None),
            ("fribb", 4.5, f_rec.get("simkl_id")),
            ("anizip", 3.5, az_rec.get("simkl_id")),
            ("seed", 1.5, s_rec.get("simkl_id")),
        ])
        rec["simkl_id"] = sim_val
        if sim_src:
            provenance[mid]["simkl_id"] = sim_src
        if sim_conf:
            conflict_counters["simkl_id"] += 1

        # MAL Dub ID
        rec["mal_dub_id"] = mid if mid in dubs_set else None

        records[mid] = rec

    # Secondary 1-to-1 bridge for Fribb entries lacking mal_id
    by_alid = {r["anilist_id"]: mid for mid, r in records.items() if r.get("anilist_id")}
    by_aid = {r["anidb_id"]: mid for mid, r in records.items() if r.get("anidb_id")}
    by_kit = {r["kitsu_id"]: mid for mid, r in records.items() if r.get("kitsu_id")}
    by_sim = {r["simkl_id"]: mid for mid, r in records.items() if r.get("simkl_id")}

    for d in fribb_no_mal:
        cand_mids = {
            m for m in (
                by_alid.get(d.get("anilist_id")),
                by_aid.get(d.get("anidb_id")),
                by_kit.get(d.get("kitsu_id")),
                by_sim.get(d.get("simkl_id")),
            )
            if m
        }
        if len(cand_mids) == 1:
            mid = next(iter(cand_mids))
            if mid not in fribb_by_mal:
                fribb_by_mal[mid] = d

    # Pass 2: Correlate Media Type, Episode Counts, TVDB/TMDB/IMDb/Trakt IDs, Seasons, and Metadata
    ep_counts = {}
    schedule_ids = {}
    aliases_map = {}

    for mid, rec in records.items():
        s_rec = seed_records.get(mid) or {}
        f_rec = fribb_by_mal.get(mid) or {}
        a_rec = arm_by_mal.get(mid) or {}
        k_rec = kometa_by_mal_1to1.get(mid) or {}
        k_multi = (kometa_by_mal_multi.get(mid) or [None])[0] or {}
        ab_rec = anibridge_by_mal.get(mid) or {}
        w_rec = wikidata_by_mal.get(mid) or {}
        al_id = rec.get("anilist_id")
        al_rec = anilist_by_mal.get(mid) or (anilist_by_alid.get(al_id) if al_id else None) or {}
        az_rec = anizip_by_mal.get(mid) or {}
        sm_rec = simkl_by_mal.get(mid) or {}
        tr_rec = trakt_by_mal.get(mid) or {}
        aid = rec.get("anidb_id")
        xl_rec = animelists_by_aid.get(aid) if aid else {}
        if not xl_rec:
            xl_rec = {}
        adb_t = anidb_titles_by_aid.get(aid) if aid else {}
        if not adb_t:
            adb_t = {}

        # Schedule IDs (ARM + Wikidata)
        annict_id = a_rec.get("annict_id") or w_rec.get("annict_id")
        syobocal_tid = a_rec.get("syobocal_tid") or w_rec.get("syobocal_tid")
        if annict_id or syobocal_tid:
            schedule_ids[mid] = (annict_id, syobocal_tid)

        # Media Type
        mtype, mtype_src, _ = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("format")),
            ("fribb", 3.5, f_rec.get("media_type")),
            ("simkl", 3.0, sm_rec.get("media_type")),
            ("seed", 2.0, s_rec.get("anime_media_type")),
        ])
        rec["anime_media_type"] = mtype or rec.get("anime_media_type") or "TV"

        # Episodes, Status, Duration, Year, Season, Start Date, Genres, Studios, Picture
        eps, _, _ = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("episodes")),
            ("anizip", 3.5, az_rec.get("episode_count")),
            ("simkl", 3.0, sm_rec.get("total_episodes")),
            ("seed", 2.0, s_rec.get("episodes")),
        ])
        if eps and eps > 0:
            rec["episodes"] = eps
            ep_counts[mid] = eps
            rec["anime_media_episodes"] = f"1 - {eps}"
            g_rng = bm.parse_range_tuple(rec.get("global_media_episodes"))
            if not g_rng or g_rng[0] == 1:
                rec["global_media_episodes"] = f"1 - {eps}"
            else:
                rec["global_media_episodes"] = f"{g_rng[0]} - {g_rng[0] + eps - 1}"
        else:
            rg = bm.parse_range_tuple(rec.get("anime_media_episodes"))
            if rg:
                rec["episodes"] = rg[1]
                ep_counts[mid] = rg[1]

        status_val, _, _ = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("status")),
            ("seed", 2.0, s_rec.get("status")),
        ])
        if status_val:
            rec["status"] = status_val

        dur_val, _, _ = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("duration_seconds")),
            ("seed", 2.0, s_rec.get("duration")),
        ])
        if dur_val:
            rec["duration"] = dur_val

        year_val, _, _ = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("year")),
            ("simkl", 3.0, sm_rec.get("year")),
            ("seed", 2.0, s_rec.get("year")),
        ])
        if year_val:
            rec["year"] = year_val

        season_val, _, _ = _pick_consensus([
            ("anilist_graphql", 5.0, al_rec.get("season")),
            ("seed", 2.0, s_rec.get("anime_season")),
        ])
        if season_val:
            rec["anime_season"] = season_val

        if al_rec.get("start_date"):
            rec["start_date"] = al_rec["start_date"]
        elif s_rec.get("start_date"):
            rec["start_date"] = s_rec["start_date"]

        if al_rec.get("genres_csv"):
            rec["genres"] = al_rec["genres_csv"]
        elif sm_rec.get("genres_csv"):
            rec["genres"] = sm_rec["genres_csv"]

        if al_rec.get("studios_csv"):
            rec["studios"] = al_rec["studios_csv"]
        elif sm_rec.get("studios_csv"):
            rec["studios"] = sm_rec["studios_csv"]

        if not rec.get("mal_picture") and (al_rec.get("cover_image_large") or al_rec.get("cover_image_medium")):
            rec["mal_picture"] = al_rec.get("cover_image_large") or al_rec.get("cover_image_medium")

        # Titles & English Title
        title_main, _, _ = _pick_consensus([
            ("seed", 4.5, s_rec.get("mal_title") if s_rec.get("mal_title") and not str(s_rec.get("mal_title")).startswith("Anime #") else None),
            ("anilist_romaji", 4.0, al_rec.get("title_romaji")),
            ("anidb_main", 3.5, adb_t.get("main_title")),
            ("animelists_name", 3.0, xl_rec.get("name")),
            ("anilist_pref", 2.5, al_rec.get("title_user_preferred")),
            ("anilist_en", 2.0, al_rec.get("title_english")),
        ])
        if title_main:
            rec["mal_title"] = title_main

        title_en, en_src, _ = _pick_consensus([
            ("anilist_en", 5.0, al_rec.get("title_english")),
            ("anidb_official_en", 4.5, adb_t.get("official_en")),
            ("simkl_en", 3.5, sm_rec.get("title_en")),
            ("seed_en", 2.0, s_rec.get("title_english")),
        ])
        if title_en:
            rec["title_english"] = title_en
            provenance[mid]["title_english"] = en_src

        # Populate aliases_map from AniList, AniDB, Anime-Lists XML, and canonical titles
        if rec.get("mal_title"):
            bm._add_alias(aliases_map, mid, rec["mal_title"], "canonical")
        if rec.get("title_english"):
            bm._add_alias(aliases_map, mid, rec["title_english"], "english")
        for t_field, t_type in [("title_romaji", "romaji"), ("title_user_preferred", "synonym")]:
            if al_rec.get(t_field):
                bm._add_alias(aliases_map, mid, al_rec[t_field], t_type)
        if al_rec.get("synonyms_json"):
            for syn in json.loads(al_rec["synonyms_json"]):
                bm._add_alias(aliases_map, mid, syn, "synonym")
        if adb_t.get("main_title"):
            bm._add_alias(aliases_map, mid, adb_t["main_title"], "romaji")
        if adb_t.get("official_en"):
            bm._add_alias(aliases_map, mid, adb_t["official_en"], "english")
        if adb_t.get("synonyms_json"):
            for syn in json.loads(adb_t["synonyms_json"]):
                bm._add_alias(aliases_map, mid, syn, "synonym")
        if xl_rec.get("name"):
            bm._add_alias(aliases_map, mid, xl_rec["name"], "romaji")

        # Correlate thetvdb_id (never assign series TVDB ID to MOVIE records)
        is_movie = rec.get("anime_media_type") == "MOVIE"
        if is_movie:
            rec["thetvdb_id"] = None
        else:
            tvdb_val, tvdb_src, tvdb_conf = _pick_consensus([
                ("anibridge_v3", 5.0, ab_rec.get("tvdb_show_id")),
                ("animelists_xml", 4.5, xl_rec.get("thetvdb_id")),
                ("kometa", 4.0, k_rec.get("thetvdb_id") or k_multi.get("thetvdb_id")),
                ("fribb", 3.5, f_rec.get("thetvdb_id")),
                ("anizip", 3.0, az_rec.get("thetvdb_id")),
                ("wikidata", 2.5, w_rec.get("thetvdb_id")),
                ("simkl", 2.0, sm_rec.get("thetvdb_id")),
                ("trakt", 2.0, tr_rec.get("thetvdb_id")),
                ("seed", 1.5, s_rec.get("thetvdb_id")),
            ])
            rec["thetvdb_id"] = tvdb_val
            if tvdb_src:
                provenance[mid]["thetvdb_id"] = tvdb_src
            if tvdb_conf:
                conflict_counters["thetvdb_id"] += 1

        # Correlate themoviedb_id (movie ID for MOVIE, show/movie fallback for others)
        ab_tmdb = ab_rec.get("tmdb_movie_id") if is_movie else (ab_rec.get("tmdb_show_id") or ab_rec.get("tmdb_movie_id"))
        w_tmdb = w_rec.get("tmdb_movie_id") if is_movie else (w_rec.get("tmdb_tv_id") or w_rec.get("tmdb_movie_id"))
        tmdb_val, tmdb_src, tmdb_conf = _pick_consensus([
            ("anibridge_v3", 5.0, ab_tmdb),
            ("animelists_xml", 4.5, xl_rec.get("themoviedb_id")),
            ("fribb", 4.0, f_rec.get("themoviedb_id")),
            ("wikidata", 3.5, w_tmdb),
            ("anizip", 3.0, az_rec.get("themoviedb_id")),
            ("simkl", 2.5, sm_rec.get("themoviedb_id")),
            ("trakt", 2.0, tr_rec.get("themoviedb_id")),
            ("seed", 1.5, s_rec.get("themoviedb_id")),
        ])
        rec["themoviedb_id"] = tmdb_val
        if tmdb_src:
            provenance[mid]["themoviedb_id"] = tmdb_src
        if tmdb_conf:
            conflict_counters["themoviedb_id"] += 1

        # Correlate imdb_id
        imdb_val, imdb_src, imdb_conf = _pick_consensus([
            ("anibridge_v3", 5.0, ab_rec.get("imdb_movie_id")),
            ("animelists_xml", 4.5, xl_rec.get("imdb_id")),
            ("kometa", 4.0, k_rec.get("imdb_id") or k_multi.get("imdb_id")),
            ("fribb", 3.5, f_rec.get("imdb_id")),
            ("wikidata", 3.5, w_rec.get("imdb_id")),
            ("anizip", 3.0, az_rec.get("imdb_id")),
            ("simkl", 2.5, sm_rec.get("imdb_id")),
            ("trakt", 2.0, tr_rec.get("imdb_id")),
            ("seed", 1.5, s_rec.get("imdb_id")),
        ])
        rec["imdb_id"] = imdb_val
        if imdb_src:
            provenance[mid]["imdb_id"] = imdb_src
        if imdb_conf:
            conflict_counters["imdb_id"] += 1

        # Correlate trakt_id & trakt_slug
        if tr_rec.get("trakt_id"):
            rec["trakt_id"] = tr_rec["trakt_id"]
            if tr_rec.get("trakt_slug"):
                rec["trakt_slug"] = tr_rec["trakt_slug"]

        def _s_int(v):
            if v is None or isinstance(v, bool):
                return None
            if isinstance(v, int):
                return v if v >= 0 else None
            s = str(v).strip()
            return int(s) if s.isdigit() else None

        # Correlate thetvdb_season & episode offset
        if xl_rec.get("default_tvdb_season_raw") == "a":
            rec["thetvdb_season"] = "a"
            provenance[mid]["thetvdb_season"] = "animelists_tvdb_absolute"
        else:
            season_num, season_src, _ = _pick_consensus([
                ("anibridge_v3_tvdb", 5.0, _s_int(ab_rec.get("tvdb_season"))),
                ("animelists_tvdb", 4.5, _s_int(xl_rec.get("default_tvdb_season"))),
                ("kometa_tvdb", 4.0, _s_int(k_rec.get("tvdb_season") if k_rec.get("tvdb_season") is not None else k_multi.get("tvdb_season"))),
                ("fribb_tvdb", 3.5, _s_int(f_rec.get("season_tvdb"))),
                ("anibridge_v3_tmdb", 3.0, _s_int(ab_rec.get("tmdb_season"))),
                ("animelists_tmdb", 2.5, _s_int(xl_rec.get("tmdb_season"))),
                ("fribb_tmdb", 2.0, _s_int(f_rec.get("season_tmdb"))),
                ("seed", 1.5, _s_int(s_rec.get("thetvdb_season"))),
            ])
            if season_num is not None and season_num >= 0:
                rec["thetvdb_season"] = season_num
                provenance[mid]["thetvdb_season"] = season_src

        # Apply positive episode offset to global_media_episodes
        k_off = k_rec.get("tvdb_epoffset") or k_multi.get("tvdb_epoffset") or 0
        ep_off = (
            (xl_rec.get("episode_offset") if xl_rec.get("episode_offset") and xl_rec["episode_offset"] > 0 else None)
            or (k_off if k_off > 0 else None)
            or (f_rec.get("episode_offset") if f_rec.get("episode_offset") and f_rec["episode_offset"] > 0 else None)
            or (xl_rec.get("tmdb_offset") if xl_rec.get("tmdb_offset") and xl_rec["tmdb_offset"] > 0 else None)
            or 0
        )
        if ep_off > 0 and ep_counts.get(mid):
            cnt = ep_counts[mid]
            rec["anime_media_episodes"] = f"1 - {cnt}"
            rec["global_media_episodes"] = f"{1 + ep_off} - {cnt + ep_off}"
            rec["thetvdb_part"] = 2
        elif ab_rec.get("tvdb_part"):
            rec["thetvdb_part"] = ab_rec["tvdb_part"]

        # Deterministic 100% global_media_type
        am_type = rec.get("anime_media_type") or "TV"
        if am_type == "MOVIE":
            rec["global_media_type"] = "movie"
        elif am_type in ("TV", "ONA", "TV_SHORT"):
            rec["global_media_type"] = "show"
        else:
            # OVA, SPECIAL, MUSIC
            if rec.get("thetvdb_id") or (rec.get("episodes") or 0) > 1:
                rec["global_media_type"] = "show"
            elif rec.get("themoviedb_id") and not rec.get("thetvdb_id") and (rec.get("episodes") or 1) == 1:
                rec["global_media_type"] = "movie"
            else:
                rec["global_media_type"] = "show"

    # 3. Build Typed Franchise Graph & Prequel Inheritance from AniList Relations (seeding from seed_path if present)
    alid_to_mid = {r["anilist_id"]: mid for mid, r in records.items() if r.get("anilist_id")}
    franchise_links = {}
    prequel_edges = collections.defaultdict(set)
    if seed_path and os.path.isfile(seed_path):
        try:
            sc = _ro_conn(seed_path)
            stbls = {r[0] for r in sc.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "franchise_links" in stbls:
                for mid, rel_mid, rtype, src in sc.execute("SELECT mal_id, related_mal_id, relation_type, source FROM franchise_links"):
                    if rtype and rtype != "RELATED" and mid in records and rel_mid in records and mid != rel_mid:
                        franchise_links[(mid, rel_mid, rtype)] = src or "seed_anilist"
                        if rtype == "PREQUEL":
                            prequel_edges[mid].add(rel_mid)
                        elif rtype == "SEQUEL":
                            prequel_edges[rel_mid].add(mid)
            if "anime_aliases" in stbls:
                for mid, title, ttype in sc.execute("SELECT mal_id, title, title_type FROM anime_aliases"):
                    if mid in records and title:
                        bm._add_alias(aliases_map, mid, title, ttype or "synonym")
            sc.close()
        except Exception:
            pass
    for rel in anilist_relations:
        mid = rel.get("mal_id") or alid_to_mid.get(rel.get("anilist_id"))
        rel_mid = rel.get("related_mal_id") or alid_to_mid.get(rel.get("related_anilist_id"))
        rtype = rel.get("relation_type")
        if not mid or not rel_mid or mid == rel_mid or mid not in records or rel_mid not in records or not rtype:
            continue
        franchise_links[(mid, rel_mid, rtype)] = "anilist"
        if rtype == "PREQUEL":
            prequel_edges[mid].add(rel_mid)
            franchise_links.setdefault((rel_mid, mid, "SEQUEL"), "anilist_inv")
        elif rtype == "SEQUEL":
            prequel_edges[rel_mid].add(mid)
            franchise_links.setdefault((rel_mid, mid, "PREQUEL"), "anilist_inv")

    prequel_edges = {k: sorted(v) for k, v in prequel_edges.items()}
    bm.apply_prequel_inheritance(records, prequel_edges, {})
    bm.sanitize_trakt_collisions(records)
    pruned_shells = bm.finalize_parts_and_uniqueness(
        records,
        seed_ids=seed_ids,
        canonical_mal_ids=set(anilist_by_mal.keys()),
        ep_counts=ep_counts,
        aliases_map=aliases_map,
    )
    for mid in list(records.keys()):
        if not records[mid].get("mal_title"):
            del records[mid]
            pruned_shells += 1

    # Ensure 100% global_media_type post-finalization
    for mid, rec in records.items():
        if not rec.get("global_media_type"):
            rec["global_media_type"] = "movie" if rec.get("anime_media_type") == "MOVIE" else "show"

    # 4. Canonical tvdb_rules Unification (AniBridge v3 + Anime-Lists XML)
    all_rules = []
    for rl in anibridge_rules:
        mid = rl["mal_id"]
        if mid not in records:
            continue
        all_rules.append({
            "mal_id": mid,
            "tvdb_id": rl["tvdb_id"],
            "season": rl["season"],
            "part": rl["part"],
            "rule_type": rl["rule_type"],
            "range_start": rl["range_start"],
            "range_end": rl["range_end"],
            "ep_start": rl["ep_start"],
            "absolute_start": None,
            "source": "anibridge_v3",
        })

    # Add Anime-Lists XML rules in canonical (ep_start = local MAL start, range_start = target TVDB start) coordinates
    for mid, rec in records.items():
        aid = rec.get("anidb_id")
        if not aid or aid not in animelists_by_aid:
            continue
        xl = animelists_by_aid[aid]
        tvdb_id = xl.get("thetvdb_id") or rec.get("thetvdb_id")
        if not tvdb_id:
            continue
        ep_off = xl.get("episode_offset") or 0
        def_season = xl.get("default_tvdb_season")
        cnt = ep_counts.get(mid)
        if ep_off > 0 and def_season is not None and def_season >= 0:
            all_rules.append({
                "mal_id": mid,
                "tvdb_id": tvdb_id,
                "season": def_season,
                "part": 2,
                "rule_type": "episodeoffset",
                "range_start": 1 + ep_off,
                "range_end": (cnt + ep_off) if cnt else None,
                "ep_start": 1,
                "absolute_start": None,
                "source": "anime_lists_xml",
            })
        for m in animelists_mappings_by_aid.get(aid, []):
            a_season = m.get("anidb_season")
            t_season = m.get("tvdb_season") if m.get("tvdb_season") is not None else m.get("tmdb_season")
            if a_season not in (0, 1) or t_season is None or t_season < 0:
                continue
            m_start = m.get("start_ep")
            m_end = m.get("end_ep")
            m_off = m.get("offset_ep") or 0
            text = m.get("mapping_text") or ""
            pairs = []
            for tok in text.split(";"):
                tok = tok.strip()
                if "-" in tok:
                    a, b = tok.split("-", 1)
                    pa, pb = bm.parse_pos_int(a), bm.parse_pos_int(b)
                    if pa and pb:
                        pairs.append((pa, pb))
            if pairs:
                a_eps = [p[0] for p in pairs]
                t_eps = [p[1] for p in pairs]
                all_rules.append({
                    "mal_id": mid,
                    "tvdb_id": tvdb_id,
                    "season": t_season,
                    "part": 2 if min(t_eps) > 1 else 1,
                    "rule_type": "explicit",
                    "range_start": min(t_eps),
                    "range_end": max(t_eps),
                    "ep_start": min(a_eps),
                    "absolute_start": None,
                    "source": "anime_lists_xml",
                })
            elif m_start is not None:
                target_start = max(1, m_start + m_off)
                target_end = max(target_start, m_end + m_off) if m_end is not None else None
                all_rules.append({
                    "mal_id": mid,
                    "tvdb_id": tvdb_id,
                    "season": t_season,
                    "part": 2 if target_start > 1 else 1,
                    "rule_type": "mapping",
                    "range_start": target_start,
                    "range_end": target_end,
                    "ep_start": m_start,
                    "absolute_start": None,
                    "source": "anime_lists_xml",
                })

    # 5. Map Filler Ranges using full pre-trimmed aliases & complete SEQUEL chains
    cache = bm.ApiCache(".cache")
    filler_ranges, filler_shows = bm.hydrate_and_map_filler_ranges(
        records, aliases_map, franchise_links, cache, ep_counts, deadline=time.time() + 15.0
    )
    if not filler_ranges and seed_path and os.path.isfile(seed_path):
        try:
            sc = _ro_conn(seed_path)
            filler_ranges = [
                (r[0], r[1], r[2], r[3])
                for r in sc.execute("SELECT mal_id, ep_start, ep_end, filler_type FROM filler_ranges")
                if r[0] in records
            ]
            filler_shows = len({r[0] for r in filler_ranges})
            sc.close()
        except Exception:
            pass

    # Filter aliases to top 5 English/Romaji-prioritized alternates per show for lean mappings DB
    per_show_aliases = collections.defaultdict(list)
    primary_norms = {mid: bm._norm_alias(rec.get("mal_title")) for mid, rec in records.items()}
    for (mid, norm), alias_val in aliases_map.items():
        if mid not in records or norm == primary_norms.get(mid):
            continue
        clean_t, ttype = alias_val[0], alias_val[1]
        lrank = alias_val[2] if len(alias_val) > 2 else 2
        short_pen = 1 if len(clean_t) <= 3 else 0
        per_show_aliases[mid].append((bm._ALIAS_PRIORITY.get(ttype, 5), lrank, short_pen, len(clean_t), norm, clean_t, ttype))
    slim_aliases = {}
    for mid, items in per_show_aliases.items():
        items.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        for _, _, _, _, norm, clean_t, ttype in items[:5]:
            slim_aliases[(mid, norm)] = (clean_t, ttype)

    franchise_links = {k: v for k, v in franchise_links.items() if k[0] in records and k[1] in records and k[0] != k[1]}
    schedule_ids = {k: v for k, v in schedule_ids.items() if k in records and (v[0] or v[1])}
    filler_ranges = [r for r in filler_ranges if r[0] in records]

    stats = bm.compute_stats(records)
    bm.write_sqlite_database(
        output_mappings_path,
        records,
        all_rules,
        stats,
        aliases_map=slim_aliases,
        franchise_links=franchise_links,
        schedule_ids=schedule_ids,
        filler_ranges=filler_ranges,
        filler_shows=filler_shows,
    )

    # 6. Build Comprehensive Correlated dist/anime_metadata.db
    if output_metadata_path:
        _write_correlated_metadata_db(
            output_metadata_path, records, aliases_map, franchise_links, filler_ranges, provenance
        )

    # 7. Compress & Write Manifest + Correlation Report
    raw_size = os.path.getsize(output_mappings_path)
    raw_sha = bm.sha256_file(output_mappings_path)
    xz_size = None
    xz_sha = None
    if compress_xz:
        xz_path = f"{output_mappings_path}.xz"
        with open(output_mappings_path, "rb") as src, lzma.open(xz_path, "wb", preset=6) as dst:
            shutil.copyfileobj(src, dst, 65536)
        xz_size = os.path.getsize(xz_path)
        xz_sha = bm.sha256_file(xz_path)

    conn_check = _ro_conn(output_mappings_path)
    valid_rules_count = conn_check.execute("SELECT COUNT(*) FROM tvdb_rules").fetchone()[0]
    conn_check.close()

    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "schema_version": bm.SCHEMA_VERSION,
        "generated_at_utc": now_iso,
        "seed_rows": len(seed_records),
        "pruned_seed_shells": pruned_shells,
        "total_rows": len(records),
        "rule_count": valid_rules_count,
        "alias_count": len(slim_aliases),
        "franchise_link_count": len(franchise_links),
        "schedule_id_count": len(schedule_ids),
        "filler_range_count": len(filler_ranges),
        "filler_show_count": filler_shows,
        "build_seconds": round(time.perf_counter() - t0, 2),
        "files": {
            "raw": {"filename": os.path.basename(output_mappings_path), "size_bytes": raw_size, "sha256": raw_sha},
        },
        "coverage": stats["columns"],
    }
    if xz_size:
        manifest["files"]["xz"] = {
            "filename": f"{os.path.basename(output_mappings_path)}.xz",
            "size_bytes": xz_size,
            "sha256": xz_sha,
        }

    out_dir = os.path.dirname(os.path.abspath(output_mappings_path))
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)

    c = manifest["coverage"]
    xz_note = f"`anime_mappings.db.xz` (`{xz_size:,}` bytes) | " if xz_size else ""
    notes_text = (
        f"## Otaku Mapping Database ({now_iso})\n\n"
        f"- **Rows**: `{manifest['total_rows']:,}` (seed: `{manifest['seed_rows']:,}`) | "
        f"**TVDB/Cour Rules**: `{manifest['rule_count']:,}` | "
        f"**Aliases**: `{manifest['alias_count']:,}` | "
        f"**Franchise Links**: `{manifest['franchise_link_count']:,}` | "
        f"**Schedule IDs**: `{manifest['schedule_id_count']:,}` | "
        f"**Filler Ranges**: `{manifest['filler_range_count']:,}` (`{manifest['filler_show_count']:,}` shows)\n"
        f"- **Archives**: {xz_note}`anime_mappings.db` (`{raw_size:,}` bytes)\n"
        f"- **Coverage**: `anilist_id={c['anilist_id']:,}`, `kitsu_id={c['kitsu_id']:,}`, "
        f"`anidb_id={c['anidb_id']:,}`, `simkl_id={c['simkl_id']:,}`, `thetvdb_id={c['thetvdb_id']:,}`, "
        f"`themoviedb_id={c['themoviedb_id']:,}`, `imdb_id={c['imdb_id']:,}`, `trakt_id={c['trakt_id']:,}`, "
        f"`mal_dub_id={c['mal_dub_id']:,}`, `episodes={c.get('episodes', 0):,}`, `genres={c.get('genres', 0):,}`, "
        f"`studios={c.get('studios', 0):,}`, `year={c['year']:,}`, `anime_season={c['anime_season']:,}`, "
        f"`start_date={c.get('start_date', 0):,}`, `title_english={c.get('title_english', 0):,}`\n"
        f"- **Recent (`mal_id >= 55000`, n=`{c['recent_total']:,}`)**: "
        f"`thetvdb_id={c['recent_thetvdb_id']:,}`, `themoviedb_id={c['recent_themoviedb_id']:,}`, "
        f"`anidb_id={c['recent_anidb_id']:,}`, `trakt_id={c['recent_trakt_id']:,}`, "
        f"`start_date={c.get('recent_start_date', 0):,}`\n"
    )
    with open(os.path.join(out_dir, "RELEASE_NOTES.md"), "w", encoding="utf-8") as nf:
        nf.write(notes_text)

    report = {
        "generated_at_utc": now_iso,
        "active_sources_only": True,
        "retired_sources_excluded": [
            "manami-project/anime-offline-database (archived 2026-07-04)",
            "eliasbenb/PlexAniBridge-Mappings (archived 2026-04-05)",
            "MALSync/MAL-Sync-Backup GitHub raw (HTTP 451)",
        ],
        "service_database_row_counts": service_counts,
        "correlated_total_rows": len(records),
        "tvdb_rules_count": valid_rules_count,
        "typed_franchise_links_count": len(franchise_links),
        "full_aliases_count": len(aliases_map),
        "slim_aliases_count": len(slim_aliases),
        "conflicts_resolved_by_consensus": dict(conflict_counters),
        "coverage": stats["columns"],
    }
    if report_path:
        with open(report_path, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)

    print(
        f"[OK] Correlated {len(records):,} rows across {len(service_counts)} active service DBs -> "
        f"{output_mappings_path} ({raw_size:,} bytes, {valid_rules_count:,} rules, {len(franchise_links):,} typed franchise links) "
        f"in {manifest['build_seconds']}s"
    )
    return report


def _write_correlated_metadata_db(db_path, records, full_aliases_map, franchise_links, filler_ranges, provenance):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute(
        """
        CREATE TABLE anime_metadata (
            mal_id INTEGER PRIMARY KEY,
            mal_title TEXT NOT NULL,
            title_english TEXT,
            mal_picture TEXT,
            anime_media_type TEXT NOT NULL,
            global_media_type TEXT NOT NULL,
            status TEXT,
            episodes INTEGER,
            duration INTEGER,
            year INTEGER,
            anime_season TEXT,
            start_date TEXT,
            genres TEXT,
            studios TEXT,
            provenance_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE anime_aliases (
            mal_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            alias_norm TEXT NOT NULL,
            source_type TEXT NOT NULL,
            PRIMARY KEY (mal_id, alias_norm)
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
    meta_rows = []
    for mid, r in sorted(records.items()):
        meta_rows.append((
            mid,
            r.get("mal_title") or f"Anime #{mid}",
            r.get("title_english"),
            r.get("mal_picture"),
            r.get("anime_media_type") or "TV",
            r.get("global_media_type") or "show",
            r.get("status"),
            r.get("episodes"),
            r.get("duration"),
            r.get("year"),
            r.get("anime_season"),
            r.get("start_date"),
            r.get("genres"),
            r.get("studios"),
            json.dumps(provenance.get(mid) or {}, ensure_ascii=False),
        ))
    conn.executemany("INSERT INTO anime_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", meta_rows)
    alias_rows = [
        (mid, val[0], norm, val[1])
        for (mid, norm), val in sorted(full_aliases_map.items())
        if mid in records
    ]
    conn.executemany("INSERT OR IGNORE INTO anime_aliases VALUES (?,?,?,?)", alias_rows)
    conn.executemany("INSERT OR IGNORE INTO filler_ranges VALUES (?,?,?,?)", filler_ranges)
    conn.execute("CREATE INDEX idx_meta_year_season ON anime_metadata(year, anime_season)")
    conn.execute("CREATE INDEX idx_meta_aliases_norm ON anime_aliases(alias_norm)")
    conn.commit()
    conn.execute("ANALYZE")
    conn.execute("VACUUM")
    conn.close()
    print(f"[OK] Built comprehensive correlated metadata DB: {db_path} ({os.path.getsize(db_path):,} bytes, {len(alias_rows):,} aliases)")


def main():
    parser = argparse.ArgumentParser(description="Correlate per-service databases into anime_mappings.db and anime_metadata.db")
    parser.add_argument("--data-dir", default="data", help="Directory containing anime_metadata_<service>_<data>.db files")
    parser.add_argument("--seed", default="dist/anime_mappings.db", help="Seed DB for legacy baseline continuity")
    parser.add_argument("--output-mappings", default="dist/anime_mappings.db", help="Output lean anime_mappings.db path")
    parser.add_argument("--output-metadata", default="dist/anime_metadata.db", help="Output comprehensive anime_metadata.db path")
    parser.add_argument("--report", default="dist/correlation_report.json", help="Output correlation audit report JSON")
    args = parser.parse_args()
    correlate_all_services(
        args.data_dir,
        args.seed,
        args.output_mappings,
        args.output_metadata,
        args.report,
        compress_xz=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
