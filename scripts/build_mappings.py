#!/usr/bin/env python3
"""Otaku-Mapping-DB automated multi-source SQLite builder.

Merges open anime mapping datasets (Manami Anime Offline Database, kawaiioverflow ARM,
Fribb anime-lists, Anime-Lists XML, Kometa Anime-IDs, Wikidata SPARQL, AniDB official
titles XML, MAL-Dubs) with bounded cached live API hydration (AniList GraphQL +
Prequel/Sequel inheritance, AniZip, MALSync TVDB rules, Simkl) and seed database
preservation to build a compact, fully indexed anime_mappings.db plus .xz archive
and SHA-256 manifest.
"""

import argparse
import collections
import datetime
import gzip
import hashlib
import json
import lzma
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

SCHEMA_VERSION = "2.1"

SOURCE_URLS = {
    "fribb": "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-full.json",
    "manami": "https://github.com/manami-project/anime-offline-database/releases/latest/download/anime-offline-database-minified.json",
    "arm": "https://raw.githubusercontent.com/kawaiioverflow/arm/master/arm.json",
    "anime_lists": "https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-master.xml",
    "mal_dubs": "https://raw.githubusercontent.com/MAL-Dubs/MAL-Dubs/main/data/dubInfo.json",
    "kometa": "https://raw.githubusercontent.com/Kometa-Team/Anime-IDs/master/anime_ids.json",
    "anidb_titles": "https://anidb.net/api/anime-titles.xml.gz",
}

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
SIMKL_CLIENT_ID = os.environ.get(
    "SIMKL_CLIENT_ID",
    "59dfdc579d244e1edf6f89874d521d37a69a95a1abd349910cb056a1872ba2c8",
)
_EXTERNAL_ID_COLS = (
    "mal_dub_id",
    "anilist_id",
    "kitsu_id",
    "anidb_id",
    "simkl_id",
    "thetvdb_id",
    "themoviedb_id",
    "imdb_id",
    "trakt_id",
)

MAL_PICTURE_PREFIX = "https://cdn.myanimelist.net/images/anime/"
_RE_IMDB = re.compile(r"(tt\d{5,10})")
_RE_MAL_URL = re.compile(r"myanimelist\.net/anime/(\d+)")
_RE_AL_URL = re.compile(r"anilist\.co/anime/(\d+)")
_RE_KITSU_URL = re.compile(r"kitsu\.(?:app|io)/anime/(\d+)")
_RE_ANIDB_URL = re.compile(r"anidb\.net/anime/(\d+)")
_RE_SIMKL_URL = re.compile(r"simkl\.com/anime/(\d+)")
_RE_RANGE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")

REQUIRED_COLUMNS = (
    "mal_id",
    "mal_dub_id",
    "anilist_id",
    "kitsu_id",
    "anidb_id",
    "simkl_id",
    "thetvdb_id",
    "themoviedb_id",
    "imdb_id",
    "trakt_id",
    "mal_title",
    "mal_picture",
    "thetvdb_season",
    "thetvdb_part",
    "duration",
    "score",
    "status",
    "anime_media_type",
    "global_media_type",
    "anime_media_episodes",
    "global_media_episodes",
)

INDEXED_STATEMENTS = (
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_anidb ON "anime" (anidb_id ASC)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_anilist ON "anime" (anilist_id ASC)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_kitsu ON "anime" (kitsu_id ASC)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_mal ON "anime" (mal_id ASC)',
    'CREATE UNIQUE INDEX IF NOT EXISTS ix_simkl ON "anime" (simkl_id ASC)',
    'CREATE INDEX IF NOT EXISTS ix_tvdb ON "anime" (thetvdb_id) WHERE thetvdb_id IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_tmdb ON "anime" (themoviedb_id) WHERE themoviedb_id IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_imdb ON "anime" (imdb_id) WHERE imdb_id IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_trakt ON "anime" (trakt_id) WHERE trakt_id IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_mal_dub ON "anime" (mal_dub_id) WHERE mal_dub_id IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_mal_title ON "anime" (mal_title) WHERE mal_title IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_tvdb_rules_mal ON "tvdb_rules" (mal_id)',
    'CREATE INDEX IF NOT EXISTS ix_tvdb_rules_tvdb ON "tvdb_rules" (tvdb_id) WHERE tvdb_id IS NOT NULL',
)


def parse_pos_int(val):
    """Return strictly positive integer or None from int/numeric/comma-separated string."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val if val > 0 else None
    text = str(val).strip()
    if not text or text.lower() in ("none", "null", "unknown", "nan"):
        return None
    if "," in text:
        text = text.split(",", 1)[0].strip()
    if text.isdigit():
        num = int(text)
        return num if num > 0 else None
    return None


def parse_nonneg_int(val):
    """Return integer >= 0 (valid for season=0 specials or offsets) or None."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val if val >= 0 else None
    text = str(val).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    return None


def clean_imdb_id(val):
    """Extract canonical 'ttNNNNN' IMDb identifier or None from string/list."""
    if not val:
        return None
    if isinstance(val, (list, tuple)):
        for item in val:
            found = clean_imdb_id(item)
            if found:
                return found
        return None
    m = _RE_IMDB.search(str(val))
    return m.group(1) if m else None


def clean_season(val):
    """Normalize season value: integer >= 0 or 'a' for absolute numbering."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val if val >= 0 else None
    text = str(val).strip().lower()
    if text == "a":
        return "a"
    if text.isdigit():
        return int(text)
    return None


def normalize_picture(val):
    """Preserve upstream relative MAL image path convention ('12/12345.jpg') when possible."""
    if not val:
        return None
    text = str(val).strip()
    if not text or text.lower() in ("none", "null"):
        return None
    if text.startswith(MAL_PICTURE_PREFIX):
        return text[len(MAL_PICTURE_PREFIX):]
    if text.startswith("http://") or text.startswith("https://"):
        # Avoid storing full third-party URLs in mal_picture because AniListBrowser
        # prepends 'https://cdn.myanimelist.net/images/anime/' unconditionally.
        return None
    return text


def format_episode_range(start, count):
    """Format inclusive episode range string like '1 - 12' matching upstream convention."""
    if not count or count <= 0:
        return None
    s = max(1, int(start))
    return f"{s} - {s + int(count) - 1}"


def parse_range_tuple(val):
    """Parse '1 - 12' or '13-24' into (start, end) tuple or None."""
    m = _RE_RANGE.match(str(val or ""))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def empty_record(mal_id):
    return {col: None for col in REQUIRED_COLUMNS} | {"mal_id": int(mal_id)}


def _http_get_bytes(url, headers=None, timeout=30, retries=2):
    req_headers = {"User-Agent": "Otaku-Mapping-DB-Builder/2.1 (https://github.com/RPDevs-Builds/Otaku-Mapping-DB)"}
    if headers:
        req_headers.update(headers)
    last_exc = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read(), resp.status, dict(resp.headers)
        except urllib.error.HTTPError as exc:
            err_code = exc.code
            err_hdrs = dict(exc.headers or {})
            try:
                exc.close()
            except Exception:
                pass
            if err_code in (304, 404, 400):
                return b"", err_code, err_hdrs
            last_exc = exc
        except Exception as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_exc}")


def _http_post_json(url, payload, timeout=20, retries=2):
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "User-Agent": "Otaku-Mapping-DB-Builder/2.1 (https://github.com/RPDevs-Builds/Otaku-Mapping-DB)",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    last_exc = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            err_code = exc.code
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                exc.close()
            except Exception:
                pass
            if err_code == 429:
                wait_s = min(20.0, float(retry_after or 5.0))
                time.sleep(wait_s)
            last_exc = exc
        except Exception as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"POST {url} failed: {last_exc}")


class ApiCache:
    """Persistent JSON response cache under --cache-dir for incremental API hydration."""

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    def _path(self, namespace, key):
        safe_key = re.sub(r"[^A-Za-z0-9_.-]", "_", str(key))
        return os.path.join(self.cache_dir, f"{namespace}__{safe_key}.json")

    def get(self, namespace, key, max_age_seconds=7 * 86400):
        if not self.cache_dir:
            return None
        path = self._path(namespace, key)
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
            if time.time() - float(doc.get("fetched_at", 0)) > max_age_seconds:
                return None
            return doc.get("data")
        except Exception:
            return None

    def set(self, namespace, key, data):
        if not self.cache_dir:
            return
        path = self._path(namespace, key)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"fetched_at": time.time(), "data": data}, f)
        os.replace(tmp, path)


def load_source_blob(
    name,
    url,
    cache_dir=None,
    offline_dir=None,
    max_cache_age=6 * 3600,
    extra_headers=None,
    optional=False,
):
    """Fetch bulk source bytes from offline fixture dir, fresh local cache, or live URL."""
    ext = ".xml.gz" if name == "anidb_titles" else (".xml" if name == "anime_lists" else ".json")
    if offline_dir:
        for candidate in (f"{name}{ext}", f"{name}.json", f"{name}.xml", f"{name}.xml.gz"):
            p = os.path.join(offline_dir, candidate)
            if os.path.isfile(p):
                with open(p, "rb") as f:
                    return f.read()
        if optional:
            return None
        raise FileNotFoundError(f"Offline fixture not found for source '{name}' in {offline_dir}")

    meta_p = os.path.join(cache_dir, f"bulk_{name}.meta.json") if cache_dir else None
    blob_p = os.path.join(cache_dir, f"bulk_{name}{ext}") if cache_dir else None
    headers = dict(extra_headers or {})
    if cache_dir and os.path.isfile(blob_p) and os.path.isfile(meta_p):
        try:
            with open(meta_p, "r", encoding="utf-8") as mf:
                meta = json.load(mf)
            if time.time() - float(meta.get("fetched_at", 0)) < max_cache_age:
                with open(blob_p, "rb") as bf:
                    return bf.read()
            if meta.get("etag"):
                headers["If-None-Match"] = meta["etag"]
            if meta.get("last_modified"):
                headers["If-Modified-Since"] = meta["last_modified"]
        except Exception:
            pass

    try:
        raw, status, resp_headers = _http_get_bytes(url, headers=headers, timeout=60)
    except Exception as exc:
        if blob_p and os.path.isfile(blob_p):
            print(f"[WARN] Source '{name}' fetch error ({exc}); using cached copy.", file=sys.stderr)
            with open(blob_p, "rb") as bf:
                return bf.read()
        if optional:
            print(f"[WARN] Optional source '{name}' unavailable ({exc}); skipping.", file=sys.stderr)
            return None
        raise

    if status == 304 and blob_p and os.path.isfile(blob_p):
        with open(meta_p, "r+", encoding="utf-8") as mf:
            meta = json.load(mf)
            meta["fetched_at"] = time.time()
            mf.seek(0)
            json.dump(meta, mf)
            mf.truncate()
        with open(blob_p, "rb") as bf:
            return bf.read()
    if status != 200 or not raw:
        if blob_p and os.path.isfile(blob_p):
            print(f"[WARN] Source '{name}' returned HTTP {status}; using cached copy.", file=sys.stderr)
            with open(blob_p, "rb") as bf:
                return bf.read()
        if optional:
            print(f"[WARN] Optional source '{name}' returned HTTP {status}; skipping.", file=sys.stderr)
            return None
        raise RuntimeError(f"Source '{name}' failed with HTTP {status}")

    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        tmp_b = f"{blob_p}.tmp"
        with open(tmp_b, "wb") as bf:
            bf.write(raw)
        os.replace(tmp_b, blob_p)
        meta_out = {
            "fetched_at": time.time(),
            "etag": resp_headers.get("ETag") or resp_headers.get("Etag") or "",
            "last_modified": resp_headers.get("Last-Modified") or "",
        }
        with open(meta_p, "w", encoding="utf-8") as mf:
            json.dump(meta_out, mf)
    return raw


def load_seed_database(seed_path):
    """Read existing anime_mappings.db as baseline fallback records."""
    records = {}
    if not seed_path or not os.path.isfile(seed_path):
        return records
    conn = sqlite3.connect(f"file:{os.path.abspath(seed_path)}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(anime)")}
        for row in conn.execute("SELECT * FROM anime"):
            mid = parse_pos_int(row["mal_id"])
            if not mid:
                continue
            rec = empty_record(mid)
            for col in REQUIRED_COLUMNS:
                if col == "mal_id" or col not in cols:
                    continue
                val = row[col]
                if col == "imdb_id":
                    val = clean_imdb_id(val)
                elif col == "thetvdb_season":
                    val = clean_season(val)
                elif col in ("mal_dub_id", "anilist_id", "kitsu_id", "anidb_id", "simkl_id", "thetvdb_id", "themoviedb_id", "trakt_id", "thetvdb_part", "duration"):
                    val = parse_pos_int(val)
                elif isinstance(val, str) and not val.strip():
                    val = None
                rec[col] = val
            records[mid] = rec
    finally:
        conn.close()
    return records


def _extract_id(regex, sources):
    for s in sources or ():
        m = regex.search(str(s))
        if m:
            return parse_pos_int(m.group(1))
    return None


def merge_manami(records, manami_bytes):
    """Ingest Manami anime-offline-database JSON; return (related_mal_graph, ep_counts, manami_mal_ids)."""
    payload = json.loads(manami_bytes.decode("utf-8"))
    entries = payload.get("data", payload) if isinstance(payload, dict) else payload
    related_graph = collections.defaultdict(set)
    ep_counts = {}
    seen_mal_ids = set()

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        sources = entry.get("sources") or ()
        mal_id = _extract_id(_RE_MAL_URL, sources)
        if not mal_id:
            continue
        seen_mal_ids.add(mal_id)
        rec = records.setdefault(mal_id, empty_record(mal_id))

        for col, regex in (
            ("anilist_id", _RE_AL_URL),
            ("kitsu_id", _RE_KITSU_URL),
            ("anidb_id", _RE_ANIDB_URL),
            ("simkl_id", _RE_SIMKL_URL),
        ):
            parsed = _extract_id(regex, sources)
            if parsed:
                rec[col] = parsed

        title = (entry.get("title") or "").strip()
        if title:
            rec["mal_title"] = title

        pic = normalize_picture(entry.get("picture"))
        if pic:
            rec["mal_picture"] = pic

        mtype = (entry.get("type") or "").strip().upper()
        if mtype:
            rec["anime_media_type"] = mtype
            if not rec["global_media_type"]:
                if mtype == "MOVIE":
                    rec["global_media_type"] = "movie"
                elif mtype in ("TV", "ONA"):
                    rec["global_media_type"] = "show"

        status = (entry.get("status") or "").strip().upper()
        if status:
            rec["status"] = status

        dur = entry.get("duration")
        if isinstance(dur, dict):
            val = parse_pos_int(dur.get("value"))
            unit = str(dur.get("unit") or "").upper()
            if val:
                rec["duration"] = val * 60 if unit == "MINUTES" else val

        sc = entry.get("score")
        if isinstance(sc, dict):
            med = sc.get("median") or sc.get("arithmeticMean")
            if isinstance(med, (int, float)) and med > 0:
                rec["score"] = round(float(med), 2)

        eps = parse_pos_int(entry.get("episodes"))
        if eps:
            ep_counts[mal_id] = eps
            rec["anime_media_episodes"] = format_episode_range(1, eps)
            if not rec["global_media_episodes"]:
                rec["global_media_episodes"] = format_episode_range(1, eps)

        for rel_url in entry.get("relatedAnime") or ():
            m_rel = _RE_MAL_URL.search(str(rel_url))
            if m_rel:
                rel_id = parse_pos_int(m_rel.group(1))
                if rel_id and rel_id != mal_id:
                    related_graph[mal_id].add(rel_id)

    return related_graph, ep_counts, seen_mal_ids


def _parse_fribb_tmdb(raw_tmdb, media_type):
    if isinstance(raw_tmdb, dict):
        if str(media_type).upper() == "MOVIE" and raw_tmdb.get("movie"):
            return parse_pos_int(raw_tmdb.get("movie")), "movie"
        if raw_tmdb.get("tv"):
            return parse_pos_int(raw_tmdb.get("tv")), "show"
        if raw_tmdb.get("movie"):
            return parse_pos_int(raw_tmdb.get("movie")), "movie"
        for k, v in raw_tmdb.items():
            p = parse_pos_int(v)
            if p:
                return p, ("movie" if k == "movie" else "show")
        return None, None
    return parse_pos_int(raw_tmdb), None


def merge_fribb(records, fribb_bytes, ep_counts):
    """Ingest Fribb anime-list-full.json for MAL<->AniList/Kitsu/AniDB/Simkl/TVDB/TMDB/IMDb."""
    entries = json.loads(fribb_bytes.decode("utf-8"))
    by_anilist = {r["anilist_id"]: mid for mid, r in records.items() if r.get("anilist_id")}
    by_anidb = {r["anidb_id"]: mid for mid, r in records.items() if r.get("anidb_id")}
    offsets = {}

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        mid = parse_pos_int(entry.get("mal_id"))
        if not mid:
            al = parse_pos_int(entry.get("anilist_id"))
            adb = parse_pos_int(entry.get("anidb_id"))
            mid = by_anilist.get(al) or by_anidb.get(adb)
        if not mid:
            continue

        rec = records.setdefault(mid, empty_record(mid))
        if entry.get("type") and not rec["anime_media_type"]:
            rec["anime_media_type"] = str(entry["type"]).strip().upper()

        for src_k, col in (
            ("anilist_id", "anilist_id"),
            ("kitsu_id", "kitsu_id"),
            ("anidb_id", "anidb_id"),
            ("simkl_id", "simkl_id"),
            ("tvdb_id", "thetvdb_id"),
        ):
            v = parse_pos_int(entry.get(src_k))
            if v and not rec[col]:
                rec[col] = v
            elif src_k == "tvdb_id" and v:
                rec[col] = v

        tmdb_id, inferred_gtype = _parse_fribb_tmdb(entry.get("themoviedb_id"), rec["anime_media_type"])
        if tmdb_id:
            rec["themoviedb_id"] = tmdb_id
        if inferred_gtype and not rec["global_media_type"]:
            rec["global_media_type"] = inferred_gtype

        imdb = clean_imdb_id(entry.get("imdb_id"))
        if imdb:
            rec["imdb_id"] = imdb

        season_raw = entry.get("season")
        tvdb_season = None
        if isinstance(season_raw, dict):
            tvdb_season = clean_season(season_raw.get("tvdb"))
            if tvdb_season is None:
                tvdb_season = clean_season(season_raw.get("tmdb"))
        else:
            tvdb_season = clean_season(season_raw)
        if tvdb_season is not None and rec["thetvdb_season"] != "a":
            rec["thetvdb_season"] = tvdb_season

        ep_off = parse_nonneg_int(entry.get("episode_offset"))
        if ep_off is not None and ep_off > 0:
            offsets[mid] = ep_off
            if mid in ep_counts:
                rec["global_media_episodes"] = format_episode_range(1 + ep_off, ep_counts[mid])

    return offsets


def merge_anime_lists_xml(records, xml_bytes, ep_counts, offsets):
    """Ingest Anime-Lists XML keyed by anidbid and extract explicit <mapping-list> offset rules."""
    root = ET.fromstring(xml_bytes)
    by_anidb = collections.defaultdict(list)
    for mid, rec in records.items():
        if rec.get("anidb_id"):
            by_anidb[rec["anidb_id"]].append(mid)

    rules = []
    for el in root:
        anidb_id = parse_pos_int(el.attrib.get("anidbid"))
        if not anidb_id:
            continue
        mids = by_anidb.get(anidb_id, [])
        if not mids:
            continue

        tvdb_raw = (el.attrib.get("tvdbid") or "").strip()
        tvdb_id = parse_pos_int(tvdb_raw)
        default_season = clean_season(el.attrib.get("defaulttvdbseason"))
        ep_off = parse_nonneg_int(el.attrib.get("episodeoffset"))
        tmdb_tv = parse_pos_int(el.attrib.get("tmdbtv"))
        tmdb_movie = parse_pos_int(el.attrib.get("tmdbid"))
        imdb = clean_imdb_id(el.attrib.get("imdbid"))

        for mid in mids:
            rec = records[mid]
            if tvdb_id and not rec["thetvdb_id"]:
                rec["thetvdb_id"] = tvdb_id
            if not rec["themoviedb_id"]:
                if tmdb_tv:
                    rec["themoviedb_id"] = tmdb_tv
                    if not rec["global_media_type"]:
                        rec["global_media_type"] = "show"
                elif tmdb_movie:
                    rec["themoviedb_id"] = tmdb_movie
                    if not rec["global_media_type"]:
                        rec["global_media_type"] = "movie"
            if tvdb_raw.lower() == "movie" and not rec["global_media_type"]:
                rec["global_media_type"] = "movie"
            if imdb and not rec["imdb_id"]:
                rec["imdb_id"] = imdb

            if default_season == "a":
                rec["thetvdb_season"] = "a"
            elif default_season is not None and rec["thetvdb_season"] is None:
                rec["thetvdb_season"] = default_season

            if ep_off and ep_off > 0 and mid not in offsets:
                offsets[mid] = ep_off
                if mid in ep_counts:
                    rec["global_media_episodes"] = format_episode_range(1 + ep_off, ep_counts[mid])

            ml = el.find("mapping-list")
            if ml is not None:
                for m in ml.findall("mapping"):
                    t_season = parse_nonneg_int(m.attrib.get("tvdbseason"))
                    a_season = parse_nonneg_int(m.attrib.get("anidbseason"))
                    start = parse_pos_int(m.attrib.get("start"))
                    end = parse_pos_int(m.attrib.get("end"))
                    off_str = (m.attrib.get("offset") or "0").strip()
                    try:
                        m_off = int(off_str)
                    except ValueError:
                        m_off = 0
                    if t_season is not None and a_season == 1:
                        if start:
                            rules.append({
                                "mal_id": mid,
                                "tvdb_id": tvdb_id or rec["thetvdb_id"],
                                "season": t_season,
                                "rule_type": "anime_lists",
                                "range_start": start + m_off,
                                "range_end": (end + m_off) if end else None,
                                "ep_start": start,
                                "absolute_start": None,
                                "source": "anime_lists_xml",
                            })
                        elif m.text and ";" in m.text:
                            for pair in re.finditer(r"(\d+)\s*-\s*(\d+)", m.text):
                                a_ep, t_ep = int(pair.group(1)), int(pair.group(2))
                                rules.append({
                                    "mal_id": mid,
                                    "tvdb_id": tvdb_id or rec["thetvdb_id"],
                                    "season": t_season,
                                    "rule_type": "anime_lists_ep",
                                    "range_start": t_ep,
                                    "range_end": t_ep,
                                    "ep_start": a_ep,
                                    "absolute_start": None,
                                    "source": "anime_lists_xml",
                                })
    return rules


def merge_kometa(records, kometa_bytes, ep_counts, offsets):
    """Ingest Kometa anime_ids.json for any missing tvdb_id/tvdb_season/imdb_id/offset."""
    data = json.loads(kometa_bytes.decode("utf-8"))
    by_anidb = {r["anidb_id"]: mid for mid, r in records.items() if r.get("anidb_id")}

    for anidb_key, item in data.items():
        if not isinstance(item, dict):
            continue
        mid = parse_pos_int(item.get("mal_id")) or by_anidb.get(parse_pos_int(anidb_key))
        if not mid or mid not in records:
            continue
        rec = records[mid]
        tvdb_id = parse_pos_int(item.get("tvdb_id"))
        if tvdb_id and not rec["thetvdb_id"]:
            rec["thetvdb_id"] = tvdb_id
        imdb = clean_imdb_id(item.get("imdb_id"))
        if imdb and not rec["imdb_id"]:
            rec["imdb_id"] = imdb
        season = clean_season(item.get("tvdb_season"))
        if season is not None and rec["thetvdb_season"] is None:
            rec["thetvdb_season"] = season
        ep_off = parse_nonneg_int(item.get("tvdb_epoffset"))
        if ep_off and ep_off > 0 and mid not in offsets:
            offsets[mid] = ep_off
            if mid in ep_counts:
                rec["global_media_episodes"] = format_episode_range(1 + ep_off, ep_counts[mid])


def merge_arm(records, arm_bytes):
    """Backfill missing anilist_id on known MAL records from kawaiioverflow/arm."""
    if not arm_bytes:
        return 0
    entries = json.loads(arm_bytes.decode("utf-8"))
    filled = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        mid = parse_pos_int(entry.get("mal_id"))
        al_id = parse_pos_int(entry.get("anilist_id"))
        if mid and al_id and mid in records and not records[mid]["anilist_id"]:
            records[mid]["anilist_id"] = al_id
            filled += 1
    return filled


def merge_wikidata(records, wd_bytes):
    """Ingest Wikidata SPARQL results (P4086 MAL <-> P345 IMDb / P4835 TVDB / P4983-P4947 TMDB)."""
    if not wd_bytes:
        return 0
    payload = json.loads(wd_bytes.decode("utf-8"))
    bindings = ((payload or {}).get("results") or {}).get("bindings") or ()
    filled = 0
    for row in bindings:
        if not isinstance(row, dict):
            continue
        mid = parse_pos_int((row.get("mal") or {}).get("value"))
        if not mid or mid not in records:
            continue
        rec = records[mid]

        tvdb_id = parse_pos_int((row.get("tvdb") or {}).get("value"))
        if tvdb_id and not rec["thetvdb_id"]:
            rec["thetvdb_id"] = tvdb_id
            filled += 1

        tmdb_tv = parse_pos_int((row.get("tmdbTv") or {}).get("value"))
        tmdb_mv = parse_pos_int((row.get("tmdbMovie") or {}).get("value"))
        if tmdb_tv and not rec["themoviedb_id"] and rec.get("anime_media_type") != "MOVIE":
            rec["themoviedb_id"] = tmdb_tv
            if not rec["global_media_type"]:
                rec["global_media_type"] = "show"
            filled += 1
        elif tmdb_mv and not rec["themoviedb_id"]:
            rec["themoviedb_id"] = tmdb_mv
            if not rec["global_media_type"]:
                rec["global_media_type"] = "movie"
            filled += 1
        elif tmdb_tv and not rec["themoviedb_id"]:
            rec["themoviedb_id"] = tmdb_tv
            filled += 1

        imdb = clean_imdb_id((row.get("imdb") or {}).get("value"))
        if imdb and not rec["imdb_id"]:
            rec["imdb_id"] = imdb
            filled += 1
    return filled


def merge_anidb_titles(records, raw_bytes):
    """Backfill missing mal_title via AniDB official anime-titles.xml(.gz) dump."""
    if not raw_bytes:
        return 0
    if raw_bytes[:2] == b"\x1f\x8b":
        raw_bytes = gzip.decompress(raw_bytes)

    by_anidb = collections.defaultdict(list)
    for mid, rec in records.items():
        aid = rec.get("anidb_id")
        if aid and not rec.get("mal_title"):
            by_anidb[aid].append(mid)
    if not by_anidb:
        return 0

    xml_lang_key = "{http://www.w3.org/XML/1998/namespace}lang"
    root = ET.fromstring(raw_bytes.decode("utf-8"))
    filled = 0
    for anime_el in root.findall("anime"):
        aid = parse_pos_int(anime_el.attrib.get("aid"))
        if not aid or aid not in by_anidb:
            continue
        best_rank = 99
        best_title = None
        for t_el in anime_el.findall("title"):
            text = (t_el.text or "").strip()
            if not text:
                continue
            ttype = (t_el.attrib.get("type") or "").strip().lower()
            lang = (t_el.attrib.get(xml_lang_key) or "").strip().lower()
            if ttype == "main":
                rank = 0
            elif ttype == "official" and lang == "x-jat":
                rank = 1
            elif ttype == "official" and lang == "en":
                rank = 2
            elif ttype == "official":
                rank = 3
            elif ttype == "syn" and lang in ("x-jat", "en"):
                rank = 4
            else:
                continue
            if rank < best_rank:
                best_rank = rank
                best_title = text
        if best_title:
            for mid in by_anidb[aid]:
                if not records[mid]["mal_title"]:
                    records[mid]["mal_title"] = best_title
                    filled += 1
    return filled


def merge_mal_dubs(records, dubs_bytes):
    """Populate mal_dub_id from MAL-Dubs dubbed list."""
    payload = json.loads(dubs_bytes.decode("utf-8"))
    for raw_id in payload.get("dubbed", ()):
        mid = parse_pos_int(raw_id)
        if mid and mid in records:
            records[mid]["mal_dub_id"] = mid


def hydrate_anilist_relations(records, cache: ApiCache, ep_counts, offline_dir=None, deadline=0.0):
    """Batch query AniList GraphQL (50 MAL IDs/req) for missing metadata & PREQUEL relation graph."""
    prequel_edges = collections.defaultdict(list)
    if offline_dir:
        fix = os.path.join(offline_dir, "anilist_batch.json")
        if os.path.isfile(fix):
            with open(fix, "r", encoding="utf-8") as f:
                media_list = json.load(f)
            _apply_anilist_media(records, media_list, ep_counts, prequel_edges)
        return prequel_edges

    # Prioritize missing-title/status rows, recent IDs (>= 55000), and TV/ONA/OVA/MOVIE missing IDs
    candidates = [
        mid for mid, r in records.items()
        if (
            not r.get("mal_title")
            or not r.get("status")
            or (
                r.get("anime_media_type") in ("TV", "ONA", "OVA", "MOVIE", None)
                and (
                    r.get("status") in ("ONGOING", "UPCOMING")
                    or (mid >= 50000 and (not r.get("thetvdb_id") or not r.get("themoviedb_id")))
                )
            )
        )
    ]
    candidates.sort(
        key=lambda m: (
            0 if (m >= 55000 or not records[m].get("mal_title")) else 1,
            -m,
        )
    )

    query = """query ($ids: [Int]) {
  Page(perPage: 50) {
    media(idMal_in: $ids, type: ANIME) {
      id idMal status episodes duration format
      title { romaji userPreferred }
      relations { edges { relationType node { id idMal format } } }
    }
  }
}"""

    to_fetch = []
    for mid in candidates:
        ttl = 86400 if records[mid].get("status") in ("ONGOING", "UPCOMING", None) else 14 * 86400
        cached = cache.get("anilist", mid, max_age_seconds=ttl)
        if cached is not None:
            _apply_anilist_media(records, [cached], ep_counts, prequel_edges)
        else:
            to_fetch.append(mid)

    batch_size = 50
    for idx in range(0, min(len(to_fetch), 4000), batch_size):
        if deadline and time.time() >= deadline:
            break
        chunk = to_fetch[idx:idx + batch_size]
        try:
            resp = _http_post_json("https://graphql.anilist.co", {"query": query, "variables": {"ids": chunk}}, timeout=20)
            media = (((resp or {}).get("data") or {}).get("Page") or {}).get("media") or []
            seen_batch = set()
            for m in media:
                mid = parse_pos_int(m.get("idMal"))
                if mid:
                    seen_batch.add(mid)
                    cache.set("anilist", mid, m)
            for mid in chunk:
                if mid not in seen_batch:
                    cache.set("anilist", mid, {})
            _apply_anilist_media(records, media, ep_counts, prequel_edges)
            time.sleep(2.05)
        except Exception as exc:
            print(f"[WARN] AniList batch skipped: {exc}", file=sys.stderr)
            break

    return prequel_edges


def _apply_anilist_media(records, media_list, ep_counts, prequel_edges):
    status_map = {"RELEASING": "ONGOING", "NOT_YET_RELEASED": "UPCOMING", "FINISHED": "FINISHED", "CANCELLED": "FINISHED"}
    fmt_map = {"TV": "TV", "TV_SHORT": "TV", "ONA": "ONA", "OVA": "OVA", "MOVIE": "MOVIE", "SPECIAL": "SPECIAL", "MUSIC": "SPECIAL"}
    for m in media_list:
        if not isinstance(m, dict):
            continue
        mid = parse_pos_int(m.get("idMal"))
        if not mid or mid not in records:
            continue
        rec = records[mid]
        al_id = parse_pos_int(m.get("id"))
        if al_id and not rec["anilist_id"]:
            rec["anilist_id"] = al_id
        if not rec["mal_title"]:
            title_obj = m.get("title") or {}
            t = (title_obj.get("userPreferred") or title_obj.get("romaji") or "").strip()
            if t:
                rec["mal_title"] = t
        if not rec["status"] and m.get("status") in status_map:
            rec["status"] = status_map[m["status"]]
        fmt = fmt_map.get(str(m.get("format") or "").upper())
        if fmt and not rec["anime_media_type"]:
            rec["anime_media_type"] = fmt
            if not rec["global_media_type"]:
                if fmt == "MOVIE":
                    rec["global_media_type"] = "movie"
                elif fmt in ("TV", "ONA"):
                    rec["global_media_type"] = "show"
        if not rec["duration"]:
            dur_min = parse_pos_int(m.get("duration"))
            if dur_min:
                rec["duration"] = dur_min * 60
        eps = parse_pos_int(m.get("episodes"))
        if eps and mid not in ep_counts:
            ep_counts[mid] = eps
            rec["anime_media_episodes"] = format_episode_range(1, eps)
            if not rec["global_media_episodes"]:
                rec["global_media_episodes"] = format_episode_range(1, eps)

        for edge in ((m.get("relations") or {}).get("edges") or []):
            if edge.get("relationType") == "PREQUEL":
                node = edge.get("node") or {}
                pre_mal = parse_pos_int(node.get("idMal"))
                fmt = str(node.get("format") or "").upper()
                if pre_mal and fmt in ("TV", "ONA", "TV_SHORT"):
                    prequel_edges[mid].append(pre_mal)


def _fetch_simkl_record(mal_id, existing_simkl_id=None):
    """Fetch Simkl /anime/{id}?extended=full payload using existing simkl_id or /search/id?mal=."""
    if not SIMKL_CLIENT_ID:
        return {}
    simkl_id = parse_pos_int(existing_simkl_id)
    if not simkl_id:
        raw, status, _ = _http_get_bytes(
            f"https://api.simkl.com/search/id?mal={mal_id}&client_id={urllib.parse.quote(SIMKL_CLIENT_ID)}",
            timeout=12,
            retries=1,
        )
        if status == 200 and raw:
            arr = json.loads(raw.decode("utf-8"))
            if isinstance(arr, list):
                for item in arr:
                    if not isinstance(item, dict):
                        continue
                    ids = item.get("ids") or {}
                    item_mal = parse_pos_int(ids.get("mal"))
                    if item_mal is not None and item_mal != mal_id:
                        continue
                    cand_sk = parse_pos_int(ids.get("simkl") or ids.get("simkl_id"))
                    if cand_sk:
                        simkl_id = cand_sk
                        break
        time.sleep(0.15)
    if not simkl_id:
        return {}
    raw, status, _ = _http_get_bytes(
        f"https://api.simkl.com/anime/{simkl_id}?extended=full&client_id={urllib.parse.quote(SIMKL_CLIENT_ID)}",
        timeout=12,
        retries=1,
    )
    if status == 200 and raw:
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    return {}


def _apply_simkl(records, mal_id, sk, ep_counts):
    """Apply Simkl extended metadata carefully when Simkl MAL ID matches mal_id."""
    rec = records.get(mal_id)
    if not rec or not isinstance(sk, dict):
        return
    ids = sk.get("ids") if isinstance(sk.get("ids"), dict) else sk
    sk_mal = parse_pos_int(ids.get("mal"))
    if sk_mal is not None and sk_mal != mal_id:
        return

    for src_k, col in (
        ("simkl", "simkl_id"),
        ("simkl_id", "simkl_id"),
        ("anilist", "anilist_id"),
        ("kitsu", "kitsu_id"),
        ("anidb", "anidb_id"),
        ("tvdb", "thetvdb_id"),
        ("tmdb", "themoviedb_id"),
    ):
        val = parse_pos_int(ids.get(src_k))
        if val and not rec[col]:
            rec[col] = val

    imdb = clean_imdb_id(ids.get("imdb"))
    if imdb and not rec["imdb_id"]:
        rec["imdb_id"] = imdb

    if not rec["mal_title"]:
        title = (sk.get("title") or "").strip()
        if title:
            rec["mal_title"] = title

    tot_eps = parse_pos_int(sk.get("total_episodes"))
    if tot_eps and mal_id not in ep_counts:
        ep_counts[mal_id] = tot_eps
        rec["anime_media_episodes"] = format_episode_range(1, tot_eps)
        if not rec["global_media_episodes"]:
            rec["global_media_episodes"] = format_episode_range(1, tot_eps)


def hydrate_anizip_and_malsync(records, cache: ApiCache, ep_counts, offline_dir=None, max_items=350, deadline=0.0):
    """Hydrate missing TVDB/TMDB/IMDb/Simkl/season and cour rules via AniZip, MALSync, & Simkl."""
    extra_rules = []
    if offline_dir:
        az_fix = os.path.join(offline_dir, "anizip_items.json")
        if os.path.isfile(az_fix):
            with open(az_fix, "r", encoding="utf-8") as f:
                for mid_s, az_data in json.load(f).items():
                    _apply_anizip(records, int(mid_s), az_data, ep_counts)
        ms_fix = os.path.join(offline_dir, "malsync_rules.json")
        if os.path.isfile(ms_fix):
            with open(ms_fix, "r", encoding="utf-8") as f:
                for mid_s, ms_data in json.load(f).items():
                    extra_rules.extend(_apply_malsync_rules(records, int(mid_s), ms_data))
        sk_fix = os.path.join(offline_dir, "simkl_items.json")
        if os.path.isfile(sk_fix):
            with open(sk_fix, "r", encoding="utf-8") as f:
                for mid_s, sk_data in json.load(f).items():
                    _apply_simkl(records, int(mid_s), sk_data, ep_counts)
        return extra_rules

    priority_ids = [
        mid for mid, r in records.items()
        if (
            r.get("anime_media_type") in ("TV", "ONA", "MOVIE")
            and (
                not r.get("thetvdb_id")
                or not r.get("themoviedb_id")
                or not r.get("imdb_id")
                or not r.get("simkl_id")
                or r.get("status") in ("ONGOING", "UPCOMING")
            )
            and (mid >= 55000 or r.get("status") in ("ONGOING", "UPCOMING"))
        )
    ]
    priority_ids.sort(reverse=True)
    budget_ids = priority_ids[:max_items]

    for mid in budget_ids:
        if deadline and time.time() >= deadline:
            break
        rec = records[mid]
        ttl = 2 * 86400 if rec.get("status") in ("ONGOING", "UPCOMING") else 21 * 86400

        # 1. AniZip mappings
        az = cache.get("anizip", mid, max_age_seconds=ttl)
        if az is None:
            try:
                raw, status, _ = _http_get_bytes(f"https://api.ani.zip/mappings?mal_id={mid}", timeout=12, retries=1)
                az = json.loads(raw.decode("utf-8")) if (status == 200 and raw) else {}
                cache.set("anizip", mid, az)
                time.sleep(0.25)
            except Exception:
                az = {}
        if isinstance(az, dict) and az:
            _apply_anizip(records, mid, az, ep_counts)

        # 2. MALSync TVDB/cour rules
        if deadline and time.time() >= deadline:
            break
        if rec.get("anime_media_type") in ("TV", "ONA"):
            ms = cache.get("malsync_rules", mid, max_age_seconds=ttl)
            if ms is None:
                try:
                    raw, status, _ = _http_get_bytes(f"https://api.malsync.moe/tvdb/rules/cache-key/{mid}", timeout=12, retries=1)
                    ms = json.loads(raw.decode("utf-8")) if (status == 200 and raw) else {}
                    cache.set("malsync_rules", mid, ms)
                    time.sleep(0.2)
                except Exception:
                    ms = {}
            if isinstance(ms, dict) and ms:
                extra_rules.extend(_apply_malsync_rules(records, mid, ms))

        # 3. Simkl fallback when TVDB/TMDB/IMDb/Simkl ID still missing
        if deadline and time.time() >= deadline:
            break
        if SIMKL_CLIENT_ID and (
            not rec.get("thetvdb_id")
            or not rec.get("themoviedb_id")
            or not rec.get("imdb_id")
            or not rec.get("simkl_id")
        ):
            sk = cache.get("simkl", mid, max_age_seconds=ttl)
            if sk is None:
                try:
                    sk = _fetch_simkl_record(mid, rec.get("simkl_id"))
                    cache.set("simkl", mid, sk)
                    time.sleep(0.2)
                except Exception:
                    sk = {}
            if isinstance(sk, dict) and sk:
                _apply_simkl(records, mid, sk, ep_counts)

    return extra_rules


def _apply_anizip(records, mal_id, az, ep_counts):
    rec = records.get(mal_id)
    if not rec:
        return
    m = az.get("mappings") or {}
    for src_k, col in (
        ("anilist_id", "anilist_id"),
        ("kitsu_id", "kitsu_id"),
        ("anidb_id", "anidb_id"),
        ("thetvdb_id", "thetvdb_id"),
        ("themoviedb_id", "themoviedb_id"),
    ):
        v = parse_pos_int(m.get(src_k))
        if v and not rec[col]:
            rec[col] = v
    imdb = clean_imdb_id(m.get("imdb_id"))
    if imdb and not rec["imdb_id"]:
        rec["imdb_id"] = imdb

    ep_count = parse_pos_int(az.get("episodeCount"))
    if ep_count and mal_id not in ep_counts:
        ep_counts[mal_id] = ep_count
        rec["anime_media_episodes"] = format_episode_range(1, ep_count)
        if not rec["global_media_episodes"]:
            rec["global_media_episodes"] = format_episode_range(1, ep_count)

    episodes = az.get("episodes") or {}
    ep1 = episodes.get("1")
    if isinstance(ep1, dict):
        s_num = clean_season(ep1.get("seasonNumber"))
        if s_num is not None and rec["thetvdb_season"] is None:
            rec["thetvdb_season"] = s_num
        tvdb_show = parse_pos_int(ep1.get("tvdbShowId"))
        if tvdb_show and not rec["thetvdb_id"]:
            rec["thetvdb_id"] = tvdb_show


def _apply_malsync_rules(records, mal_id, ms):
    out_rules = []
    rec = records.get(mal_id)
    tvdb_id = parse_pos_int(ms.get("tvdbId"))
    if rec and tvdb_id and not rec["thetvdb_id"]:
        rec["thetvdb_id"] = tvdb_id
    for rule in ms.get("rules") or ():
        if not isinstance(rule, dict):
            continue
        if rule.get("provider") != "mal" or parse_pos_int(rule.get("id")) != mal_id:
            continue
        season = parse_nonneg_int(rule.get("season"))
        start = parse_pos_int(rule.get("start"))
        ep_start = parse_pos_int(rule.get("episodeStart")) or 1
        end = parse_pos_int(rule.get("end"))
        abs_start = parse_pos_int(rule.get("absoluteStart"))
        rtype = str(rule.get("type") or "season")
        if season is None or start is None:
            continue
        if rec and season > 0 and rec["thetvdb_season"] is None:
            rec["thetvdb_season"] = season
        out_rules.append({
            "mal_id": mal_id,
            "tvdb_id": tvdb_id,
            "season": season,
            "rule_type": rtype,
            "range_start": start,
            "range_end": end,
            "ep_start": ep_start,
            "absolute_start": abs_start,
            "source": "malsync_rules",
        })
    return out_rules


def apply_prequel_inheritance(records, prequel_edges, manami_related):
    """Conservatively propagate missing franchise IDs from verified direct TV prequels."""
    # Iterative shallow pass (up to 3 hops) handles S3 <- S2 <- S1
    for _ in range(3):
        changed = False
        for mid, rec in records.items():
            if rec.get("anime_media_type") not in ("TV", "ONA"):
                continue
            if rec.get("thetvdb_id") and rec.get("themoviedb_id"):
                continue
            pre_candidates = list(prequel_edges.get(mid) or [])
            if not pre_candidates and mid in manami_related:
                # Only use Manami relatedAnime fallback if candidate is an earlier TV series with tvdb_id
                pre_candidates = sorted(
                    rel for rel in manami_related[mid]
                    if rel < mid and rel in records and records[rel].get("anime_media_type") == "TV" and records[rel].get("thetvdb_id")
                )
            if not pre_candidates:
                continue
            # Require all prequel candidates with a TVDB ID to agree unambiguously on one tvdb_id
            tvdb_set = {records[p]["thetvdb_id"] for p in pre_candidates if p in records and records[p].get("thetvdb_id")}
            tmdb_set = {records[p]["themoviedb_id"] for p in pre_candidates if p in records and records[p].get("themoviedb_id")}
            imdb_set = {records[p]["imdb_id"] for p in pre_candidates if p in records and records[p].get("imdb_id")}

            if len(tvdb_set) == 1 and not rec["thetvdb_id"]:
                rec["thetvdb_id"] = next(iter(tvdb_set))
                changed = True
            if len(tmdb_set) == 1 and not rec["themoviedb_id"]:
                rec["themoviedb_id"] = next(iter(tmdb_set))
                changed = True
            if len(imdb_set) == 1 and not rec["imdb_id"]:
                rec["imdb_id"] = next(iter(imdb_set))
                changed = True

            # Only infer incremented season when there is a single explicit AniList PREQUEL with integer season > 0
            explicit_pres = prequel_edges.get(mid) or []
            if len(explicit_pres) == 1 and rec["thetvdb_season"] is None and explicit_pres[0] in records:
                p_season = records[explicit_pres[0]].get("thetvdb_season")
                if isinstance(p_season, int) and p_season >= 1:
                    rec["thetvdb_season"] = p_season + 1
                    changed = True
        if not changed:
            break


def finalize_parts_and_uniqueness(records, seed_ids=None, canonical_mal_ids=None):
    """Compute missing thetvdb_part ordinals, enforce SQLite UNIQUE cols, and prune empty/synthetic shells.

    Returns:
        int: Count of empty orphan or stale synthetic (>64000 non-Manami with zero external IDs)
            records pruned after uniqueness deduplication.
    """
    groups = collections.defaultdict(list)
    for mid, rec in records.items():
        if rec["thetvdb_id"] and rec["thetvdb_season"] is not None:
            groups[(rec["thetvdb_id"], rec["thetvdb_season"])].append(rec)

    for (tvdb_id, season), rows in groups.items():
        if season == 0:
            for r in rows:
                if not r["thetvdb_part"]:
                    r["thetvdb_part"] = 1
            continue
        def _sort_key(r):
            g = parse_range_tuple(r.get("global_media_episodes"))
            return (g[0] if g else 1, r["mal_id"])
        rows.sort(key=_sort_key)
        for idx, r in enumerate(rows, 1):
            if not r["thetvdb_part"] or season != "a":
                # Keep explicit valid part or assign sequential cour index
                if r["thetvdb_part"] is None:
                    r["thetvdb_part"] = idx

    # Enforce UNIQUE constraints deterministically across non-primary columns:
    # records sorted by mal_id ascending (older canonical entries keep unique claim)
    seen = {col: set() for col in ("anilist_id", "kitsu_id", "anidb_id", "simkl_id")}
    for mid in sorted(records.keys()):
        rec = records[mid]
        for col in seen:
            val = rec.get(col)
            if val is None:
                continue
            if val in seen[col]:
                rec[col] = None
            else:
                seen[col].add(val)

    # Prune empty orphan rows and stale synthetic (>64000 non-Manami) rows left with zero external IDs
    _ = seed_ids  # retained for explicit caller diagnostics
    pruned = 0
    for mid in list(records.keys()):
        rec = records[mid]
        if any(rec.get(c) is not None for c in _EXTERNAL_ID_COLS):
            continue
        is_empty_shell = rec.get("mal_title") is None and rec.get("status") is None
        is_stale_synthetic = mid > 64000 and (canonical_mal_ids is None or mid not in canonical_mal_ids)
        if is_empty_shell or is_stale_synthetic:
            del records[mid]
            pruned += 1
    return pruned


def write_sqlite_database(output_path, records, rules, stats):
    """Write normalized anime_mappings.db, build indexes, run ANALYZE + VACUUM."""
    out_dir = os.path.dirname(os.path.abspath(output_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    tmp_path = f"{output_path}.tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    conn = sqlite3.connect(tmp_path)
    try:
        conn.execute("PRAGMA page_size = 4096")
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = NORMAL")

        conn.execute("CREATE TABLE activities (sync_id INTEGER PRIMARY KEY, last_updated TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE anime ("
            "mal_id INTEGER PRIMARY KEY, mal_dub_id INTEGER, anilist_id INTEGER, "
            "kitsu_id INTEGER, anidb_id INTEGER, simkl_id INTEGER, thetvdb_id INTEGER, "
            "themoviedb_id INTEGER, imdb_id INTEGER, trakt_id INTEGER, mal_title TEXT, "
            "mal_picture TEXT, thetvdb_season INTEGER, thetvdb_part INTEGER, "
            "duration INTEGER, score REAL, status TEXT, anime_media_type TEXT, "
            "global_media_type TEXT, anime_media_episodes TEXT, global_media_episodes TEXT, "
            "UNIQUE(mal_id))"
        )
        conn.execute(
            "CREATE TABLE tvdb_rules ("
            "mal_id INTEGER NOT NULL, tvdb_id INTEGER, season INTEGER NOT NULL, "
            "rule_type TEXT NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER, "
            "ep_start INTEGER NOT NULL, absolute_start INTEGER, source TEXT NOT NULL, "
            "PRIMARY KEY (mal_id, season, rule_type, range_start, ep_start))"
        )
        conn.execute("CREATE TABLE build_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")

        placeholders = ", ".join("?" for _ in REQUIRED_COLUMNS)
        cols_sql = ", ".join(REQUIRED_COLUMNS)
        rows = [
            tuple(records[mid][col] for col in REQUIRED_COLUMNS)
            for mid in sorted(records.keys())
        ]
        conn.executemany(f"INSERT INTO anime ({cols_sql}) VALUES ({placeholders})", rows)

        conn.execute("INSERT INTO activities (sync_id, last_updated) VALUES (1, ?)", (str(int(time.time())),))

        dedup_rules = {}
        for rl in rules:
            key = (rl["mal_id"], rl["season"], rl["rule_type"], rl["range_start"], rl["ep_start"])
            if key not in dedup_rules:
                dedup_rules[key] = rl
        conn.executemany(
            "INSERT INTO tvdb_rules (mal_id, tvdb_id, season, rule_type, range_start, range_end, ep_start, absolute_start, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    rl["mal_id"], rl["tvdb_id"], rl["season"], rl["rule_type"],
                    rl["range_start"], rl["range_end"], rl["ep_start"], rl["absolute_start"], rl["source"]
                )
                for rl in dedup_rules.values()
            ],
        )

        meta_rows = [
            ("schema_version", SCHEMA_VERSION),
            ("built_at_utc", datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
            ("row_count", str(len(records))),
            ("rule_count", str(len(dedup_rules))),
            ("coverage_json", json.dumps(stats)),
        ]
        conn.executemany("INSERT INTO build_meta (key, value) VALUES (?, ?)", meta_rows)

        for stmt in INDEXED_STATEMENTS:
            conn.execute(stmt)

        conn.commit()
        conn.execute("ANALYZE")
        conn.commit()
        conn.execute("VACUUM")
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick != "ok":
            raise RuntimeError(f"PRAGMA quick_check failed: {quick}")
    finally:
        conn.close()

    os.replace(tmp_path, output_path)


def compute_stats(records):
    total = len(records)
    counts = {}
    for col in REQUIRED_COLUMNS:
        if col == "mal_id":
            counts[col] = total
            continue
        counts[col] = sum(1 for r in records.values() if r.get(col) not in (None, ""))
    recent = [r for mid, r in records.items() if mid >= 55000]
    counts["recent_total"] = len(recent)
    for col in ("anilist_id", "kitsu_id", "anidb_id", "simkl_id", "thetvdb_id", "themoviedb_id", "imdb_id", "mal_dub_id"):
        counts[f"recent_{col}"] = sum(1 for r in recent if r.get(col) not in (None, ""))
    return {"total_rows": total, "columns": counts}


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _build_wikidata_url():
    sparql = (
        "SELECT ?mal ?imdb ?tvdb ?tmdbTv ?tmdbMovie WHERE { "
        "?item wdt:P4086 ?mal . "
        "{ ?item wdt:P345 ?imdb . } UNION "
        "{ ?item wdt:P4835 ?tvdb . } UNION "
        "{ ?item wdt:P4983 ?tmdbTv . } UNION "
        "{ ?item wdt:P4947 ?tmdbMovie . } "
        "}"
    )
    return f"{WIKIDATA_SPARQL_URL}?{urllib.parse.urlencode({'query': sparql, 'format': 'json'})}"


def build_pipeline(args):
    t0 = time.perf_counter()
    cache = ApiCache(args.cache_dir)

    records = load_seed_database(args.seed)
    seed_ids = set(records.keys())
    seed_count = len(records)
    ep_counts = {}
    for mid, r in records.items():
        rg = parse_range_tuple(r.get("anime_media_episodes"))
        if rg:
            ep_counts[mid] = rg[1]

    manami_bytes = load_source_blob("manami", SOURCE_URLS["manami"], args.cache_dir, args.offline_fixtures)
    fribb_bytes = load_source_blob("fribb", SOURCE_URLS["fribb"], args.cache_dir, args.offline_fixtures)
    arm_bytes = load_source_blob("arm", SOURCE_URLS["arm"], args.cache_dir, args.offline_fixtures, optional=True)
    xml_bytes = load_source_blob("anime_lists", SOURCE_URLS["anime_lists"], args.cache_dir, args.offline_fixtures)
    kometa_bytes = load_source_blob("kometa", SOURCE_URLS["kometa"], args.cache_dir, args.offline_fixtures)
    dubs_bytes = load_source_blob("mal_dubs", SOURCE_URLS["mal_dubs"], args.cache_dir, args.offline_fixtures)
    anidb_titles_bytes = load_source_blob(
        "anidb_titles", SOURCE_URLS["anidb_titles"], args.cache_dir, args.offline_fixtures,
        max_cache_age=24 * 3600, optional=True,
    )
    wikidata_bytes = load_source_blob(
        "wikidata", _build_wikidata_url(), args.cache_dir, args.offline_fixtures,
        max_cache_age=24 * 3600,
        extra_headers={"Accept": "application/sparql-results+json"},
        optional=True,
    )

    manami_related, manami_eps, manami_mal_ids = merge_manami(records, manami_bytes)
    ep_counts.update(manami_eps)
    offsets = merge_fribb(records, fribb_bytes, ep_counts)
    merge_arm(records, arm_bytes)
    xml_rules = merge_anime_lists_xml(records, xml_bytes, ep_counts, offsets)
    merge_kometa(records, kometa_bytes, ep_counts, offsets)
    merge_wikidata(records, wikidata_bytes)
    merge_anidb_titles(records, anidb_titles_bytes)
    merge_mal_dubs(records, dubs_bytes)

    extra_rules = []
    prequel_edges = {}
    if args.api_budget_seconds > 0 or args.offline_fixtures:
        wall_deadline = time.time() + args.api_budget_seconds if not args.offline_fixtures else 0.0
        prequel_edges = hydrate_anilist_relations(records, cache, ep_counts, args.offline_fixtures, deadline=wall_deadline)
        extra_rules = hydrate_anizip_and_malsync(
            records, cache, ep_counts, args.offline_fixtures, max_items=args.max_api_items, deadline=wall_deadline
        )

    apply_prequel_inheritance(records, prequel_edges, manami_related)
    pruned_shells = finalize_parts_and_uniqueness(records, seed_ids=seed_ids, canonical_mal_ids=manami_mal_ids)

    all_rules = xml_rules + extra_rules
    stats = compute_stats(records)
    write_sqlite_database(args.output, records, all_rules, stats)

    raw_size = os.path.getsize(args.output)
    raw_sha = sha256_file(args.output)

    xz_path = None
    xz_size = None
    xz_sha = None
    if args.compress_xz:
        xz_path = f"{args.output}.xz"
        with open(args.output, "rb") as src, lzma.open(xz_path, "wb", preset=6) as dst:
            while True:
                buf = src.read(65536)
                if not buf:
                    break
                dst.write(buf)
        xz_size = os.path.getsize(xz_path)
        xz_sha = sha256_file(xz_path)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seed_rows": seed_count,
        "pruned_seed_shells": pruned_shells,
        "total_rows": len(records),
        "rule_count": len(all_rules),
        "build_seconds": round(time.perf_counter() - t0, 2),
        "files": {
            "raw": {
                "filename": os.path.basename(args.output),
                "size_bytes": raw_size,
                "sha256": raw_sha,
            },
        },
        "coverage": stats["columns"],
    }
    if xz_path:
        manifest["files"]["xz"] = {
            "filename": os.path.basename(xz_path),
            "size_bytes": xz_size,
            "sha256": xz_sha,
        }
    if args.manifest:
        mdir = os.path.dirname(os.path.abspath(args.manifest))
        if mdir:
            os.makedirs(mdir, exist_ok=True)
        with open(args.manifest, "w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2)

    notes_path = getattr(args, "release_notes", "")
    if notes_path:
        ndir = os.path.dirname(os.path.abspath(notes_path))
        if ndir:
            os.makedirs(ndir, exist_ok=True)
        c = manifest["coverage"]
        xz_note = f"`anime_mappings.db.xz` (`{xz_size:,}` bytes) | " if xz_size else ""
        notes_text = (
            f"## Otaku Mapping Database ({manifest['generated_at_utc']})\n\n"
            f"- **Rows**: `{manifest['total_rows']:,}` (seed: `{manifest['seed_rows']:,}`) | "
            f"**TVDB/Cour Rules**: `{manifest['rule_count']:,}`\n"
            f"- **Archives**: {xz_note}`anime_mappings.db` (`{raw_size:,}` bytes)\n"
            f"- **Coverage**: `anilist_id={c['anilist_id']:,}`, `kitsu_id={c['kitsu_id']:,}`, "
            f"`anidb_id={c['anidb_id']:,}`, `simkl_id={c['simkl_id']:,}`, `thetvdb_id={c['thetvdb_id']:,}`, "
            f"`themoviedb_id={c['themoviedb_id']:,}`, `imdb_id={c['imdb_id']:,}`, `mal_dub_id={c['mal_dub_id']:,}`\n"
            f"- **Recent (`mal_id >= 55000`, n=`{c['recent_total']:,}`)**: "
            f"`thetvdb_id={c['recent_thetvdb_id']:,}`, `themoviedb_id={c['recent_themoviedb_id']:,}`, "
            f"`anidb_id={c['recent_anidb_id']:,}`\n"
        )
        with open(notes_path, "w", encoding="utf-8") as nf:
            nf.write(notes_text)

    print(
        f"Built {args.output}: {len(records):,} rows ({len(records) - seed_count:+,} vs seed, "
        f"pruned {pruned_shells} empty shells), {len(all_rules):,} rules, {raw_size:,} bytes"
        + (f", xz={xz_size:,} bytes" if xz_size else "")
        + f" in {manifest['build_seconds']}s"
    )
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Build enriched Otaku anime_mappings.db")
    parser.add_argument("--seed", default="anime_mappings.db", help="Optional baseline SQLite DB to preserve legacy fields")
    parser.add_argument("--output", default="dist/anime_mappings.db", help="Output SQLite DB path")
    parser.add_argument("--compress-xz", action="store_true", help="Also produce <output>.xz")
    parser.add_argument("--manifest", default="dist/manifest.json", help="Output JSON build manifest path")
    parser.add_argument("--release-notes", default="", help="Optional markdown release notes output path")
    parser.add_argument("--cache-dir", default=".cache", help="Persistent cache dir for bulk & API downloads")
    parser.add_argument("--offline-fixtures", default="", help="Read deterministic JSON/XML fixtures from local dir")
    parser.add_argument("--api-budget-seconds", type=float, default=180.0, help="Max wall time for live API hydration")
    parser.add_argument("--max-api-items", type=int, default=350, help="Max delta shows hydrated per run")
    args = parser.parse_args()
    build_pipeline(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
