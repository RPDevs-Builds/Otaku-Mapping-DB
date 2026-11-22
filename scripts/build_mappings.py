#!/usr/bin/env python3
"""Otaku-Mapping-DB automated multi-source SQLite builder.

Merges open anime mapping datasets (Manami Anime Offline Database, kawaiioverflow ARM,
Fribb anime-lists, Anime-Lists XML, Kometa Anime-IDs, PlexAniBridge Mappings, Wikidata
SPARQL, AniDB official titles XML, MAL-Dubs) with bounded cached live API hydration
(AniList GraphQL + Prequel/Sequel inheritance, AniZip, MALSync TVDB rules, Simkl,
Trakt, AnimeFillerList) and seed database preservation to build a compact, fully indexed
anime_mappings.db plus .xz archive and SHA-256 manifest.
"""

import argparse
import collections
import concurrent.futures
import datetime
import gzip
import hashlib
import html as html_lib
import json
import lzma
import os
import re
import sqlite3
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

SCHEMA_VERSION = "2.5"

SOURCE_URLS = {
    "fribb": "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-full.json",
    "manami": "https://github.com/manami-project/anime-offline-database/releases/latest/download/anime-offline-database-minified.json",
    "arm": "https://raw.githubusercontent.com/kawaiioverflow/arm/master/arm.json",
    "anime_lists": "https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-master.xml",
    "mal_dubs": "https://raw.githubusercontent.com/MAL-Dubs/MAL-Dubs/main/data/dubInfo.json",
    "kometa": "https://raw.githubusercontent.com/Kometa-Team/Anime-IDs/master/anime_ids.json",
    "plexanibridge": "https://raw.githubusercontent.com/eliasbenb/PlexAniBridge-Mappings/v2/mappings.json",
    "anidb_titles": "https://anidb.net/api/anime-titles.xml.gz",
    "animefillerlist": "https://www.animefillerlist.com/shows",
}

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
SIMKL_CLIENT_ID = os.environ.get(
    "SIMKL_CLIENT_ID",
    "59dfdc579d244e1edf6f89874d521d37a69a95a1abd349910cb056a1872ba2c8",
)
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "").strip()
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
    "year",
    "anime_season",
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
    'CREATE INDEX IF NOT EXISTS ix_year ON "anime" (year) WHERE year IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_tvdb_rules_mal ON "tvdb_rules" (mal_id)',
    'CREATE INDEX IF NOT EXISTS ix_tvdb_rules_tvdb ON "tvdb_rules" (tvdb_id) WHERE tvdb_id IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ix_aliases_title ON "anime_aliases" (title)',
    'CREATE INDEX IF NOT EXISTS ix_franchise_rel ON "franchise_links" (related_mal_id)',
)

_RE_WS = re.compile(r"\s+")
_RE_LATIN = re.compile(r"[A-Za-z0-9]")
_RE_ALPHA = re.compile(r"[A-Za-z]")
_ALIAS_PRIORITY = {"main": 0, "official": 1, "english": 1, "romaji": 1, "syn": 2, "short": 3}


def _norm_alias(title):
    if not title:
        return ""
    return _RE_WS.sub(" ", str(title).lower()).strip()


def _alias_lang_rank(title_type, lang, clean_title=""):
    low_t = str(title_type or "").lower()
    low_l = str(lang or "").lower()
    if low_t in ("english", "romaji") or low_l in ("en", "x-jat"):
        return 0
    if low_t in ("main", "official"):
        return 1
    if low_l in ("ja", "und", ""):
        return 2 if all(ord(ch) < 128 for ch in clean_title) else 3
    return 3


def _add_alias(aliases_map, mal_id, title, source, lang="und", title_type="syn"):
    """Register deduplicated searchable Latin/English/Romaji alternate title for *mal_id*."""
    if aliases_map is None or not title:
        return
    clean = html_lib.unescape(str(title)).strip()
    if len(clean) < 2 or len(clean) > 120 or not _RE_LATIN.search(clean) or clean.isdigit():
        return
    if len(clean) <= 4 and not _RE_ALPHA.search(clean):
        return
    norm = _norm_alias(clean)
    if not norm:
        return
    key = (int(mal_id), norm)
    prio = _ALIAS_PRIORITY.get(title_type, 5)
    lrank = _alias_lang_rank(title_type, lang, clean)
    existing = aliases_map.get(key)
    if existing is None or (prio, lrank) < (
        _ALIAS_PRIORITY.get(existing[1], 5),
        existing[2] if len(existing) > 2 else 2,
    ):
        aliases_map[key] = (clean, title_type or "syn", lrank)


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


_VALID_ANIME_SEASONS = frozenset({"WINTER", "SPRING", "SUMMER", "FALL"})


def parse_year(val):
    """Return plausible release year integer (1900-2035) or None."""
    y = parse_pos_int(val)
    if y is not None and 1900 <= y <= 2035:
        return y
    return None


def clean_anime_season(val):
    """Normalize seasonal cour name ('WINTER', 'SPRING', 'SUMMER', 'FALL') or None."""
    if not val:
        return None
    s = str(val).strip().upper()
    return s if s in _VALID_ANIME_SEASONS else None


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
            if err_code == 429:
                retry_after = err_hdrs.get("Retry-After")
                try:
                    wait_s = min(5.0, max(0.5, float(retry_after or 2.0)))
                except (TypeError, ValueError):
                    wait_s = 2.0
                if attempt < retries:
                    time.sleep(wait_s)
                    continue
                return b"", 429, err_hdrs
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
    """Persistent thread-safe JSON response cache under --cache-dir for incremental API hydration."""

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self._lock = threading.Lock()
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
        tmp = f"{path}.{threading.get_ident()}.tmp"
        with self._lock:
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
                elif col == "year":
                    val = parse_year(val)
                elif col == "anime_season":
                    val = clean_anime_season(val)
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


def merge_manami(records, manami_bytes, aliases_map=None):
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
            _add_alias(aliases_map, mal_id, title, "manami", lang="x-jat", title_type="main")
        for syn in (entry.get("synonyms") or ())[:5]:
            _add_alias(aliases_map, mal_id, syn, "manami", lang="und", title_type="syn")

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

        aseas = entry.get("animeSeason")
        if isinstance(aseas, dict):
            yr = parse_year(aseas.get("year"))
            if yr:
                rec["year"] = yr
            seas = clean_anime_season(aseas.get("season"))
            if seas:
                rec["anime_season"] = seas

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

    def _apply_fribb_entry(entry, mid, allow_overwrite):
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
            if v and (not rec[col] or (allow_overwrite and src_k == "tvdb_id")):
                rec[col] = v

        tmdb_id, inferred_gtype = _parse_fribb_tmdb(entry.get("themoviedb_id"), rec["anime_media_type"])
        if tmdb_id and (allow_overwrite or not rec["themoviedb_id"]):
            rec["themoviedb_id"] = tmdb_id
        if inferred_gtype and not rec["global_media_type"]:
            rec["global_media_type"] = inferred_gtype

        imdb = clean_imdb_id(entry.get("imdb_id"))
        if imdb and (allow_overwrite or not rec["imdb_id"]):
            rec["imdb_id"] = imdb

        season_raw = entry.get("season")
        tvdb_season = None
        if isinstance(season_raw, dict):
            tvdb_season = clean_season(season_raw.get("tvdb"))
            if tvdb_season is None:
                tvdb_season = clean_season(season_raw.get("tmdb"))
        else:
            tvdb_season = clean_season(season_raw)
        if tvdb_season is not None and rec["thetvdb_season"] != "a" and (allow_overwrite or rec["thetvdb_season"] is None):
            rec["thetvdb_season"] = tvdb_season

        ep_off = parse_nonneg_int(entry.get("episode_offset"))
        if ep_off is not None and ep_off > 0 and (allow_overwrite or mid not in offsets):
            offsets[mid] = ep_off
            if mid in ep_counts:
                rec["global_media_episodes"] = format_episode_range(1 + ep_off, ep_counts[mid])

    # Apply explicit mal_id entries first; secondary AniList/AniDB fallbacks may only fill missing columns.
    fallback_entries = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        mid = parse_pos_int(entry.get("mal_id"))
        if mid:
            _apply_fribb_entry(entry, mid, allow_overwrite=True)
        else:
            fallback_entries.append(entry)

    for entry in fallback_entries:
        al = parse_pos_int(entry.get("anilist_id"))
        adb = parse_pos_int(entry.get("anidb_id"))
        mid = by_anilist.get(al) or by_anidb.get(adb)
        if mid:
            _apply_fribb_entry(entry, mid, allow_overwrite=False)

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
                                if a_ep < 1 or t_ep < 1:
                                    continue
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


def merge_arm(records, arm_bytes, schedule_ids=None):
    """Backfill missing anilist_id and collect syobocal/annict schedule IDs from kawaiioverflow/arm."""
    if not arm_bytes:
        return 0
    entries = json.loads(arm_bytes.decode("utf-8"))
    filled = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        mid = parse_pos_int(entry.get("mal_id"))
        if not mid or mid not in records:
            continue
        al_id = parse_pos_int(entry.get("anilist_id"))
        if al_id and not records[mid]["anilist_id"]:
            records[mid]["anilist_id"] = al_id
            filled += 1
        if schedule_ids is not None:
            syo = parse_pos_int(entry.get("syobocal_tid"))
            ann = parse_pos_int(entry.get("annict_id"))
            if syo or ann:
                prev_syo, prev_ann = schedule_ids.get(mid, (None, None))
                schedule_ids[mid] = (syo or prev_syo, ann or prev_ann)
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


_RE_PAB_SEASON = re.compile(r"^s(\d+)$", re.I)


def _as_int_list(val):
    if val is None or isinstance(val, bool):
        return []
    seq = val if isinstance(val, list) else (val,)
    out = []
    for x in seq:
        v = parse_pos_int(x)
        if v:
            out.append(v)
    return out


def merge_plexanibridge(records, pab_bytes):
    """Fill missing TVDB/TMDB/IMDb/season metadata from PlexAniBridge-Mappings (fill-missing only, type-safe)."""
    if not pab_bytes:
        return 0
    payload = json.loads(pab_bytes.decode("utf-8"))
    if isinstance(payload, list):
        entries = payload
    elif isinstance(payload, dict):
        entries = [v for k, v in payload.items() if not str(k).startswith("$") and isinstance(v, dict)]
    else:
        return 0

    filled = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        mids = _as_int_list(entry.get("mal_id"))
        if not mids:
            continue

        tvdb_vals = _as_int_list(entry.get("tvdb_id"))
        p_tvdb = tvdb_vals[0] if len(tvdb_vals) == 1 else None
        tmdb_tv_vals = _as_int_list(entry.get("tmdb_show_id"))
        p_tmdb_tv = tmdb_tv_vals[0] if len(tmdb_tv_vals) == 1 else None
        tmdb_mv_vals = _as_int_list(entry.get("tmdb_movie_id"))
        p_tmdb_mv = tmdb_mv_vals[0] if len(tmdb_mv_vals) == 1 else None

        raw_imdb = entry.get("imdb_id")
        if isinstance(raw_imdb, list):
            imdb_list = [clean_imdb_id(x) for x in raw_imdb if clean_imdb_id(x)]
        else:
            single_i = clean_imdb_id(raw_imdb)
            imdb_list = [single_i] if single_i else []
        p_imdb = imdb_list[0] if len(imdb_list) == 1 else None

        tmap = entry.get("tvdb_mappings")
        cand_season = None
        is_special_only_map = False
        if isinstance(tmap, dict) and tmap:
            pos_seasons = []
            has_s0 = False
            for k in tmap.keys():
                m_s = _RE_PAB_SEASON.match(str(k).strip())
                if m_s:
                    sn = int(m_s.group(1))
                    if sn >= 1:
                        pos_seasons.append(sn)
                    elif sn == 0:
                        has_s0 = True
            if len(pos_seasons) == 1 and not has_s0:
                cand_season = pos_seasons[0]
            if has_s0 and not pos_seasons:
                is_special_only_map = True

        for mid in mids:
            rec = records.get(mid)
            if not rec:
                continue
            mtype = str(rec.get("anime_media_type") or "").upper()

            if p_tvdb and not rec["thetvdb_id"] and (mtype != "MOVIE" or not is_special_only_map):
                rec["thetvdb_id"] = p_tvdb
                filled += 1

            if not rec["themoviedb_id"]:
                if mtype == "MOVIE":
                    if p_tmdb_mv:
                        rec["themoviedb_id"] = p_tmdb_mv
                        if not rec["global_media_type"]:
                            rec["global_media_type"] = "movie"
                        filled += 1
                elif mtype in ("TV", "ONA"):
                    if p_tmdb_tv:
                        rec["themoviedb_id"] = p_tmdb_tv
                        if not rec["global_media_type"]:
                            rec["global_media_type"] = "show"
                        filled += 1
                else:
                    if p_tmdb_mv:
                        rec["themoviedb_id"] = p_tmdb_mv
                        if not rec["global_media_type"]:
                            rec["global_media_type"] = "movie"
                        filled += 1
                    elif p_tmdb_tv and not is_special_only_map:
                        rec["themoviedb_id"] = p_tmdb_tv
                        if not rec["global_media_type"]:
                            rec["global_media_type"] = "show"
                        filled += 1

            if p_imdb and not rec["imdb_id"]:
                # Guard movies against inheriting a parent TV series IMDb via special s0-only mapping
                if mtype != "MOVIE" or p_tmdb_mv or (not is_special_only_map and len(mids) == 1):
                    rec["imdb_id"] = p_imdb
                    filled += 1

            if cand_season is not None and mtype in ("TV", "ONA") and rec["thetvdb_season"] is None:
                rec["thetvdb_season"] = cand_season
                filled += 1

    return filled


def merge_anidb_titles(records, raw_bytes, aliases_map=None):
    """Backfill missing mal_title and collect multi-language aliases via AniDB anime-titles.xml(.gz)."""
    if not raw_bytes:
        return 0
    if raw_bytes[:2] == b"\x1f\x8b":
        raw_bytes = gzip.decompress(raw_bytes)

    all_anidb = collections.defaultdict(list)
    for mid, rec in records.items():
        aid = rec.get("anidb_id")
        if aid and (not rec.get("mal_title") or aliases_map is not None):
            all_anidb[aid].append(mid)
    if not all_anidb:
        return 0

    xml_lang_key = "{http://www.w3.org/XML/1998/namespace}lang"
    root = ET.fromstring(raw_bytes.decode("utf-8"))
    filled = 0
    allowed_langs = ("x-jat", "en", "ja")
    for anime_el in root.findall("anime"):
        aid = parse_pos_int(anime_el.attrib.get("aid"))
        if not aid or aid not in all_anidb:
            continue
        best_rank = 99
        best_title = None
        alias_candidates = []
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
            elif ttype == "short" and lang in ("x-jat", "en"):
                rank = 5
            else:
                continue
            if rank < best_rank:
                best_rank = rank
                best_title = text
            if aliases_map is not None and lang in allowed_langs and ttype in ("main", "official", "syn", "short"):
                alias_candidates.append((rank, text, lang, ttype))

        mids = all_anidb[aid]
        if best_title:
            for mid in mids:
                if not records[mid]["mal_title"]:
                    records[mid]["mal_title"] = best_title
                    filled += 1
        if aliases_map is not None and alias_candidates:
            alias_candidates.sort(key=lambda x: x[0])
            for _, text, lang, ttype in alias_candidates[:6]:
                for mid in mids:
                    _add_alias(aliases_map, mid, text, "anidb", lang=lang, title_type=ttype)
    return filled


def merge_mal_dubs(records, dubs_bytes):
    """Populate mal_dub_id from MAL-Dubs dubbed list."""
    payload = json.loads(dubs_bytes.decode("utf-8"))
    for raw_id in payload.get("dubbed", ()):
        mid = parse_pos_int(raw_id)
        if mid and mid in records:
            records[mid]["mal_dub_id"] = mid


def hydrate_anilist_relations(records, cache: ApiCache, ep_counts, offline_dir=None, deadline=0.0, franchise_links=None, aliases_map=None):
    """Batch query AniList GraphQL (50 MAL IDs/req) for missing metadata & full relation graph."""
    prequel_edges = collections.defaultdict(list)
    if offline_dir:
        fix = os.path.join(offline_dir, "anilist_batch.json")
        if os.path.isfile(fix):
            with open(fix, "r", encoding="utf-8") as f:
                media_list = json.load(f)
            _apply_anilist_media(
                records, media_list, ep_counts, prequel_edges,
                franchise_links=franchise_links, aliases_map=aliases_map,
            )
        return prequel_edges

    # Prioritize missing-title/status rows, recent IDs (>= 55000), and TV/ONA/OVA/MOVIE missing IDs or years
    candidates = [
        mid for mid, r in records.items()
        if (
            not r.get("mal_title")
            or not r.get("status")
            or (mid >= 55000 and not r.get("year"))
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
            0 if not records[m].get("mal_title") else (1 if m >= 55000 else 2),
            -m,
        )
    )

    query = """query ($ids: [Int]) {
  Page(perPage: 50) {
    media(idMal_in: $ids, type: ANIME) {
      id idMal status episodes duration format season seasonYear
      startDate { year }
      title { romaji english userPreferred }
      synonyms
      relations { edges { relationType node { id idMal format } } }
    }
  }
}"""

    to_fetch = []
    for mid in candidates:
        ttl = 86400 if records[mid].get("status") in ("ONGOING", "UPCOMING", None) else 14 * 86400
        cached = cache.get("anilist", mid, max_age_seconds=ttl)
        if cached is not None:
            _apply_anilist_media(
                records, [cached], ep_counts, prequel_edges,
                franchise_links=franchise_links, aliases_map=aliases_map,
            )
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
            _apply_anilist_media(
                records, media, ep_counts, prequel_edges,
                franchise_links=franchise_links, aliases_map=aliases_map,
            )
            time.sleep(2.05)
        except Exception as exc:
            print(f"[WARN] AniList batch skipped: {exc}", file=sys.stderr)
            break

    # Secondary pass: query direct AniList `id_in` for untitled records that carry anilist_id (unlinked idMal)
    by_anilist = {r["anilist_id"]: mid for mid, r in records.items() if r.get("anilist_id") and not r.get("mal_title")}
    to_fetch_al = []
    for al_id, mid in sorted(by_anilist.items()):
        cached = cache.get("anilist_by_id", al_id, max_age_seconds=30 * 86400)
        if isinstance(cached, dict) and cached:
            patched = dict(cached)
            patched["idMal"] = mid
            _apply_anilist_media(
                records, [patched], ep_counts, prequel_edges,
                franchise_links=franchise_links, aliases_map=aliases_map,
            )
        elif cached is None:
            to_fetch_al.append((al_id, mid))

    id_query = """query ($ids: [Int]) {
  Page(perPage: 50) {
    media(id_in: $ids, type: ANIME) {
      id idMal status episodes duration format season seasonYear
      startDate { year }
      title { romaji english userPreferred }
      synonyms
      relations { edges { relationType node { id idMal format } } }
    }
  }
}"""
    for idx in range(0, min(len(to_fetch_al), 1500), batch_size):
        if deadline and time.time() >= deadline:
            break
        pair_chunk = to_fetch_al[idx:idx + batch_size]
        al_chunk = [al for al, _m in pair_chunk]
        try:
            resp = _http_post_json("https://graphql.anilist.co", {"query": id_query, "variables": {"ids": al_chunk}}, timeout=20)
            media = (((resp or {}).get("data") or {}).get("Page") or {}).get("media") or []
            seen_al = set()
            for m in media:
                al = parse_pos_int(m.get("id"))
                if al:
                    seen_al.add(al)
                    cache.set("anilist_by_id", al, m)
                    if not parse_pos_int(m.get("idMal")) and al in by_anilist:
                        m["idMal"] = by_anilist[al]
            for al in al_chunk:
                if al not in seen_al:
                    cache.set("anilist_by_id", al, {})
            _apply_anilist_media(
                records, media, ep_counts, prequel_edges,
                franchise_links=franchise_links, aliases_map=aliases_map,
            )
            time.sleep(2.05)
        except Exception as exc:
            print(f"[WARN] AniList id_in batch skipped: {exc}", file=sys.stderr)
            break

    return prequel_edges


_CAPTURED_RELATIONS = frozenset({"PREQUEL", "SEQUEL", "SIDE_STORY", "SPIN_OFF", "PARENT", "ALTERNATIVE", "SUMMARY"})


def _apply_anilist_media(records, media_list, ep_counts, prequel_edges, franchise_links=None, aliases_map=None):
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
        title_obj = m.get("title") or {}
        if not rec["mal_title"]:
            t = (title_obj.get("userPreferred") or title_obj.get("romaji") or title_obj.get("english") or "").strip()
            if t:
                rec["mal_title"] = t
        if aliases_map is not None:
            for tk, lang, ttype in (("romaji", "x-jat", "romaji"), ("english", "en", "english"), ("userPreferred", "und", "main")):
                _add_alias(aliases_map, mid, title_obj.get(tk), "anilist", lang=lang, title_type=ttype)
            for syn in (m.get("synonyms") or ())[:4]:
                _add_alias(aliases_map, mid, syn, "anilist", lang="und", title_type="syn")
        if not rec["status"] and m.get("status") in status_map:
            rec["status"] = status_map[m["status"]]
        if not rec["year"]:
            yr = parse_year(m.get("seasonYear") or ((m.get("startDate") or {}).get("year")))
            if yr:
                rec["year"] = yr
        if not rec["anime_season"]:
            seas = clean_anime_season(m.get("season"))
            if seas:
                rec["anime_season"] = seas
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
            rel_type = str(edge.get("relationType") or "").upper()
            node = edge.get("node") or {}
            rel_mal = parse_pos_int(node.get("idMal"))
            nfmt = str(node.get("format") or "").upper()
            if rel_type == "PREQUEL" and rel_mal and nfmt in ("TV", "ONA", "TV_SHORT"):
                prequel_edges[mid].append(rel_mal)
            if franchise_links is not None and rel_mal and rel_mal != mid and rel_type in _CAPTURED_RELATIONS:
                franchise_links[(mid, rel_mal, rel_type)] = "anilist"


def _media_kind(rec):
    if rec.get("anime_media_type") == "MOVIE" or rec.get("global_media_type") == "movie":
        return "movie"
    return "show"


_RE_TRAILING_YEAR = re.compile(r"\s*\(\d{4}\)\s*$")


def _norm_trakt_title(title):
    if not title:
        return ""
    s = unicodedata.normalize("NFKC", str(title)).lower()
    s = _RE_TRAILING_YEAR.sub(" ", s)
    return _RE_NON_ALNUM.sub(" ", s).strip()


def _trakt_lookup_key(rec):
    kind = _media_kind(rec)
    imdb = rec.get("imdb_id")
    if imdb:
        return ("imdb", imdb, kind)
    tmdb = rec.get("themoviedb_id")
    if tmdb:
        return ("tmdb", tmdb, kind)
    return None


def _match_trakt_title_year_candidate(arr, rec, kind, title_norms):
    """Select an unambiguous Trakt entry matching *title_norms*, +/-1 year, and non-conflicting IDs."""
    rec_year = parse_pos_int(rec.get("year"))
    if not rec_year or not title_norms or not isinstance(arr, list):
        return None
    exact_matches = []
    near_matches = []
    for entry in arr:
        if not isinstance(entry, dict) or kind not in entry:
            continue
        obj = entry.get(kind) or {}
        ids = obj.get("ids") or {}
        tid = parse_pos_int(ids.get("trakt"))
        if not tid:
            continue
        tyear = parse_pos_int(obj.get("year"))
        if not tyear or abs(tyear - rec_year) > 1:
            continue
        t_norm = _norm_trakt_title(obj.get("title"))
        if not t_norm or t_norm not in title_norms:
            continue
        c_tvdb = parse_pos_int(ids.get("tvdb"))
        c_tmdb = parse_pos_int(ids.get("tmdb"))
        c_imdb = clean_imdb_id(ids.get("imdb"))
        if rec.get("thetvdb_id") and c_tvdb and rec["thetvdb_id"] != c_tvdb:
            continue
        if rec.get("themoviedb_id") and c_tmdb and rec["themoviedb_id"] != c_tmdb:
            continue
        if rec.get("imdb_id") and c_imdb and rec["imdb_id"] != c_imdb:
            continue
        hit = {"trakt_id": tid, "thetvdb_id": c_tvdb, "themoviedb_id": c_tmdb, "imdb_id": c_imdb}
        if tyear == rec_year:
            exact_matches.append(hit)
        else:
            near_matches.append(hit)
    pool = exact_matches or near_matches
    distinct_tids = {h["trakt_id"] for h in pool}
    return pool[0] if len(distinct_tids) == 1 else None


def _apply_trakt_payload(rec, payload):
    """Apply resolved Trakt ID and backfill missing IMDb/TMDb/TVDB IDs safely."""
    if not rec or not isinstance(payload, dict):
        return False
    tid = parse_pos_int(payload.get("trakt_id"))
    if not tid or rec.get("trakt_id"):
        return False
    rec["trakt_id"] = tid
    if not rec.get("thetvdb_id") and parse_pos_int(payload.get("thetvdb_id")):
        rec["thetvdb_id"] = parse_pos_int(payload["thetvdb_id"])
    if not rec.get("themoviedb_id") and parse_pos_int(payload.get("themoviedb_id")):
        rec["themoviedb_id"] = parse_pos_int(payload["themoviedb_id"])
    c_imdb = clean_imdb_id(payload.get("imdb_id"))
    if not rec.get("imdb_id") and c_imdb:
        rec["imdb_id"] = c_imdb
    return True


def sanitize_trakt_collisions(records):
    """Clear same-franchise cross-type leaks, orphan seed leaks, and same-kind trakt_id collisions.

    Note: Trakt assigns independent auto-incrementing integer namespaces to `/shows/:id` vs
    `/movies/:id`, so two unrelated works (a MOVIE and a TV series with distinct IMDb/TMDb IDs)
    legitimately can share a numeric trakt_id and are NOT treated as collisions.
    """
    by_tid = collections.defaultdict(list)
    for mid, r in records.items():
        tid = parse_pos_int(r.get("trakt_id"))
        if tid:
            by_tid[tid].append((mid, r))

    cleared = 0
    for tid, members in by_tid.items():
        if len(members) <= 1:
            continue

        # 1. Clear ungrounded orphan entries (no tvdb/tmdb/imdb) sharing a trakt_id owned by grounded siblings
        grounded = [
            (m, r) for m, r in members
            if r.get("imdb_id") or r.get("themoviedb_id") or r.get("thetvdb_id")
        ]
        if grounded:
            for _m, r in members:
                if not (r.get("imdb_id") or r.get("themoviedb_id") or r.get("thetvdb_id")):
                    r["trakt_id"] = None
                    cleared += 1

        # 2. Clear cross-type leaks within the SAME series IMDb/TMDb (e.g. MOVIE inheriting TV show trakt_id)
        remaining = [(m, r) for m, r in members if parse_pos_int(r.get("trakt_id")) == tid]
        by_imdb_all = collections.defaultdict(list)
        for m, r in remaining:
            if r.get("imdb_id"):
                by_imdb_all[r["imdb_id"]].append((m, r))
        for _imdb, grp in by_imdb_all.items():
            mv_grp = [(m, r) for m, r in grp if r.get("anime_media_type") == "MOVIE" or r.get("global_media_type") == "movie"]
            tv_grp = [(m, r) for m, r in grp if r.get("anime_media_type") in ("TV", "ONA")]
            if mv_grp and tv_grp:
                # Series IMDb belongs to TV/ONA show; clear leaked show trakt_id from MOVIE entries
                for _m, r in mv_grp:
                    r["trakt_id"] = None
                    cleared += 1

        # 3. Within each Trakt namespace ('show' vs 'movie'), clear disconnected disjoint IMDb collisions
        for kind_filter in ("show", "movie"):
            kind_members = [
                (m, r) for m, r in members
                if parse_pos_int(r.get("trakt_id")) == tid and _media_kind(r) == kind_filter and r.get("imdb_id")
            ]
            by_imdb = collections.defaultdict(list)
            for m, r in kind_members:
                by_imdb[r["imdb_id"]].append((m, r))
            if len(by_imdb) > 1:
                ranked = sorted(by_imdb.items(), key=lambda kv: (-len(kv[1]), kv[0]))
                top_len = len(ranked[0][1])
                second_len = len(ranked[1][1])
                if top_len >= 2 and top_len > second_len:
                    for _imdb, grp in ranked[1:]:
                        for _m, r in grp:
                            r["trakt_id"] = None
                            cleared += 1
    return cleared


def propagate_trakt_sibling_ids(records):
    """Fill missing trakt_id from unambiguous sibling records sharing identical (imdb_id, kind) or (tmdb_id, kind)."""
    sanitize_trakt_collisions(records)
    imdb_tids = collections.defaultdict(set)
    tmdb_tids = collections.defaultdict(set)
    for r in records.values():
        tid = parse_pos_int(r.get("trakt_id"))
        if not tid:
            continue
        mt = _media_kind(r)
        if r.get("imdb_id"):
            imdb_tids[(r["imdb_id"], mt)].add(tid)
        if r.get("themoviedb_id"):
            tmdb_tids[(r["themoviedb_id"], mt)].add(tid)

    imdb_exact = {k: next(iter(v)) for k, v in imdb_tids.items() if len(v) == 1}
    tmdb_exact = {k: next(iter(v)) for k, v in tmdb_tids.items() if len(v) == 1}
    filled = 0
    for r in records.values():
        if parse_pos_int(r.get("trakt_id")):
            continue
        imdb = r.get("imdb_id")
        tmdb = r.get("themoviedb_id")
        mt = _media_kind(r)
        tid = imdb_exact.get((imdb, mt)) if imdb else None
        if not tid and tmdb:
            tid = tmdb_exact.get((tmdb, mt))
        if tid:
            r["trakt_id"] = tid
            filled += 1
    return filled


_HYDRATION_MEDIA_PRIORITY = {"TV": 0, "ONA": 1, "MOVIE": 2, "OVA": 3}
_HYDRATION_STATUS_PRIORITY = {"ONGOING": 0, "FINISHED": 1, "UPCOMING": 2}


def _hydration_priority_key(mid, rec):
    """Sort key prioritizing airing/finished TV & Movie titles over PVs/specials and unreleased shells."""
    mtype = str((rec or {}).get("anime_media_type") or "").upper()
    status = str((rec or {}).get("status") or "").upper()
    return (
        _HYDRATION_MEDIA_PRIORITY.get(mtype, 4),
        _HYDRATION_STATUS_PRIORITY.get(status, 3),
        -int(mid),
    )


def hydrate_trakt_ids(
    records,
    cache: ApiCache,
    offline_dir=None,
    max_items=100,
    deadline=0.0,
    client_id=None,
    delay_seconds=0.28,
    progress_every=0,
    on_batch=None,
    aliases_map=None,
):
    """Backfill missing trakt_id via sibling consensus, ApiCache, ID search, and title+year fallback."""
    if offline_dir:
        tk_fix = os.path.join(offline_dir, "trakt_items.json")
        if os.path.isfile(tk_fix):
            with open(tk_fix, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for mid_s, val in data.items():
                    mid = parse_pos_int(mid_s)
                    tid = parse_pos_int(val.get("trakt_id") if isinstance(val, dict) else val)
                    if mid and tid and mid in records and not records[mid]["trakt_id"]:
                        records[mid]["trakt_id"] = tid
        return {"sibling_filled": 0, "http_requests": 0, "resolved_http": 0}

    sibling_filled = propagate_trakt_sibling_ids(records)

    # Index normalized candidate titles for title+year fallback when IMDb/TMDb are absent
    titles_by_mid = collections.defaultdict(set)
    for mid, r in records.items():
        mt = _norm_trakt_title(r.get("mal_title"))
        if len(mt) >= 3:
            titles_by_mid[mid].add(mt)
    for (amid, _anorm), aval in (aliases_map or {}).items():
        if aval and len(aval) >= 2 and aval[1] != "short":
            at = _norm_trakt_title(aval[0])
            if len(at) >= 4:
                titles_by_mid[amid].add(at)

    id_candidates = []
    title_candidates = []
    for mid, r in records.items():
        if r.get("trakt_id"):
            continue
        if r.get("imdb_id") or r.get("themoviedb_id"):
            id_candidates.append(mid)
        elif (
            str(r.get("anime_media_type") or "").upper() in ("TV", "ONA", "MOVIE")
            and parse_pos_int(r.get("year"))
            and (r.get("year") or 0) >= 1960
            and len(_norm_trakt_title(r.get("mal_title"))) >= 3
        ):
            title_candidates.append(mid)
    id_candidates.sort(key=lambda mid: _hydration_priority_key(mid, records.get(mid)))
    title_candidates.sort(key=lambda mid: _hydration_priority_key(mid, records.get(mid)))

    # Pass 1: apply cached Trakt IDs & build in-run dedup memo (0 HTTP calls)
    to_fetch = []
    lookup_memo = {}
    for mid in id_candidates:
        tid = cache.get("trakt", mid, max_age_seconds=30 * 86400)
        key = _trakt_lookup_key(records[mid])
        if tid is None:
            to_fetch.append((mid, "id"))
        else:
            pos_tid = parse_pos_int(tid) or 0
            if key and key not in lookup_memo:
                lookup_memo[key] = pos_tid
            if pos_tid and not records[mid]["trakt_id"]:
                records[mid]["trakt_id"] = pos_tid

    for mid in title_candidates:
        cached = cache.get("trakt", f"title_{mid}", max_age_seconds=30 * 86400)
        if cached is None:
            to_fetch.append((mid, "title"))
        elif isinstance(cached, dict):
            _apply_trakt_payload(records[mid], cached)

    api_key = (client_id or TRAKT_CLIENT_ID or "").strip()
    if not api_key:
        sanitize_trakt_collisions(records)
        return {"sibling_filled": sibling_filled, "http_requests": 0, "resolved_http": 0}

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": api_key,
    }
    rate_limit_hits = 0
    http_requests = 0
    resolved_http = 0
    processed_unique = 0
    t_start = time.time()

    def _trakt_get_with_retry(req_url):
        nonlocal http_requests, rate_limit_hits
        http_requests += 1
        raw, status, resp_headers = _http_get_bytes(req_url, headers=headers, timeout=12, retries=1)
        if status == 429:
            rate_limit_hits += 1
            if rate_limit_hits >= 5:
                return None, 429
            try:
                retry_after = min(65.0, max(2.0, float((resp_headers or {}).get("Retry-After") or 10.0)))
            except (TypeError, ValueError):
                retry_after = 10.0
            time.sleep(retry_after)
            raw, status, _ = _http_get_bytes(req_url, headers=headers, timeout=12, retries=0)
        return raw, status

    for mid, mode in to_fetch:
        rec = records[mid]
        key = _trakt_lookup_key(rec) if mode == "id" else None
        if key and key in lookup_memo:
            tid = lookup_memo[key]
            cache.set("trakt", mid, tid)
            if tid and not rec["trakt_id"]:
                rec["trakt_id"] = int(tid)
                resolved_http += 1
            continue

        if max_items and processed_unique >= max_items:
            continue
        if deadline and time.time() >= deadline:
            continue

        processed_unique += 1
        tid = 0
        try:
            kind = _media_kind(rec)
            if mode == "id":
                imdb = rec.get("imdb_id")
                mtype_known = rec.get("anime_media_type") in ("TV", "ONA", "MOVIE") or rec.get("global_media_type") in ("show", "movie")
                q_type = kind if mtype_known else "show,movie"
                url = (
                    f"https://api.trakt.tv/search/imdb/{urllib.parse.quote(imdb)}?type={q_type}"
                    if imdb
                    else f"https://api.trakt.tv/search/tmdb/{rec['themoviedb_id']}?type={kind}"
                )
                raw, status = _trakt_get_with_retry(url)
                if status == 429:
                    break
                if status == 200 and raw:
                    arr = json.loads(raw.decode("utf-8"))
                    if isinstance(arr, list) and arr:
                        entry = next((e for e in arr if (kind in e)), arr[0])
                        obj = entry.get(kind) or entry.get("show") or entry.get("movie") or {}
                        tid = parse_pos_int(((obj.get("ids") or {}).get("trakt"))) or 0
                if status in (200, 404, 400):
                    cache.set("trakt", mid, tid)
                    if key:
                        lookup_memo[key] = tid
                if parse_pos_int(tid) and not rec["trakt_id"]:
                    rec["trakt_id"] = int(tid)
                    resolved_http += 1
            else:
                q_title = str(rec.get("mal_title") or "").strip()
                yr = parse_pos_int(rec.get("year"))
                url = (
                    f"https://api.trakt.tv/search/{kind}"
                    f"?query={urllib.parse.quote(q_title)}&years={max(1950, yr - 1)}-{yr + 1}"
                )
                raw, status = _trakt_get_with_retry(url)
                if status == 429:
                    break
                hit = None
                if status == 200 and raw:
                    arr = json.loads(raw.decode("utf-8"))
                    hit = _match_trakt_title_year_candidate(arr, rec, kind, titles_by_mid.get(mid))
                if status in (200, 404, 400):
                    cache.set("trakt", f"title_{mid}", hit if hit else 0)
                if hit and _apply_trakt_payload(rec, hit):
                    resolved_http += 1

            if delay_seconds > 0:
                time.sleep(delay_seconds)
        except Exception:
            if mode == "id":
                cache.set("trakt", mid, 0)
                if key:
                    lookup_memo[key] = 0
            else:
                cache.set("trakt", f"title_{mid}", 0)

        if on_batch and (processed_unique % 50 == 0):
            on_batch(records)
        if progress_every and (processed_unique % progress_every == 0):
            total_now = sum(1 for r in records.values() if r.get("trakt_id"))
            elapsed_s = int(time.time() - t_start)
            print(
                f"  [Trakt unique={processed_unique:,}] http={http_requests:,} +{resolved_http:,} "
                f"total_trakt={total_now:,} elapsed={elapsed_s}s",
                flush=True,
            )

    if on_batch:
        on_batch(records)
    sanitize_trakt_collisions(records)
    resolved_total = sum(1 for r in records.values() if r.get("trakt_id"))
    print(
        f"Trakt hydration: total={resolved_total:,} (siblings=+{sibling_filled:,}, "
        f"queue={len(to_fetch):,}, http={http_requests:,}, resolved=+{resolved_http:,})"
    )
    return {"sibling_filled": sibling_filled, "http_requests": http_requests, "resolved_http": resolved_http}


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
    by_anilist = {r["anilist_id"]: mid for mid, r in records.items() if r.get("anilist_id")}
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
                    extra_rules.extend(_apply_malsync_rules(records, int(mid_s), ms_data, by_anilist=by_anilist))
        sk_fix = os.path.join(offline_dir, "simkl_items.json")
        if os.path.isfile(sk_fix):
            with open(sk_fix, "r", encoding="utf-8") as f:
                for mid_s, sk_data in json.load(f).items():
                    _apply_simkl(records, int(mid_s), sk_data, ep_counts)
        return extra_rules

    tvdb_counts = collections.Counter(
        r.get("thetvdb_id") for r in records.values()
        if r.get("thetvdb_id") and r.get("anime_media_type") in ("TV", "ONA")
    )
    priority_ids = [
        mid for mid, r in records.items()
        if (
            r.get("anime_media_type") in ("TV", "ONA", "MOVIE")
            and (
                (
                    (
                        not r.get("thetvdb_id")
                        or not r.get("themoviedb_id")
                        or not r.get("imdb_id")
                        or not r.get("simkl_id")
                        or r.get("status") in ("ONGOING", "UPCOMING")
                    )
                    and (mid >= 55000 or r.get("status") in ("ONGOING", "UPCOMING"))
                )
                or (
                    r.get("anime_media_type") in ("TV", "ONA")
                    and (
                        (r.get("thetvdb_part") or 1) > 1
                        or (r.get("thetvdb_id") and tvdb_counts.get(r["thetvdb_id"], 0) >= 2 and mid >= 30000)
                    )
                )
            )
        )
    ]
    priority_ids.sort(key=lambda mid: _hydration_priority_key(mid, records.get(mid)))

    # Pass 1: apply already-cached AniZip/MALSync/Simkl payloads across all candidates first (0 HTTP calls),
    # queueing only items needing uncached live API fetches against max_items.
    uncached_queue = []
    malsync_queue = []
    seen_ms_tvdb = set()
    for mid in priority_ids:
        rec = records[mid]
        ttl = 2 * 86400 if rec.get("status") in ("ONGOING", "UPCOMING") else 21 * 86400
        need_az_check = (
            not rec.get("thetvdb_id") or not rec.get("themoviedb_id") or not rec.get("imdb_id")
            or rec.get("status") in ("ONGOING", "UPCOMING")
        ) and (mid >= 55000 or rec.get("status") in ("ONGOING", "UPCOMING"))
        need_az = {} if not need_az_check else cache.get("anizip", mid, max_age_seconds=ttl)
        if isinstance(need_az, dict) and need_az:
            _apply_anizip(records, mid, need_az, ep_counts)

        # Fetch MALSync once per shared thetvdb_id franchise (returns whole TVDB series cour rules)
        tid = rec.get("thetvdb_id")
        ms_eligible = (
            rec.get("anime_media_type") in ("TV", "ONA")
            and tid is not None
            and (tvdb_counts.get(tid, 0) >= 2 or (rec.get("thetvdb_part") or 1) > 1 or rec.get("status") == "ONGOING")
        )
        need_ms = {}
        if ms_eligible:
            need_ms = cache.get("malsync_rules", mid, max_age_seconds=30 * 86400)
            if isinstance(need_ms, dict) and need_ms:
                seen_ms_tvdb.add(tid)
                extra_rules.extend(_apply_malsync_rules(records, mid, need_ms, by_anilist=by_anilist))

        need_simkl_check = (
            bool(SIMKL_CLIENT_ID)
            and (mid >= 55000 or rec.get("status") in ("ONGOING", "UPCOMING"))
            and (
                not rec.get("thetvdb_id")
                or not rec.get("themoviedb_id")
                or not rec.get("imdb_id")
                or not rec.get("simkl_id")
            )
        )
        need_sk = {} if not need_simkl_check else cache.get("simkl", mid, max_age_seconds=ttl)
        if isinstance(need_sk, dict) and need_sk:
            _apply_simkl(records, mid, need_sk, ep_counts)

        want_az = need_az_check and (need_az is None)
        want_ms = ms_eligible and (need_ms is None) and (tid not in seen_ms_tvdb)
        want_sk = need_simkl_check and (need_sk is None)
        if want_ms:
            seen_ms_tvdb.add(tid)
            malsync_queue.append(mid)
        if want_az or want_sk:
            uncached_queue.append((mid, want_az, want_sk, ttl))

    ms_items = malsync_queue[:min(80, max_items)]
    if ms_items and not (deadline and time.time() >= deadline):
        def _fetch_malsync_worker(mid):
            if deadline and time.time() >= deadline:
                return mid, None
            ms = {}
            try:
                raw, status, _ = _http_get_bytes(f"https://api.malsync.moe/tvdb/rules/cache-key/{mid}", timeout=12, retries=1)
                ms = json.loads(raw.decode("utf-8")) if (status == 200 and raw) else {}
                cache.set("malsync_rules", mid, ms)
                time.sleep(0.05)
            except Exception:
                ms = {}
            return mid, ms

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for mid, ms in pool.map(_fetch_malsync_worker, ms_items):
                if isinstance(ms, dict) and ms:
                    extra_rules.extend(_apply_malsync_rules(records, mid, ms, by_anilist=by_anilist))

    az_sk_items = uncached_queue[:max_items]
    if az_sk_items and not (deadline and time.time() >= deadline):
        def _fetch_az_sk_worker(item):
            mid, fetch_az, fetch_sk, _ttl = item
            if deadline and time.time() >= deadline:
                return mid, None, None
            rec_snapshot = records.get(mid) or {}
            az = None
            sk = None
            has_tvdb = bool(rec_snapshot.get("thetvdb_id"))
            has_tmdb = bool(rec_snapshot.get("themoviedb_id"))
            has_imdb = bool(rec_snapshot.get("imdb_id"))
            has_simkl = bool(rec_snapshot.get("simkl_id"))

            if fetch_az:
                try:
                    raw, status, _ = _http_get_bytes(f"https://api.ani.zip/mappings?mal_id={mid}", timeout=12, retries=1)
                    az = json.loads(raw.decode("utf-8")) if (status == 200 and raw) else {}
                    cache.set("anizip", mid, az)
                    m = az.get("mappings") or {}
                    has_tvdb = has_tvdb or bool(parse_pos_int(m.get("thetvdb_id")))
                    has_tmdb = has_tmdb or bool(parse_pos_int(m.get("themoviedb_id")))
                    has_imdb = has_imdb or bool(clean_imdb_id(m.get("imdb_id")))
                    time.sleep(0.08)
                except Exception:
                    az = {}

            if fetch_sk and SIMKL_CLIENT_ID and not (deadline and time.time() >= deadline) and (
                not has_tvdb or not has_tmdb or not has_imdb or not has_simkl
            ):
                try:
                    sk = _fetch_simkl_record(mid, rec_snapshot.get("simkl_id"))
                    cache.set("simkl", mid, sk)
                    time.sleep(0.08)
                except Exception:
                    sk = {}
            return mid, az, sk

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for mid, az, sk in pool.map(_fetch_az_sk_worker, az_sk_items):
                if isinstance(az, dict) and az:
                    _apply_anizip(records, mid, az, ep_counts)
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


def _apply_malsync_rules(records, mal_id, ms, by_anilist=None):
    out_rules = []
    rec = records.get(mal_id)
    tvdb_id = parse_pos_int(ms.get("tvdbId"))
    if rec and tvdb_id and not rec["thetvdb_id"]:
        rec["thetvdb_id"] = tvdb_id
    for rule in ms.get("rules") or ():
        if not isinstance(rule, dict):
            continue
        prov = str(rule.get("provider") or "").lower()
        rid = parse_pos_int(rule.get("id"))
        if not rid:
            continue
        if prov == "mal":
            target_mid = rid if rid in records else None
        elif prov == "anilist" and by_anilist:
            target_mid = by_anilist.get(rid)
        else:
            continue
        if not target_mid or target_mid not in records:
            continue
        trec = records[target_mid]
        if trec.get("anime_media_type") not in ("TV", "ONA", "OVA", "SPECIAL"):
            continue
        if tvdb_id and trec.get("thetvdb_id") and trec["thetvdb_id"] != tvdb_id:
            continue
        season = parse_nonneg_int(rule.get("season"))
        start = parse_pos_int(rule.get("start"))
        ep_start = parse_pos_int(rule.get("episodeStart")) or 1
        end = parse_pos_int(rule.get("end"))
        abs_start = parse_pos_int(rule.get("absoluteStart"))
        rtype = str(rule.get("type") or "season")
        if season is None or start is None:
            continue
        if season > 0 and trec["thetvdb_season"] is None:
            trec["thetvdb_season"] = season
        out_rules.append({
            "mal_id": target_mid,
            "tvdb_id": tvdb_id or trec.get("thetvdb_id"),
            "season": season,
            "rule_type": rtype,
            "range_start": start,
            "range_end": end,
            "ep_start": ep_start,
            "absolute_start": abs_start,
            "source": "malsync_rules",
        })
    return out_rules


_VALID_FILLER_TYPES = ("Filler", "Mixed Canon/Filler", "Recap")
_RE_AFL_H1 = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
_RE_AFL_EP_TABLE = re.compile(r'<table[^>]*class="[^"]*EpisodeList[^"]*"[^>]*>(.*?)</table>', re.S | re.I)
_RE_AFL_EP_ROW = re.compile(
    r'<tr[^>]*>.*?<td[^>]*class="[^"]*Number[^"]*"[^>]*>\s*(\d+)\s*</td>.*?'
    r'<td[^>]*class="[^"]*Type[^"]*"[^>]*>\s*<span[^>]*>(.*?)</span>',
    re.S | re.I,
)
_RE_AFL_SHOW_LINK = re.compile(r'href="(/shows/([a-z0-9-]+))"', re.I)
_RE_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _classify_afl_filler(raw_type):
    if not raw_type:
        return ""
    low = str(raw_type).strip().lower()
    if "recap" in low:
        return "Recap"
    if "mixed" in low or "canon/filler" in low:
        return "Mixed Canon/Filler"
    if "filler" in low:
        return "Filler"
    return ""


def _norm_afl_title(title, strip_parens=False):
    if not title:
        return ""
    clean = re.sub(r"<[^>]+>", "", html_lib.unescape(str(title)))
    clean = re.sub(r"\s*filler\s+list\s*$", "", clean, flags=re.I)
    if strip_parens:
        clean = re.sub(r"\([^)]*\)", " ", clean)
    return _RE_NON_ALNUM.sub(" ", clean.lower()).strip()


def _parse_afl_directory_html(raw_html):
    seen = set()
    slugs = []
    for _path, slug in _RE_AFL_SHOW_LINK.findall(raw_html or ""):
        s = slug.strip().lower()
        if s and s not in seen:
            seen.add(s)
            slugs.append(s)
    return slugs


def _parse_afl_show_html(raw_html, slug=""):
    m_h1 = _RE_AFL_H1.search(raw_html or "")
    title = _norm_afl_title(m_h1.group(1)) if m_h1 else _norm_afl_title(slug.replace("-", " "))
    m_tbl = _RE_AFL_EP_TABLE.search(raw_html or "")
    body = m_tbl.group(1) if m_tbl else (raw_html or "")
    max_ep = 0
    non_canon = {}
    for num_s, raw_type in _RE_AFL_EP_ROW.findall(body):
        ep = parse_pos_int(num_s)
        if not ep:
            continue
        if ep > max_ep:
            max_ep = ep
        ftype = _classify_afl_filler(raw_type)
        if ftype:
            non_canon[str(ep)] = ftype
    return {"slug": slug, "title": title, "max_ep": max_ep, "episodes": non_canon}


def _coalesce_filler_ranges(per_ep_entries):
    """Collapse sorted (mal_id, episode, filler_type) tuples into contiguous (mal_id, ep_start, ep_end, filler_type)."""
    ranges = []
    if not per_ep_entries:
        return ranges
    ordered = sorted(set(per_ep_entries), key=lambda x: (x[0], x[1], x[2]))
    cur_mid, cur_start, cur_end, cur_type = ordered[0][0], ordered[0][1], ordered[0][1], ordered[0][2]
    for mid, ep, ftype in ordered[1:]:
        if mid == cur_mid and ftype == cur_type and ep == cur_end + 1:
            cur_end = ep
        elif mid == cur_mid and ftype == cur_type and ep == cur_end:
            continue
        else:
            ranges.append((cur_mid, cur_start, cur_end, cur_type))
            cur_mid, cur_start, cur_end, cur_type = mid, ep, ep, ftype
    ranges.append((cur_mid, cur_start, cur_end, cur_type))
    return ranges


def _map_afl_shows_to_ranges(shows, records, aliases_map, franchise_links, ep_counts):
    """Align parsed AnimeFillerList show payloads to TV/ONA MAL IDs and sequel/TVDB chains."""
    title_index = collections.defaultdict(list)
    prequel_set = {
        mid for (mid, _rel, rtype) in (franchise_links or {})
        if rtype == "PREQUEL"
    }

    _RE_SEASON_TAIL = re.compile(r"\s*(?::|-)?\s*(?:season\s+1|1st\s+season|part\s+1)\s*$", re.I)

    def _register_title(mid, raw_t, prio):
        if mid not in records or not raw_t:
            return
        rec = records[mid]
        mtype = rec.get("anime_media_type") or ""
        if mtype not in ("TV", "ONA"):
            return
        rank = (
            0 if mtype == "TV" else 1,
            prio,
            1 if mid in prequel_set else 0,
            0 if rec.get("thetvdb_id") else 1,
            mid,
        )
        norm = _norm_afl_title(raw_t)
        if norm:
            title_index[norm].append((rank, mid))
        stripped = _norm_afl_title(raw_t, strip_parens=True)
        if stripped and stripped != norm:
            s_rank = (rank[0], prio + 2, rank[2], rank[3], mid)
            title_index[stripped].append((s_rank, mid))
        base_season = _norm_afl_title(_RE_SEASON_TAIL.sub("", str(raw_t)), strip_parens=True)
        if base_season and len(base_season) >= 4 and base_season not in (norm, stripped):
            b_rank = (rank[0], prio + 4, rank[2], rank[3], mid)
            title_index[base_season].append((b_rank, mid))

    for mid, rec in records.items():
        _register_title(mid, rec.get("mal_title"), 0)
    for (mid, _norm), alias_val in (aliases_map or {}).items():
        clean_t = alias_val[0]
        ttype = alias_val[1]
        _register_title(mid, clean_t, _ALIAS_PRIORITY.get(ttype, 4))

    sequel_adj = collections.defaultdict(list)
    for (mid, rel_mid, rtype), src in (franchise_links or {}).items():
        if rtype != "SEQUEL" or mid not in records or rel_mid not in records or mid == rel_mid:
            continue
        if records[rel_mid].get("anime_media_type") not in ("TV", "ONA"):
            continue
        if (ep_counts.get(rel_mid) or 0) <= 0:
            continue
        sequel_adj[mid].append((0 if str(src).startswith("anilist") else 1, rel_mid))

    # Build ordered TVDB sibling sequence per thetvdb_id for older series lacking AniList SEQUEL edges
    tvdb_series = collections.defaultdict(list)
    for mid, rec in records.items():
        tid = rec.get("thetvdb_id")
        if not tid or rec.get("anime_media_type") not in ("TV", "ONA"):
            continue
        seas = rec.get("thetvdb_season")
        s_ord = seas if isinstance(seas, int) and seas >= 1 else (0 if seas == "a" else 999)
        part = rec.get("thetvdb_part") or 1
        tvdb_series[tid].append((s_ord, part, mid))
    for tid in tvdb_series:
        tvdb_series[tid].sort()

    per_ep_entries = []
    matched_shows = 0
    for show in shows:
        ep_map = show.get("episodes") or {}
        if not ep_map:
            continue
        slug = str(show.get("slug") or "").strip().lower()
        cand_norms = [
            _norm_afl_title(show.get("title")),
            _norm_afl_title(show.get("title"), strip_parens=True),
            _norm_afl_title(slug.replace("-", " ")),
        ]
        cands = []
        seen_norms = set()
        for norm in cand_norms:
            if norm and norm not in seen_norms and norm in title_index:
                seen_norms.add(norm)
                cands.extend(title_index[norm])
        if not cands:
            continue
        cands.sort(key=lambda x: x[0])
        root_mid = cands[0][1]
        matched_shows += 1

        max_ep = parse_pos_int(show.get("max_ep")) or max(int(k) for k in ep_map)
        root_eps = ep_counts.get(root_mid) or 0
        chain = [root_mid]
        seen_chain = {root_mid}
        total_cov = root_eps

        if max_ep > root_eps + 2:
            cur = root_mid
            while len(chain) < 16:
                nexts = [r for _, r in sorted(sequel_adj.get(cur, [])) if r not in seen_chain]
                if not nexts:
                    break
                nxt = nexts[0]
                chain.append(nxt)
                seen_chain.add(nxt)
                total_cov += (ep_counts.get(nxt) or 0)
                cur = nxt

            # Extend via ordered TVDB sibling sequence if sequel graph stopped before covering max_ep
            if total_cov < max_ep - 2:
                rtvdb = records[root_mid].get("thetvdb_id")
                sibs = [m for _, _, m in tvdb_series.get(rtvdb, [])] if rtvdb else []
                tail = chain[-1]
                start_idx = sibs.index(tail) + 1 if tail in sibs else len(sibs)
                for smid in sibs[start_idx:]:
                    if len(chain) >= 16 or total_cov >= max_ep - 2:
                        break
                    if smid in seen_chain:
                        continue
                    seps = ep_counts.get(smid) or 0
                    if seps > 0:
                        chain.append(smid)
                        seen_chain.add(smid)
                        total_cov += seps

        spans = []
        cursor = 1
        for idx, cmid in enumerate(chain):
            ceps = ep_counts.get(cmid) or 0
            cstatus = str(records[cmid].get("status") or "").upper()
            is_last = idx == len(chain) - 1
            if ceps > 0:
                if is_last and cstatus == "ONGOING" and max_ep >= cursor + ceps - 1:
                    sp_end = max_ep
                else:
                    sp_end = cursor + ceps - 1
                spans.append((cursor, sp_end, cmid))
                cursor = sp_end + 1
            elif len(chain) == 1 and cstatus != "FINISHED":
                spans.append((1, max_ep, cmid))
                break
            else:
                break

        for ep_s, ftype in ep_map.items():
            ep_i = parse_pos_int(ep_s)
            if not ep_i or ftype not in _VALID_FILLER_TYPES:
                continue
            for sp_start, sp_end, sp_mid in spans:
                if sp_start <= ep_i <= sp_end:
                    per_ep_entries.append((sp_mid, ep_i - sp_start + 1, ftype))
                    break

    return _coalesce_filler_ranges(per_ep_entries), matched_shows


def hydrate_and_map_filler_ranges(records, aliases_map, franchise_links, cache: ApiCache, ep_counts, offline_dir=None, deadline=0.0):
    """Fetch/cache AnimeFillerList show tags and map non-canon episodes to coalesced filler_ranges."""
    if offline_dir:
        fix = os.path.join(offline_dir, "animefillerlist.json")
        if not os.path.isfile(fix):
            return [], 0
        with open(fix, "r", encoding="utf-8") as f:
            raw_items = json.load(f)
        shows = []
        for entry in raw_items or ():
            if not isinstance(entry, dict):
                continue
            if "html" in entry:
                shows.append(_parse_afl_show_html(entry["html"], entry.get("slug", "")))
            else:
                shows.append({
                    "slug": entry.get("slug", ""),
                    "title": entry.get("title", ""),
                    "max_ep": entry.get("max_ep") or 0,
                    "episodes": entry.get("episodes") or {},
                })
        return _map_afl_shows_to_ranges(shows, records, aliases_map, franchise_links, ep_counts)

    slugs = cache.get("afl", "_directory", max_age_seconds=7 * 86400)
    if slugs is None:
        try:
            raw, status, _ = _http_get_bytes(SOURCE_URLS["animefillerlist"], timeout=15, retries=1)
            slugs = _parse_afl_directory_html(raw.decode("utf-8", "ignore")) if (status == 200 and raw) else []
            if slugs:
                cache.set("afl", "_directory", slugs)
        except Exception:
            slugs = []
    if not isinstance(slugs, list) or not slugs:
        return [], 0

    shows = []
    to_fetch = []
    for slug in slugs:
        cached_show = cache.get("afl", f"show_{slug}", max_age_seconds=21 * 86400)
        if isinstance(cached_show, dict):
            shows.append(cached_show)
        else:
            to_fetch.append(slug)

    if to_fetch and not (deadline and time.time() >= deadline):
        def _fetch_afl_worker(slug):
            if deadline and time.time() >= deadline:
                return None
            try:
                raw, status, _ = _http_get_bytes(f"{SOURCE_URLS['animefillerlist']}/{slug}", timeout=12, retries=1)
                if status == 200 and raw:
                    parsed = _parse_afl_show_html(raw.decode("utf-8", "ignore"), slug=slug)
                    cache.set("afl", f"show_{slug}", parsed)
                    time.sleep(0.05)
                    return parsed
                elif status == 404:
                    cache.set("afl", f"show_{slug}", {"slug": slug, "title": "", "max_ep": 0, "episodes": {}})
            except Exception:
                pass
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for parsed in pool.map(_fetch_afl_worker, to_fetch):
                if isinstance(parsed, dict):
                    shows.append(parsed)

    return _map_afl_shows_to_ranges(shows, records, aliases_map, franchise_links, ep_counts)


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


def write_sqlite_database(output_path, records, rules, stats, aliases_map=None, franchise_links=None, schedule_ids=None, filler_ranges=None, filler_shows=0):
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
            "themoviedb_id INTEGER, imdb_id INTEGER, trakt_id INTEGER, mal_title TEXT COLLATE NOCASE, "
            "mal_picture TEXT, thetvdb_season INTEGER, thetvdb_part INTEGER, "
            "duration INTEGER, score REAL, status TEXT, anime_media_type TEXT, "
            "global_media_type TEXT, anime_media_episodes TEXT, global_media_episodes TEXT, "
            "year INTEGER, anime_season TEXT, "
            "UNIQUE(mal_id))"
        )
        conn.execute(
            "CREATE TABLE tvdb_rules ("
            "mal_id INTEGER NOT NULL, tvdb_id INTEGER, season INTEGER NOT NULL, "
            "rule_type TEXT NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER, "
            "ep_start INTEGER NOT NULL, absolute_start INTEGER, source TEXT NOT NULL, "
            "PRIMARY KEY (mal_id, season, rule_type, range_start, ep_start))"
        )
        conn.execute(
            "CREATE TABLE anime_aliases ("
            "mal_id INTEGER NOT NULL, title TEXT NOT NULL COLLATE NOCASE, "
            "PRIMARY KEY (mal_id, title)) WITHOUT ROWID"
        )
        conn.execute(
            "CREATE TABLE franchise_links ("
            "mal_id INTEGER NOT NULL, related_mal_id INTEGER NOT NULL, "
            "relation_type TEXT NOT NULL, source TEXT NOT NULL, "
            "PRIMARY KEY (mal_id, related_mal_id, relation_type)) WITHOUT ROWID"
        )
        conn.execute(
            "CREATE TABLE schedule_ids ("
            "mal_id INTEGER PRIMARY KEY, syobocal_tid INTEGER, annict_id INTEGER)"
        )
        conn.execute(
            "CREATE TABLE filler_ranges ("
            "mal_id INTEGER NOT NULL, ep_start INTEGER NOT NULL, "
            "ep_end INTEGER NOT NULL, filler_type TEXT NOT NULL, "
            "PRIMARY KEY (mal_id, ep_start, ep_end)) WITHOUT ROWID"
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
            mid = rl["mal_id"]
            if mid not in records:
                continue
            r_tvdb = rl.get("tvdb_id")
            a_tvdb = records[mid].get("thetvdb_id")
            if r_tvdb and a_tvdb and r_tvdb != a_tvdb:
                continue
            if (rl.get("range_start") or 0) < 1 or (rl.get("ep_start") or 0) < 1 or (rl.get("season") is None or rl["season"] < 0):
                continue
            key = (mid, rl["season"], rl["rule_type"], rl["range_start"], rl["ep_start"])
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

        alias_rows = [
            (mid, val[0])
            for (mid, _norm), val in sorted((aliases_map or {}).items())
            if mid in records
        ]
        if alias_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO anime_aliases (mal_id, title) VALUES (?, ?)",
                alias_rows,
            )

        link_rows = [
            (mid, rel_mid, rtype, src)
            for (mid, rel_mid, rtype), src in sorted((franchise_links or {}).items())
            if mid in records and rel_mid in records and mid != rel_mid
        ]
        if link_rows:
            conn.executemany(
                "INSERT INTO franchise_links (mal_id, related_mal_id, relation_type, source) VALUES (?, ?, ?, ?)",
                link_rows,
            )

        sched_rows = [
            (mid, syo, ann)
            for mid, (syo, ann) in sorted((schedule_ids or {}).items())
            if mid in records and (syo or ann)
        ]
        if sched_rows:
            conn.executemany(
                "INSERT INTO schedule_ids (mal_id, syobocal_tid, annict_id) VALUES (?, ?, ?)",
                sched_rows,
            )

        clean_filler_rows = [
            (mid, s, e, ft)
            for (mid, s, e, ft) in (filler_ranges or [])
            if mid in records and s >= 1 and e >= s and ft in _VALID_FILLER_TYPES
        ]
        if clean_filler_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO filler_ranges (mal_id, ep_start, ep_end, filler_type) VALUES (?, ?, ?, ?)",
                clean_filler_rows,
            )

        meta_rows = [
            ("schema_version", SCHEMA_VERSION),
            ("built_at_utc", datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
            ("row_count", str(len(records))),
            ("rule_count", str(len(dedup_rules))),
            ("alias_count", str(len(alias_rows))),
            ("franchise_link_count", str(len(link_rows))),
            ("schedule_id_count", str(len(sched_rows))),
            ("filler_range_count", str(len(clean_filler_rows))),
            ("filler_show_count", str(int(filler_shows or 0))),
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
    for col in ("anilist_id", "kitsu_id", "anidb_id", "simkl_id", "thetvdb_id", "themoviedb_id", "imdb_id", "trakt_id", "mal_dub_id"):
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
    plexanibridge_bytes = load_source_blob(
        "plexanibridge", SOURCE_URLS["plexanibridge"], args.cache_dir, args.offline_fixtures,
        max_cache_age=24 * 3600, optional=True,
    )
    wikidata_bytes = load_source_blob(
        "wikidata", _build_wikidata_url(), args.cache_dir, args.offline_fixtures,
        max_cache_age=24 * 3600,
        extra_headers={"Accept": "application/sparql-results+json"},
        optional=True,
    )

    aliases_map = {}
    franchise_links = {}
    schedule_ids = {}

    manami_related, manami_eps, manami_mal_ids = merge_manami(records, manami_bytes, aliases_map=aliases_map)
    ep_counts.update(manami_eps)
    offsets = merge_fribb(records, fribb_bytes, ep_counts)
    merge_arm(records, arm_bytes, schedule_ids=schedule_ids)
    xml_rules = merge_anime_lists_xml(records, xml_bytes, ep_counts, offsets)
    merge_kometa(records, kometa_bytes, ep_counts, offsets)
    merge_wikidata(records, wikidata_bytes)
    merge_plexanibridge(records, plexanibridge_bytes)
    merge_anidb_titles(records, anidb_titles_bytes, aliases_map=aliases_map)
    merge_mal_dubs(records, dubs_bytes)

    extra_rules = []
    prequel_edges = {}
    wall_deadline = 0.0
    if args.api_budget_seconds > 0 or args.offline_fixtures:
        now_s = time.time()
        wall_deadline = (now_s + args.api_budget_seconds) if not args.offline_fixtures else 0.0
        anilist_deadline = (now_s + args.api_budget_seconds * 0.25) if wall_deadline else 0.0
        anizip_deadline = (now_s + args.api_budget_seconds * 0.55) if wall_deadline else 0.0
        trakt_deadline = (now_s + args.api_budget_seconds * 0.80) if wall_deadline else 0.0
        prequel_edges = hydrate_anilist_relations(
            records, cache, ep_counts, args.offline_fixtures,
            deadline=anilist_deadline, franchise_links=franchise_links, aliases_map=aliases_map,
        )
        extra_rules = hydrate_anizip_and_malsync(
            records, cache, ep_counts, args.offline_fixtures, max_items=args.max_api_items, deadline=anizip_deadline
        )
        hydrate_trakt_ids(
            records, cache, args.offline_fixtures,
            max_items=min(1000, args.max_api_items), deadline=trakt_deadline, aliases_map=aliases_map,
        )

    apply_prequel_inheritance(records, prequel_edges, manami_related)
    sanitize_trakt_collisions(records)
    pruned_shells = finalize_parts_and_uniqueness(records, seed_ids=seed_ids, canonical_mal_ids=manami_mal_ids)

    # Add symmetric inverse PREQUEL <-> SEQUEL edges + Manami RELATED fallback before AFL chain mapping
    explicit_pairs = set()
    inv_additions = {}
    for (mid, rel_mid, rtype), src in franchise_links.items():
        if mid not in records or rel_mid not in records or mid == rel_mid:
            continue
        explicit_pairs.add((mid, rel_mid))
        if rtype == "PREQUEL":
            inv_additions.setdefault((rel_mid, mid, "SEQUEL"), f"{src}_inv")
        elif rtype == "SEQUEL":
            inv_additions.setdefault((rel_mid, mid, "PREQUEL"), f"{src}_inv")
    for k, v in inv_additions.items():
        franchise_links.setdefault(k, v)
        explicit_pairs.add((k[0], k[1]))
    for mid, rels in manami_related.items():
        if mid not in records:
            continue
        for rel_mid in rels:
            if rel_mid in records and rel_mid != mid and (mid, rel_mid) not in explicit_pairs:
                franchise_links.setdefault((mid, rel_mid, "RELATED"), "manami")

    # Map AnimeFillerList ranges using full pre-trimmed aliases & complete SEQUEL chains
    filler_ranges = []
    filler_shows = 0
    if args.api_budget_seconds > 0 or args.offline_fixtures:
        filler_ranges, filler_shows = hydrate_and_map_filler_ranges(
            records, aliases_map, franchise_links, cache, ep_counts,
            offline_dir=args.offline_fixtures, deadline=wall_deadline,
        )

    # Filter alternate aliases (skip canonical mal_title already stored on anime.mal_title, max 5 English/Romaji-prioritized alternates per show)
    per_show_aliases = collections.defaultdict(list)
    primary_norms = {mid: _norm_alias(rec.get("mal_title")) for mid, rec in records.items()}
    for (mid, norm), alias_val in aliases_map.items():
        if mid not in records or norm == primary_norms.get(mid):
            continue
        clean_t, ttype = alias_val[0], alias_val[1]
        lrank = alias_val[2] if len(alias_val) > 2 else 2
        short_pen = 1 if len(clean_t) <= 3 else 0
        per_show_aliases[mid].append((_ALIAS_PRIORITY.get(ttype, 5), lrank, short_pen, len(clean_t), norm, clean_t, ttype))
    slim_aliases = {}
    for mid, items in per_show_aliases.items():
        items.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        for _, _, _, _, norm, clean_t, ttype in items[:5]:
            slim_aliases[(mid, norm)] = (clean_t, ttype)
    aliases_map = slim_aliases

    # Trim collections to surviving records for exact manifest counts
    aliases_map = {k: v for k, v in aliases_map.items() if k[0] in records}
    franchise_links = {k: v for k, v in franchise_links.items() if k[0] in records and k[1] in records and k[0] != k[1]}
    schedule_ids = {k: v for k, v in schedule_ids.items() if k in records and (v[0] or v[1])}
    filler_ranges = [r for r in filler_ranges if r[0] in records]

    all_rules = xml_rules + extra_rules
    stats = compute_stats(records)
    write_sqlite_database(
        args.output, records, all_rules, stats,
        aliases_map=aliases_map, franchise_links=franchise_links, schedule_ids=schedule_ids,
        filler_ranges=filler_ranges, filler_shows=filler_shows,
    )

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
        "alias_count": len(aliases_map),
        "franchise_link_count": len(franchise_links),
        "schedule_id_count": len(schedule_ids),
        "filler_range_count": len(filler_ranges),
        "filler_show_count": filler_shows,
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
            f"**TVDB/Cour Rules**: `{manifest['rule_count']:,}` | "
            f"**Aliases**: `{manifest['alias_count']:,}` | "
            f"**Franchise Links**: `{manifest['franchise_link_count']:,}` | "
            f"**Schedule IDs**: `{manifest['schedule_id_count']:,}` | "
            f"**Filler Ranges**: `{manifest['filler_range_count']:,}` (`{manifest['filler_show_count']:,}` shows)\n"
            f"- **Archives**: {xz_note}`anime_mappings.db` (`{raw_size:,}` bytes)\n"
            f"- **Coverage**: `anilist_id={c['anilist_id']:,}`, `kitsu_id={c['kitsu_id']:,}`, "
            f"`anidb_id={c['anidb_id']:,}`, `simkl_id={c['simkl_id']:,}`, `thetvdb_id={c['thetvdb_id']:,}`, "
            f"`themoviedb_id={c['themoviedb_id']:,}`, `imdb_id={c['imdb_id']:,}`, `trakt_id={c['trakt_id']:,}`, "
            f"`mal_dub_id={c['mal_dub_id']:,}`, `year={c['year']:,}`, `anime_season={c['anime_season']:,}`\n"
            f"- **Recent (`mal_id >= 55000`, n=`{c['recent_total']:,}`)**: "
            f"`thetvdb_id={c['recent_thetvdb_id']:,}`, `themoviedb_id={c['recent_themoviedb_id']:,}`, "
            f"`anidb_id={c['recent_anidb_id']:,}`, `trakt_id={c['recent_trakt_id']:,}`\n"
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
