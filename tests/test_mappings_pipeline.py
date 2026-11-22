#!/usr/bin/env python3
"""Hermetic offline unit tests for Otaku-Mapping-DB builder and validator."""

import gzip
import json
import os
import sqlite3
import sys
import tempfile
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPTS_DIR = os.path.join(_REPO_ROOT, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import build_mappings  # noqa: E402
import validate_mappings  # noqa: E402


class TestMappingsPipeline(unittest.TestCase):
    def _write_seed(self, path):
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE anime (mal_id INTEGER PRIMARY KEY, mal_dub_id INTEGER, anilist_id INTEGER, "
            "kitsu_id INTEGER, anidb_id INTEGER, simkl_id INTEGER, thetvdb_id INTEGER, "
            "themoviedb_id INTEGER, imdb_id INTEGER, trakt_id INTEGER, mal_title TEXT, "
            "mal_picture TEXT, thetvdb_season INTEGER, thetvdb_part INTEGER, "
            "duration INTEGER, score REAL, status TEXT, anime_media_type TEXT, "
            "global_media_type TEXT, anime_media_episodes TEXT, global_media_episodes TEXT, UNIQUE(mal_id))"
        )
        conn.execute("CREATE TABLE activities (sync_id INTEGER PRIMARY KEY, last_updated TEXT NOT NULL)")
        conn.execute("INSERT INTO activities VALUES (1, '1700000000')")
        # Seed legacy show with trakt_id + dirty '' imdb_id that must be cleaned to NULL
        conn.execute(
            "INSERT INTO anime (mal_id, anilist_id, thetvdb_id, themoviedb_id, imdb_id, trakt_id, mal_title, "
            "thetvdb_season, anime_media_type, global_media_type, anime_media_episodes, global_media_episodes) "
            "VALUES (21, 21, 81797, 37854, '', 37696, 'One Piece', 'a', 'TV', 'show', '1 - 1100', '1 - 1100')"
        )
        # Seed synthetic orphan shell whose duplicate anilist_id (101) gets claimed by real mal_id=100 and must be pruned
        conn.execute(
            "INSERT INTO anime (mal_id, anilist_id, anime_media_type) "
            "VALUES (66863, 101, 'TV')"
        )
        conn.commit()
        conn.close()

    def _write_fixtures(self, fdir):
        manami = {
            "data": [
                {
                    "sources": [
                        "https://myanimelist.net/anime/100",
                        "https://anilist.co/anime/101",
                        "https://anidb.net/anime/102",
                        "https://kitsu.app/anime/103",
                        "https://simkl.com/anime/104",
                    ],
                    "title": "Test Saga Season 1",
                    "type": "TV",
                    "episodes": 12,
                    "status": "FINISHED",
                    "animeSeason": {"year": 2023, "season": "FALL"},
                    "picture": "https://cdn.myanimelist.net/images/anime/12/34567.jpg",
                    "duration": {"value": 1440, "unit": "SECONDS"},
                    "score": {"median": 8.456},
                    "tags": ["action", "fantasy", "mostly teen cast"],
                    "studios": ["wit studio"],
                },
                {
                    "sources": [
                        "https://myanimelist.net/anime/200",
                        "https://anilist.co/anime/201",
                    ],
                    "title": "Test Saga Season 2",
                    "type": "TV",
                    "episodes": 12,
                    "status": "UPCOMING",
                    "picture": "https://cdn.myanimelist.net/images/anime/99/88888.jpg",
                    "duration": {"value": 24, "unit": "MINUTES"},
                    "relatedAnime": ["https://myanimelist.net/anime/100"],
                },
                {
                    "sources": [
                        "https://myanimelist.net/anime/300",
                        "https://anilist.co/anime/201",  # intentional UNIQUE collision test with mal_id=200
                        "https://anidb.net/anime/302",
                    ],
                    "title": "Split Cour Show",
                    "type": "TV",
                    "episodes": 13,
                    "status": "ONGOING",
                },
                {
                    "sources": [
                        "https://myanimelist.net/anime/500",
                    ],
                    "title": "Simkl Hydrated Show",
                    "type": "TV",
                    "episodes": 12,
                    "status": "FINISHED",
                },
                {
                    "sources": ["https://myanimelist.net/anime/309", "https://anilist.co/anime/491"],
                    "title": "Overwrite Guard Show",
                    "type": "TV",
                    "episodes": 13,
                    "status": "FINISHED",
                },
                {
                    "sources": ["https://myanimelist.net/anime/600"],
                    "title": "PlexAniBridge TV Fill",
                    "type": "TV",
                    "episodes": 12,
                    "status": "FINISHED",
                },
                {
                    "sources": ["https://myanimelist.net/anime/601"],
                    "title": "PlexAniBridge Movie Guard",
                    "type": "MOVIE",
                    "episodes": 1,
                    "status": "FINISHED",
                },
                {
                    "sources": ["https://myanimelist.net/anime/700"],
                    "title": "Legacy Saga (Kyuu Saga)",
                    "type": "TV",
                    "episodes": 12,
                    "status": "FINISHED",
                    "synonyms": ["000", "LS", "Legacy Saga Full English Title &amp; Extra"],
                },
                {
                    "sources": ["https://myanimelist.net/anime/701"],
                    "title": "Legacy Saga 2nd Season",
                    "type": "TV",
                    "episodes": 12,
                    "status": "FINISHED",
                },
            ]
        }
        with open(os.path.join(fdir, "manami.json"), "w", encoding="utf-8") as f:
            json.dump(manami, f)

        fribb = [
            {
                "type": "TV",
                "mal_id": 100,
                "anilist_id": 101,
                "anidb_id": 102,
                "tvdb_id": 777001,
                "themoviedb_id": {"tv": 888001},
                "imdb_id": ["tt1234567"],
                "season": {"tvdb": 1},
            },
            {
                "type": "TV",
                "mal_id": 300,
                "anidb_id": 302,
                "tvdb_id": 999001,
                "episode_offset": 12,
                "season": {"tvdb": 1},
            },
            {
                "type": "TV",
                "mal_id": 400,
                "anidb_id": 402,
            },
            {
                "type": "TV",
                "mal_id": 309,
                "tvdb_id": 189011,
                "themoviedb_id": {"tv": 35767},
                "imdb_id": ["tt14642362"],
                "season": {"tvdb": 1},
            },
            {
                "type": "TV",
                "anilist_id": 491,
                "anidb_id": 2841,
                "tvdb_id": 167921,
                "themoviedb_id": {"tv": 56389},
                "imdb_id": ["tt0367414"],
                "season": {"tvdb": 0},
            },
            {
                "type": "TV",
                "mal_id": 700,
                "tvdb_id": 700700,
                "season": {"tvdb": 1},
            },
            {
                "type": "TV",
                "mal_id": 701,
                "tvdb_id": 700700,
                "season": {"tvdb": 2},
            },
        ]
        with open(os.path.join(fdir, "fribb.json"), "w", encoding="utf-8") as f:
            json.dump(fribb, f)

        pab = [
            {
                "mal_id": [600],
                "tvdb_id": 600001,
                "tmdb_show_id": 600002,
                "imdb_id": ["tt6000001"],
                "tvdb_mappings": {"s2": "e1-e12"},
            },
            {
                "mal_id": [601],
                "tvdb_id": 777001,
                "tmdb_show_id": 888001,
                "imdb_id": ["tt1234567"],
                "tvdb_mappings": {"s0": "e1"},
            },
        ]
        with open(os.path.join(fdir, "plexanibridge.json"), "w", encoding="utf-8") as f:
            json.dump(pab, f)

        with open(os.path.join(fdir, "arm.json"), "w", encoding="utf-8") as f:
            json.dump([{"mal_id": 400, "anilist_id": 401, "annict_id": 1, "syobocal_tid": 2}], f)

        wikidata = {
            "results": {
                "bindings": [
                    {
                        "mal": {"type": "literal", "value": "400"},
                        "imdb": {"type": "literal", "value": "tt9998887"},
                        "tvdb": {"type": "literal", "value": "444001"},
                        "tmdbTv": {"type": "literal", "value": "555001"},
                    }
                ]
            }
        }
        with open(os.path.join(fdir, "wikidata.json"), "w", encoding="utf-8") as f:
            json.dump(wikidata, f)

        anidb_xml = """<?xml version="1.0" encoding="utf-8"?>
<animetitles>
  <anime aid="402">
    <title xml:lang="en" type="official">AniDB Restored Official EN</title>
    <title xml:lang="x-jat" type="main">AniDB Restored Main</title>
  </anime>
</animetitles>""".encode("utf-8")
        with open(os.path.join(fdir, "anidb_titles.xml.gz"), "wb") as f:
            f.write(gzip.compress(anidb_xml))

        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<anime-list>
  <anime anidbid="102" tvdbid="777001" defaulttvdbseason="1" tmdbtv="888001" imdbid="tt1234567">
    <name>Test Saga Season 1</name>
    <mapping-list>
      <mapping anidbseason="1" tvdbseason="0">;13-1;14-0;</mapping>
    </mapping-list>
  </anime>
  <anime anidbid="99999" tvdbid="movie" defaulttvdbseason="0">
    <name>Non-integer sentinel skip check</name>
  </anime>
</anime-list>"""
        with open(os.path.join(fdir, "anime_lists.xml"), "w", encoding="utf-8") as f:
            f.write(xml_str)

        with open(os.path.join(fdir, "kometa.json"), "w", encoding="utf-8") as f:
            json.dump({"102": {"mal_id": 100, "tvdb_id": 777001, "tvdb_season": 1}}, f)

        with open(os.path.join(fdir, "mal_dubs.json"), "w", encoding="utf-8") as f:
            json.dump({"dubbed": [100, 200]}, f)

        anilist_batch = [
            {
                "id": 201,
                "idMal": 200,
                "status": "NOT_YET_RELEASED",
                "episodes": 12,
                "duration": 24,
                "format": "TV",
                "season": "SPRING",
                "seasonYear": 2024,
                "genres": ["Action", "Adventure"],
                "startDate": {"year": 2024, "month": 4, "day": 6},
                "studios": {"nodes": [{"name": "MAPPA"}]},
                "title": {"romaji": "Test Saga Season 2", "english": "Test Saga II"},
                "relations": {
                    "edges": [
                        {"relationType": "PREQUEL", "node": {"id": 101, "idMal": 100, "format": "TV"}}
                    ]
                },
            }
        ]
        with open(os.path.join(fdir, "anilist_batch.json"), "w", encoding="utf-8") as f:
            json.dump(anilist_batch, f)

        malsync_rules = {
            "300": {
                "tvdbId": 999001,
                "rules": [
                    {
                        "source": "tvdb", "season": 1, "provider": "mal", "id": 300,
                        "start": 13, "end": 25, "episodeStart": 1, "type": "cour", "absoluteStart": 13,
                    }
                ],
            }
        }
        with open(os.path.join(fdir, "malsync_rules.json"), "w", encoding="utf-8") as f:
            json.dump(malsync_rules, f)

        simkl_items = {
            "500": {
                "title": "Simkl Hydrated Show",
                "total_episodes": 12,
                "ids": {
                    "mal": "500",
                    "simkl": 504,
                    "tvdb": "666001",
                    "tmdb": "777002",
                    "imdb": "tt5000500",
                },
            },
            "300": {
                "title": "Wrong Show Guard",
                "ids": {"mal": "99999", "simkl": 999, "tvdb": "111", "imdb": "tt0000111"},
            },
        }
        with open(os.path.join(fdir, "simkl_items.json"), "w", encoding="utf-8") as f:
            json.dump(simkl_items, f)

        with open(os.path.join(fdir, "trakt_items.json"), "w", encoding="utf-8") as f:
            json.dump({"500": {"trakt_id": 99500}}, f)

        afl_items = [
            {
                "slug": "test-saga",
                "title": "Test Saga Season 1",
                "max_ep": 24,
                "episodes": {
                    "5": "Mixed Canon/Filler",
                    "14": "Filler",
                    "15": "Filler",
                    "16": "Recap",
                },
            },
            {
                "slug": "simkl-hydrated-show",
                "html": (
                    "<h1>Simkl Hydrated Show Filler List</h1>"
                    '<table class="EpisodeList"><tbody>'
                    '<tr><td class="Number">1</td><td class="Title">Canon</td><td class="Type"><span>Manga Canon</span></td></tr>'
                    '<tr><td class="Number">2</td><td class="Title">Beach</td><td class="Type"><span>Filler</span></td></tr>'
                    '<tr><td class="Number">3</td><td class="Title">Pool</td><td class="Type"><span>Mostly Filler</span></td></tr>'
                    "<tr><td class=\"Number\">4</td><td class=\"Title\">Summary</td><td class=\"Type\"><span>Recap</span></td></tr>"
                    "</tbody></table>"
                ),
            },
            {
                "slug": "legacy-saga",
                "title": "Legacy Saga",
                "max_ep": 24,
                "episodes": {
                    "14": "Filler",
                },
            },
        ]
        with open(os.path.join(fdir, "animefillerlist.json"), "w", encoding="utf-8") as f:
            json.dump(afl_items, f)

    def test_hermetic_build_inheritance_indexes_and_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            seed_p = os.path.join(tmp, "seed.db")
            fdir = os.path.join(tmp, "fixtures")
            os.makedirs(fdir, exist_ok=True)
            self._write_seed(seed_p)
            self._write_fixtures(fdir)

            out_db = os.path.join(tmp, "dist", "anime_mappings.db")
            manifest_p = os.path.join(tmp, "dist", "manifest.json")

            ns = build_mappings.argparse.Namespace(
                seed=seed_p,
                output=out_db,
                compress_xz=True,
                manifest=manifest_p,
                cache_dir=os.path.join(tmp, "cache"),
                offline_fixtures=fdir,
                api_budget_seconds=0.0,
                max_api_items=10,
            )
            manifest = build_mappings.build_pipeline(ns)
            self.assertEqual(manifest["pruned_seed_shells"], 1)
            self.assertEqual(manifest["total_rows"], 11)  # seed 21 + 100, 200, 300, 309, 400, 500, 600, 601, 700, 701 (66863 pruned)
            self.assertGreaterEqual(manifest["rule_count"], 2)
            self.assertGreaterEqual(manifest["alias_count"], 1)
            self.assertGreaterEqual(manifest["franchise_link_count"], 2)
            self.assertEqual(manifest["schedule_id_count"], 1)

            errs = validate_mappings.validate_database(
                out_db,
                baseline_path=seed_p,
                xz_path=f"{out_db}.xz",
                manifest_path=manifest_p,
                min_rows=11,
            )
            self.assertEqual(errs, [], f"Validation unexpectedly failed: {errs}")

            conn = sqlite3.connect(out_db)
            conn.row_factory = sqlite3.Row
            try:
                # 1. Seed trakt_id preserved & dirty '' imdb_id cleaned to NULL; phantom shell 66863 pruned
                r21 = conn.execute("SELECT * FROM anime WHERE mal_id=21").fetchone()
                self.assertEqual(r21["trakt_id"], 37696)
                self.assertIsNone(r21["imdb_id"])
                self.assertEqual(r21["thetvdb_season"], "a")
                self.assertIsNone(conn.execute("SELECT mal_id FROM anime WHERE mal_id=66863").fetchone())

                # 2. Manami picture, score, dub, genres, studios, & animeSeason year/season populated
                r100 = conn.execute("SELECT * FROM anime WHERE mal_id=100").fetchone()
                self.assertEqual(r100["mal_picture"], "12/34567.jpg")
                self.assertEqual(r100["score"], 8.46)
                self.assertEqual(r100["mal_dub_id"], 100)
                self.assertEqual(r100["thetvdb_id"], 777001)
                self.assertEqual(r100["year"], 2023)
                self.assertEqual(r100["anime_season"], "FALL")
                self.assertEqual(r100["episodes"], 12)
                self.assertEqual(r100["genres"], "Action, Fantasy")
                self.assertEqual(r100["studios"], "WIT Studio")

                # 3. Direct AniList PREQUEL inheritance populated mal_id=200 + AniList rich metadata + symmetric SEQUEL link
                r200 = conn.execute("SELECT * FROM anime WHERE mal_id=200").fetchone()
                self.assertEqual(r200["thetvdb_id"], 777001)
                self.assertEqual(r200["themoviedb_id"], 888001)
                self.assertEqual(r200["imdb_id"], "tt1234567")
                self.assertEqual(r200["thetvdb_season"], 2)
                self.assertEqual(r200["year"], 2024)
                self.assertEqual(r200["anime_season"], "SPRING")
                self.assertEqual(r200["title_english"], "Test Saga II")
                self.assertEqual(r200["genres"], "Action, Adventure")
                self.assertEqual(r200["studios"], "MAPPA")
                self.assertEqual(r200["start_date"], "2024-04-06")
                self.assertEqual(r200["episodes"], 12)

                links_200 = {(r["related_mal_id"], r["relation_type"]) for r in conn.execute("SELECT * FROM franchise_links WHERE mal_id=200")}
                self.assertIn((100, "PREQUEL"), links_200)
                links_100 = {(r["related_mal_id"], r["relation_type"]) for r in conn.execute("SELECT * FROM franchise_links WHERE mal_id=100")}
                self.assertIn((200, "SEQUEL"), links_100)

                # 4. Split-cour episode offset applied, non-unique sibling IDs preserved + deterministic primary lookup
                r300 = conn.execute("SELECT * FROM anime WHERE mal_id=300").fetchone()
                self.assertEqual(r300["global_media_episodes"], "13 - 25")
                self.assertEqual(r300["anilist_id"], 201)  # preserved in v2.6 non-unique index
                primary_al201 = conn.execute("SELECT mal_id FROM anime WHERE anilist_id=201 ORDER BY mal_id ASC LIMIT 1").fetchone()[0]
                self.assertEqual(primary_al201, 200)
                self.assertIsNone(r300["simkl_id"])  # mismatched Simkl mal=99999 rejected
                r309 = conn.execute("SELECT * FROM anime WHERE mal_id=309").fetchone()
                self.assertEqual(r309["thetvdb_id"], 189011)
                self.assertEqual(r309["themoviedb_id"], 35767)
                self.assertEqual(r309["imdb_id"], "tt14642362")
                self.assertEqual(r309["thetvdb_season"], 1)

                # 5. ARM + Wikidata + AniDB titles + schedule_ids + anime_aliases backfill for mal_id=400
                r400 = conn.execute("SELECT * FROM anime WHERE mal_id=400").fetchone()
                self.assertEqual(r400["anilist_id"], 401)
                self.assertEqual(r400["thetvdb_id"], 444001)
                self.assertEqual(r400["themoviedb_id"], 555001)
                self.assertEqual(r400["imdb_id"], "tt9998887")
                self.assertEqual(r400["global_media_type"], "show")
                self.assertEqual(r400["mal_title"], "AniDB Restored Main")
                aliases_400 = {r["title"] for r in conn.execute("SELECT title FROM anime_aliases WHERE mal_id=400")}
                self.assertNotIn("AniDB Restored Main", aliases_400)  # primary title kept on anime table only
                self.assertIn("AniDB Restored Official EN", aliases_400)
                s400 = conn.execute("SELECT * FROM schedule_ids WHERE mal_id=400").fetchone()
                self.assertEqual(s400["annict_id"], 1)
                self.assertEqual(s400["syobocal_tid"], 2)

                # 6. Simkl + Trakt hydration for mal_id=500
                r500 = conn.execute("SELECT * FROM anime WHERE mal_id=500").fetchone()
                self.assertEqual(r500["simkl_id"], 504)
                self.assertEqual(r500["thetvdb_id"], 666001)
                self.assertEqual(r500["themoviedb_id"], 777002)
                self.assertEqual(r500["imdb_id"], "tt5000500")
                self.assertEqual(r500["trakt_id"], 99500)

                # 6b. PlexAniBridge fills TV/ONA & seasons while guarding MOVIE against parent TV IMDb/TMDB
                r600 = conn.execute("SELECT * FROM anime WHERE mal_id=600").fetchone()
                self.assertEqual(r600["thetvdb_id"], 600001)
                self.assertEqual(r600["themoviedb_id"], 600002)
                self.assertEqual(r600["imdb_id"], "tt6000001")
                self.assertEqual(r600["thetvdb_season"], 2)
                r601 = conn.execute("SELECT * FROM anime WHERE mal_id=601").fetchone()
                self.assertIsNone(r601["thetvdb_id"])
                self.assertIsNone(r601["themoviedb_id"])
                self.assertIsNone(r601["imdb_id"])

                # 7. tvdb_rules skips range_start=0 sentinels and populates MALSync + XML rules
                rules = conn.execute("SELECT * FROM tvdb_rules ORDER BY mal_id, rule_type").fetchall()
                sources = {r["source"] for r in rules}
                self.assertIn("anime_lists_xml", sources)
                self.assertIn("malsync_rules", sources)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM tvdb_rules WHERE range_start < 1").fetchone()[0], 0)

                # 8. filler_ranges maps multi-season SEQUEL & TVDB-sibling chains and unescapes/filters aliases
                fr_100 = conn.execute("SELECT ep_start, ep_end, filler_type FROM filler_ranges WHERE mal_id=100 ORDER BY ep_start").fetchall()
                self.assertEqual([tuple(r) for r in fr_100], [(5, 5, "Mixed Canon/Filler")])
                fr_200 = conn.execute("SELECT ep_start, ep_end, filler_type FROM filler_ranges WHERE mal_id=200 ORDER BY ep_start").fetchall()
                self.assertEqual([tuple(r) for r in fr_200], [(2, 3, "Filler"), (4, 4, "Recap")])
                fr_500 = conn.execute("SELECT ep_start, ep_end, filler_type FROM filler_ranges WHERE mal_id=500 ORDER BY ep_start").fetchall()
                self.assertEqual([tuple(r) for r in fr_500], [(2, 3, "Filler"), (4, 4, "Recap")])
                fr_700 = conn.execute("SELECT ep_start, ep_end FROM filler_ranges WHERE mal_id=700").fetchall()
                self.assertEqual(fr_700, [])
                fr_701 = conn.execute("SELECT ep_start, ep_end, filler_type FROM filler_ranges WHERE mal_id=701").fetchall()
                self.assertEqual([tuple(r) for r in fr_701], [(2, 2, "Filler")])
                aliases_700 = {r["title"] for r in conn.execute("SELECT title FROM anime_aliases WHERE mal_id=700")}
                self.assertNotIn("000", aliases_700)
                self.assertIn("Legacy Saga Full English Title & Extra", aliases_700)
                self.assertEqual(manifest["schema_version"], "2.6")
                self.assertEqual(manifest["filler_show_count"], 3)
                self.assertEqual(manifest["filler_range_count"], 6)
                like_plan = " | ".join(
                    str(r[-1])
                    for r in conn.execute("EXPLAIN QUERY PLAN SELECT mal_id FROM anime WHERE mal_title LIKE 'Test%'").fetchall()
                )
                self.assertIn("SEARCH anime USING COVERING INDEX ix_mal_title", like_plan)
                fts_ids = [r[0] for r in conn.execute("SELECT rowid FROM anime_fts WHERE anime_fts MATCH '\"Test Saga II\"'").fetchall()]
                self.assertEqual(fts_ids, [200])
                season_plan = " | ".join(
                    str(r[-1])
                    for r in conn.execute(
                        "EXPLAIN QUERY PLAN SELECT mal_id FROM anime "
                        "WHERE year = 2024 AND anime_season = 'SPRING' AND anime_media_type = 'TV' "
                        "ORDER BY score DESC, mal_id DESC"
                    ).fetchall()
                )
                self.assertNotIn("TEMP B-TREE", season_plan)
                self.assertNotIn("SCAN anime", season_plan)
            finally:
                conn.close()

    def test_validator_rejects_missing_index_or_dirty_imdb(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_db = os.path.join(tmp, "bad.db")
            conn = sqlite3.connect(bad_db)
            conn.execute(
                "CREATE TABLE anime (mal_id INTEGER PRIMARY KEY, mal_dub_id INTEGER, anilist_id INTEGER, "
                "kitsu_id INTEGER, anidb_id INTEGER, simkl_id INTEGER, thetvdb_id INTEGER, "
                "themoviedb_id INTEGER, imdb_id TEXT, trakt_id INTEGER, mal_title TEXT, "
                "mal_picture TEXT, thetvdb_season INTEGER, thetvdb_part INTEGER, "
                "duration INTEGER, score REAL, status TEXT, anime_media_type TEXT, "
                "global_media_type TEXT, anime_media_episodes TEXT, global_media_episodes TEXT, "
                "year INTEGER, anime_season TEXT, episodes INTEGER, title_english TEXT, "
                "genres TEXT, studios TEXT, start_date TEXT, trakt_slug TEXT)"
            )
            conn.execute("CREATE TABLE activities (sync_id INTEGER PRIMARY KEY, last_updated TEXT NOT NULL)")
            conn.execute("INSERT INTO activities VALUES (1, '1700000000')")
            conn.execute("CREATE TABLE tvdb_rules (mal_id INTEGER, tvdb_id INTEGER, season INTEGER, rule_type TEXT, range_start INTEGER, range_end INTEGER, ep_start INTEGER, absolute_start INTEGER, source TEXT)")
            conn.execute("CREATE TABLE anime_aliases (mal_id INTEGER, title TEXT)")
            conn.execute("CREATE TABLE franchise_links (mal_id INTEGER, related_mal_id INTEGER, relation_type TEXT, source TEXT)")
            conn.execute("CREATE TABLE schedule_ids (mal_id INTEGER PRIMARY KEY, syobocal_tid INTEGER, annict_id INTEGER)")
            conn.execute("CREATE TABLE filler_ranges (mal_id INTEGER NOT NULL, ep_start INTEGER NOT NULL, ep_end INTEGER NOT NULL, filler_type TEXT NOT NULL, PRIMARY KEY (mal_id, ep_start, ep_end)) WITHOUT ROWID")
            conn.execute("CREATE VIRTUAL TABLE anime_fts USING fts5(mal_title, title_english, aliases_text)")
            conn.execute("CREATE TABLE build_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            conn.execute("INSERT INTO anime (mal_id, thetvdb_id, imdb_id, trakt_id, anime_media_type) VALUES (1, 'movie', 'bad_imdb', 5555, 'TV')")
            conn.execute("INSERT INTO anime (mal_id, imdb_id, trakt_id, anime_media_type) VALUES (2, 'tt9999999', 5555, 'TV')")
            conn.execute("INSERT INTO anime (mal_id) VALUES (66863)")
            conn.execute("INSERT INTO anime_fts (rowid, mal_title, title_english, aliases_text) VALUES (1, 'A', '', ''), (2, 'B', '', ''), (66863, '', '', '')")
            conn.execute("INSERT INTO franchise_links VALUES (1, 999999, 'SEQUEL', 'anilist')")
            conn.execute("INSERT INTO filler_ranges VALUES (1, 5, 2, 'InvalidType')")
            conn.commit()
            conn.close()

            errs = validate_mappings.validate_database(bad_db, min_rows=1)
            joined = "\n".join(errs)
            self.assertIn("thetvdb_id contains 1 non-integer", joined)
            self.assertIn("malformed non-ttNNNNN", joined)
            self.assertIn("empty orphan rows", joined)
            self.assertIn("dangling mal_id references", joined)
            self.assertIn("Missing/unused index for themoviedb_id", joined)
            self.assertIn("invalid episode bounds", joined)
            self.assertIn("invalid filler_type values", joined)
            self.assertIn("same-kind trakt_id disjoint", joined)

    def test_hydrate_trakt_ids_skips_cached_misses_to_fetch_uncached_backlog(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            cache = build_mappings.ApiCache(tmp)
            cache.set("trakt", 900, 0)
            cache.set("trakt", 800, 4444)
            records = {
                900: build_mappings.empty_record(900) | {"imdb_id": "tt0000900"},
                800: build_mappings.empty_record(800) | {"imdb_id": "tt0000800"},
                801: build_mappings.empty_record(801) | {"imdb_id": "tt0000800", "trakt_id": 4444},
                700: build_mappings.empty_record(700) | {"imdb_id": "tt0000700"},
                701: build_mappings.empty_record(701) | {"imdb_id": "tt0000700"},
            }
            payload = json.dumps([{"show": {"ids": {"trakt": 7777}}}]).encode("utf-8")
            with mock.patch.dict(os.environ, {}, clear=False), \
                 mock.patch.object(build_mappings, "TRAKT_CLIENT_ID", "test_cid"), \
                 mock.patch.object(build_mappings, "_http_get_bytes", return_value=(payload, 200, {})) as mock_get:
                build_mappings.hydrate_trakt_ids(records, cache, max_items=1, delay_seconds=0.0)
            self.assertEqual(records[800]["trakt_id"], 4444)
            self.assertEqual(records[701]["trakt_id"], 7777)
            self.assertEqual(records[700]["trakt_id"], 7777)
            self.assertEqual(mock_get.call_count, 1)

    def test_sanitize_trakt_collisions_respects_namespaces_and_clears_leaks(self):
        records = {
            # Same-IMDb leak across TV & MOVIE (like FLCL) -> clear leaked MOVIE trakt_id
            227: build_mappings.empty_record(227) | {"anime_media_type": "TV", "global_media_type": "show", "imdb_id": "tt0279077", "trakt_id": 5857},
            33010: build_mappings.empty_record(33010) | {"anime_media_type": "MOVIE", "global_media_type": "movie", "imdb_id": "tt0279077", "trakt_id": 5857},
            # Independent TV & MOVIE with different IMDb/TMDb legitimately sharing Trakt namespace #8630
            2814: build_mappings.empty_record(2814) | {"anime_media_type": "TV", "global_media_type": "show", "imdb_id": "tt0414728", "themoviedb_id": 8676, "trakt_id": 8630},
            2264: build_mappings.empty_record(2264) | {"anime_media_type": "MOVIE", "global_media_type": "movie", "imdb_id": "tt0119406", "themoviedb_id": 8853, "trakt_id": 8630},
            # Ungrounded orphan leaking trakt_id from a grounded franchise
            1222: build_mappings.empty_record(1222) | {"anime_media_type": "TV", "global_media_type": "show", "imdb_id": "tt2147761", "trakt_id": 22476},
            8337: build_mappings.empty_record(8337) | {"anime_media_type": "SPECIAL", "global_media_type": "movie", "trakt_id": 22476},
            # 1-vs-1 IMDb tie within 'show' namespace -> TV beats ONA
            4382: build_mappings.empty_record(4382) | {"anime_media_type": "TV", "global_media_type": "show", "imdb_id": "tt0816407", "themoviedb_id": 42511, "trakt_id": 61784},
            5118: build_mappings.empty_record(5118) | {"anime_media_type": "ONA", "global_media_type": "show", "imdb_id": "tt1386581", "themoviedb_id": 95710, "trakt_id": 61784},
            # Disjoint TMDB within 'movie' namespace -> MOVIE beats SPECIAL recap
            1367: build_mappings.empty_record(1367) | {"anime_media_type": "MOVIE", "global_media_type": "movie", "imdb_id": "tt1133559", "themoviedb_id": 30983, "trakt_id": 25439, "trakt_slug": "conan-movie-08"},
            7304: build_mappings.empty_record(7304) | {"anime_media_type": "SPECIAL", "global_media_type": "movie", "imdb_id": "tt1133559", "themoviedb_id": 39203, "trakt_id": 25439, "trakt_slug": "conan-movie-08"},
        }
        cleared = build_mappings.sanitize_trakt_collisions(records)
        self.assertGreaterEqual(cleared, 4)
        self.assertEqual(records[227]["trakt_id"], 5857)
        self.assertIsNone(records[33010]["trakt_id"])
        self.assertEqual(records[2814]["trakt_id"], 8630)
        self.assertEqual(records[2264]["trakt_id"], 8630)
        self.assertEqual(records[1222]["trakt_id"], 22476)
        self.assertIsNone(records[8337]["trakt_id"])
        self.assertEqual(records[4382]["trakt_id"], 61784)
        self.assertIsNone(records[5118]["trakt_id"])
        self.assertEqual(records[1367]["trakt_id"], 25439)
        self.assertIsNone(records[7304]["trakt_id"])
        self.assertIsNone(records[7304]["trakt_slug"])

    def test_hydration_prioritizes_tv_ongoing_finished_over_special_upcoming(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            cache = build_mappings.ApiCache(tmp)
            records = {
                99001: build_mappings.empty_record(99001) | {
                    "anime_media_type": "SPECIAL", "status": "FINISHED", "imdb_id": "tt0099001"
                },
                99002: build_mappings.empty_record(99002) | {
                    "anime_media_type": "TV", "status": "UPCOMING", "imdb_id": "tt0099002"
                },
                99003: build_mappings.empty_record(99003) | {
                    "anime_media_type": "TV", "status": "FINISHED", "imdb_id": "tt0099003"
                },
                99004: build_mappings.empty_record(99004) | {
                    "anime_media_type": "TV", "status": "ONGOING", "imdb_id": "tt0099004"
                },
            }
            payload = json.dumps([{"show": {"ids": {"trakt": 8888}}}]).encode("utf-8")
            with mock.patch.object(build_mappings, "TRAKT_CLIENT_ID", "test_cid"), \
                 mock.patch.object(build_mappings, "_http_get_bytes", return_value=(payload, 200, {})) as mock_get:
                build_mappings.hydrate_trakt_ids(records, cache, max_items=1, delay_seconds=0.0)
            self.assertEqual(mock_get.call_count, 1)
            self.assertIn("tt0099004", mock_get.call_args[0][0])
            self.assertEqual(records[99004]["trakt_id"], 8888)
            self.assertIsNone(records[99001]["trakt_id"])
            self.assertIsNone(records[99002]["trakt_id"])
            self.assertIsNone(records[99003]["trakt_id"])

    def test_v25_trakt_title_year_fallback_and_concurrent_hydration(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            cache = build_mappings.ApiCache(tmp)
            records = {
                61001: build_mappings.empty_record(61001) | {
                    "mal_title": "Brand New Fantasy", "anime_media_type": "TV",
                    "status": "ONGOING", "year": 2025,
                },
                61002: build_mappings.empty_record(61002) | {
                    "mal_title": "Wrong Year Show", "anime_media_type": "TV",
                    "status": "FINISHED", "year": 2025, "thetvdb_id": 444444,
                },
                61003: build_mappings.empty_record(61003) | {
                    "mal_title": "Hero Tales: Season 1", "anime_media_type": "TV",
                    "status": "FINISHED", "year": 2024,
                },
            }

            def _fake_http(url, headers=None, timeout=12, retries=1):
                if "search/show" in url and "Brand%20New%20Fantasy" in url:
                    return json.dumps([
                        {"show": {"title": "Brand New Fantasy", "year": 2025, "ids": {"trakt": 91001, "tvdb": 51001, "tmdb": 61001, "imdb": "tt51001"}}}
                    ]).encode("utf-8"), 200, {}
                if "search/show" in url and "Wrong%20Year%20Show" in url:
                    # Year disagrees (2020) and TVDB disagrees (999999) -> must reject
                    return json.dumps([
                        {"show": {"title": "Wrong Year Show", "year": 2020, "ids": {"trakt": 99999, "tvdb": 999999}}}
                    ]).encode("utf-8"), 200, {}
                if "api.ani.zip" in url and "61003" in url:
                    return json.dumps({"mappings": {"thetvdb_id": 77703, "themoviedb_id": 88803}, "episodeCount": 12}).encode("utf-8"), 200, {}
                if url.endswith("/shows"):
                    return b'<a href="/shows/hero-tales">Hero Tales</a>', 200, {}
                if url.endswith("/shows/hero-tales"):
                    return (
                        b'<h1>Hero Tales Filler List</h1>'
                        b'<table class="EpisodeList"><tr><td class="Number">4</td>'
                        b'<td class="Type"><span>Filler</span></td></tr></table>'
                    ), 200, {}
                return b"{}", 200, {}

            with mock.patch.object(build_mappings, "TRAKT_CLIENT_ID", "test_cid"), \
                 mock.patch.object(build_mappings, "SIMKL_CLIENT_ID", ""), \
                 mock.patch.object(build_mappings, "_http_get_bytes", side_effect=_fake_http):
                ep_counts = {61003: 12}
                build_mappings.hydrate_anizip_and_malsync(records, cache, ep_counts, max_items=10)
                self.assertEqual(records[61003]["thetvdb_id"], 77703)
                self.assertEqual(records[61003]["themoviedb_id"], 88803)

                build_mappings.hydrate_trakt_ids(records, cache, max_items=10, delay_seconds=0.0)
                self.assertEqual(records[61001]["trakt_id"], 91001)
                self.assertEqual(records[61001]["thetvdb_id"], 51001)
                self.assertEqual(records[61001]["imdb_id"], "tt51001")
                self.assertIsNone(records[61002]["trakt_id"])

                fr, matched = build_mappings.hydrate_and_map_filler_ranges(
                    records, {}, {}, cache, ep_counts,
                )
                self.assertEqual(matched, 1)
                self.assertEqual(fr, [(61003, 4, 4, "Filler")])

    def test_v26_english_trakt_fallback_pab_rules_and_afl_dedup(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            cache = build_mappings.ApiCache(tmp)
            records = {
                62001: build_mappings.empty_record(62001) | {
                    "mal_title": "Ore dake Level Up",
                    "title_english": "Solo Fantasy",
                    "anime_media_type": "TV",
                    "status": "FINISHED",
                    "year": 2025,
                },
                62002: build_mappings.empty_record(62002) | {
                    "mal_title": "Multi Season PAB",
                    "anime_media_type": "TV",
                    "status": "FINISHED",
                },
            }
            pab_rules = []
            pab_blob = json.dumps({
                "1": {
                    "mal_id": 62002,
                    "tvdb_id": 888999,
                    "tvdb_mappings": {"s0": "e1", "s1": "e1-e12", "s2": "e1-e13"},
                }
            }).encode("utf-8")
            build_mappings.merge_plexanibridge(records, pab_blob, rules=pab_rules)
            self.assertEqual(len(pab_rules), 2)
            self.assertEqual(pab_rules[0]["season"], 1)
            self.assertEqual(pab_rules[1]["ep_start"], 13)

            def _fake_http(url, headers=None, timeout=12, retries=1):
                if "query=Solo%20Fantasy" in url:
                    return json.dumps([
                        {"show": {"title": "Solo Fantasy", "year": 2025, "ids": {"trakt": 92001, "slug": "solo-fantasy", "tvdb": 52001}}}
                    ]).encode("utf-8"), 200, {}
                return b"[]", 200, {}

            with mock.patch.object(build_mappings, "TRAKT_CLIENT_ID", "test_cid"), \
                 mock.patch.object(build_mappings, "_http_get_bytes", side_effect=_fake_http):
                build_mappings.hydrate_trakt_ids(records, cache, max_items=10, delay_seconds=0.0)
            self.assertEqual(records[62001]["trakt_id"], 92001)
            self.assertEqual(records[62001]["trakt_slug"], "solo-fantasy")

            # AFL canonical slug deduplication (non-variant base beats -definitive-filler-list)
            shows = [
                {"slug": "solo-fantasy-definitive-filler-list", "title": "Solo Fantasy", "max_ep": 12, "episodes": {"2": "Recap"}},
                {"slug": "solo-fantasy", "title": "Solo Fantasy", "max_ep": 12, "episodes": {"2": "Filler", "3": "Filler"}},
            ]
            fr, matched = build_mappings._map_afl_shows_to_ranges(shows, records, {}, {}, {62001: 12})
            self.assertEqual(matched, 1)
            self.assertEqual(fr, [(62001, 2, 3, "Filler")])


if __name__ == "__main__":
    unittest.main()
