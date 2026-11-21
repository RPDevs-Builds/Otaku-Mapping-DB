#!/usr/bin/env python3
"""Hermetic offline unit tests for Otaku-Mapping-DB builder and validator."""

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
                    "picture": "https://cdn.myanimelist.net/images/anime/12/34567.jpg",
                    "duration": {"value": 1440, "unit": "SECONDS"},
                    "score": {"median": 8.456},
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
        ]
        with open(os.path.join(fdir, "fribb.json"), "w", encoding="utf-8") as f:
            json.dump(fribb, f)

        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<anime-list>
  <anime anidbid="102" tvdbid="777001" defaulttvdbseason="1" tmdbtv="888001" imdbid="tt1234567">
    <name>Test Saga Season 1</name>
    <mapping-list>
      <mapping anidbseason="1" tvdbseason="0">;13-1;</mapping>
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
            self.assertEqual(manifest["total_rows"], 4)  # seed 21 + 100, 200, 300
            self.assertGreaterEqual(manifest["rule_count"], 2)

            errs = validate_mappings.validate_database(
                out_db,
                baseline_path=seed_p,
                xz_path=f"{out_db}.xz",
                manifest_path=manifest_p,
                min_rows=4,
            )
            self.assertEqual(errs, [], f"Validation unexpectedly failed: {errs}")

            conn = sqlite3.connect(out_db)
            conn.row_factory = sqlite3.Row
            try:
                # 1. Seed trakt_id preserved & dirty '' imdb_id cleaned to NULL
                r21 = conn.execute("SELECT * FROM anime WHERE mal_id=21").fetchone()
                self.assertEqual(r21["trakt_id"], 37696)
                self.assertIsNone(r21["imdb_id"])
                self.assertEqual(r21["thetvdb_season"], "a")

                # 2. Manami picture stripped to relative suffix + score rounded + dub populated
                r100 = conn.execute("SELECT * FROM anime WHERE mal_id=100").fetchone()
                self.assertEqual(r100["mal_picture"], "12/34567.jpg")
                self.assertEqual(r100["score"], 8.46)
                self.assertEqual(r100["mal_dub_id"], 100)
                self.assertEqual(r100["thetvdb_id"], 777001)

                # 3. Direct AniList PREQUEL inheritance populated mal_id=200 from 100
                r200 = conn.execute("SELECT * FROM anime WHERE mal_id=200").fetchone()
                self.assertEqual(r200["thetvdb_id"], 777001)
                self.assertEqual(r200["themoviedb_id"], 888001)
                self.assertEqual(r200["imdb_id"], "tt1234567")
                self.assertEqual(r200["thetvdb_season"], 2)

                # 4. Split-cour episode offset applied & UNIQUE anilist_id collision safely resolved
                r300 = conn.execute("SELECT * FROM anime WHERE mal_id=300").fetchone()
                self.assertEqual(r300["global_media_episodes"], "13 - 25")
                self.assertIsNone(r300["anilist_id"])  # 201 already owned by 200

                # 5. tvdb_rules table populated from both XML mapping-list & MALSync rules
                rules = conn.execute("SELECT * FROM tvdb_rules ORDER BY mal_id, rule_type").fetchall()
                sources = {r["source"] for r in rules}
                self.assertIn("anime_lists_xml", sources)
                self.assertIn("malsync_rules", sources)
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
                "global_media_type TEXT, anime_media_episodes TEXT, global_media_episodes TEXT)"
            )
            conn.execute("CREATE TABLE activities (sync_id INTEGER PRIMARY KEY, last_updated TEXT NOT NULL)")
            conn.execute("CREATE TABLE tvdb_rules (mal_id INTEGER, tvdb_id INTEGER, season INTEGER, rule_type TEXT, range_start INTEGER, range_end INTEGER, ep_start INTEGER, absolute_start INTEGER, source TEXT)")
            conn.execute("CREATE TABLE build_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            conn.execute("INSERT INTO anime (mal_id, thetvdb_id, imdb_id) VALUES (1, 'movie', 'bad_imdb')")
            conn.commit()
            conn.close()

            errs = validate_mappings.validate_database(bad_db, min_rows=1)
            joined = "\n".join(errs)
            self.assertIn("thetvdb_id contains 1 non-integer", joined)
            self.assertIn("malformed non-ttNNNNN", joined)
            self.assertIn("Missing/unused index for themoviedb_id", joined)


if __name__ == "__main__":
    unittest.main()
