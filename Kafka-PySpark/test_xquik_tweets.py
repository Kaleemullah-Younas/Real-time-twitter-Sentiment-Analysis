import tempfile
import unittest
from pathlib import Path

from xquik_tweets import iter_xquik_jsonl, normalize_xquik_record


class XquikTweetTests(unittest.TestCase):
    def test_normalize_maps_xquik_record_to_pipeline_row(self):
        row = normalize_xquik_record(
            {"id": "187", "username": "xquik", "text": "Streaming source works"}
        )

        self.assertEqual(row, ["187", "xquik", "Unknown", "Streaming source works"])

    def test_jsonl_reader_rejects_missing_text(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "tweets.jsonl"
            path.write_text('{"id":"187"}\n', encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "missing text"):
                list(iter_xquik_jsonl(path))


if __name__ == "__main__":
    unittest.main()
