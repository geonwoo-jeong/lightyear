import tempfile
import unittest
from pathlib import Path

from compare import check_work, classify, launch_spec, require_new_output


class EvidenceChecks(unittest.TestCase):
    def test_launch_argv0_and_working_directory_do_not_depend_on_binary_path(self):
        baseline = launch_spec(Path("/one/baseline"), "transport-many", 2000, True, False)
        candidate = launch_spec(Path("/different/path/candidate"), "transport-many", 2000, True, False)
        self.assertEqual(baseline["args"], candidate["args"])
        self.assertEqual(baseline["args"][0], "performance-audit")
        self.assertEqual(baseline["cwd"], candidate["cwd"])
        self.assertNotEqual(baseline["executable"], candidate["executable"])

    def test_mismatched_work_is_rejected(self):
        left = {"result": dict(scenario="udp-single", iterations=3, packets=48, bytes=24576, messages=0, checksum=24576)}
        right = {"result": dict(left["result"], packets=47)}
        check_work(left, left)
        with self.assertRaisesRegex(ValueError, "packets"):
            check_work(left, right)

    def test_existing_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            require_new_output(output)
            output.write_text("original evidence")
            with self.assertRaises(FileExistsError):
                require_new_output(output)
            self.assertEqual(output.read_text(), "original evidence")

    def test_threshold_verdict_uses_confidence_interval(self):
        self.assertEqual(classify(.99, 1.05), "95% interval upper bound within 5% threshold")
        self.assertEqual(classify(1.06, 1.10), "regression above 5% supported")
        self.assertEqual(classify(.99, 1.06), "inconclusive against 5% threshold")


if __name__ == "__main__":
    unittest.main()
