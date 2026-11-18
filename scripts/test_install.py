import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SERVICE = "relayd"
BINARIES = "relayd".split()
CONFIG = "service.env"


class InstallTest(unittest.TestCase):
    def test_isolated_install_and_config_preservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            source.mkdir()
            for directory in ("scripts", "packaging"):
                shutil.copytree(ROOT / directory, source / directory)
            (source / "bin").mkdir()
            for binary in BINARIES:
                (source / "bin" / binary).write_text("fake artifact")
            stage = Path(tmp) / "stage"
            env = dict(os.environ, DESTDIR=str(stage))
            command = ["sh", str(source / "scripts/install.sh")]
            subprocess.run(command, env=env, check=True, capture_output=True)
            self.assertEqual({p.name for p in (stage / "usr/local/bin").iterdir()}, set(BINARIES))
            self.assertEqual({p.name for p in (stage / "etc/systemd/system").iterdir()}, {SERVICE + ".service"})
            config = stage / "etc" / SERVICE / CONFIG
            config.write_text("custom settings")
            subprocess.run(command, env=env, check=True, capture_output=True)
            self.assertEqual(config.read_text(), "custom settings")
            self.assertTrue(Path(str(config) + ".example").is_file())


if __name__ == "__main__":
    unittest.main()
