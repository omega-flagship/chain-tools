import base64
import importlib
import importlib.util
import os
from pathlib import Path
import shutil
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "modules/chain_observer"
HIVE = Path(os.environ.get("HIVE_CHECKOUT", ROOT.parent / "omegahive"))
PETTA = Path(os.environ.get("PETTA_PATH", HIVE / ".runtime/PeTTa"))


class ModuleTests(unittest.TestCase):
    def test_manifest_and_imports_agree(self):
        manifest = tomllib.loads((MODULE / "module.toml").read_text())
        entry = (MODULE / manifest["entrypoint"]).read_text()
        self.assertIn(f'(Module {manifest["id"]})', entry)
        self.assertIn(f'(ModuleVersion {manifest["id"]} "{manifest["version"]}")', entry)
        for skill in manifest["provides"]:
            self.assertIn(f'(Provides {manifest["id"]} (Skill {skill.split(":")[1]}))', entry)
        for name in ("catalog.metta", "signatures.metta", "skills.metta", "affordance.metta", "src/chain_observer.py"):
            self.assertTrue((MODULE / name).is_file())
            self.assertIn(f'./modules/chain_observer/{name}', entry)

    @unittest.skipUnless((HIVE / "hive/runtime/extensions/helper_command_parser.py").is_file()
                         and (PETTA / "src/main.pl").is_file() and shutil.which("swipl")
                         and importlib.util.find_spec("janus_swi") is not None,
                         "requires the local Hive and PeTTa runtimes")
    def test_hive_parser_and_petta_preserve_rholang_bytes(self):
        sys.path.insert(0, str(HIVE))
        sys.path.insert(0, str(PETTA / "python"))
        sys.path.insert(0, str(MODULE / "src"))
        parser = importlib.import_module("hive.runtime.extensions.helper_command_parser")
        bridge = importlib.import_module("chain_observer")
        from petta import PeTTa

        source = 'new return in {\n  return!({"quote": "a\\\"b", "path": "C:\\\\tmp", "text": "λ"})\n}\n'
        exact_source = source.replace('"λ"', '"λ _quote_ _newline_"')
        captured = []
        with tempfile.TemporaryDirectory() as scratch:
            modules = Path(scratch) / "modules"
            shutil.copytree(MODULE, modules / "chain_observer", ignore=shutil.ignore_patterns("__pycache__"))
            (modules / "loader.metta").write_text('!(import! &self (library OmegaClaw-Core ./modules/chain_observer/entry.metta))\n')
            try:
                with patch.object(parser, "MODULE_DECLARATIONS_ROOT", modules):
                    parser.reload_signature_commands()
                    parsed = parser.signature_balance_parentheses("chain-query <<'RHO'\n" + source + "RHO")
                    encoded = base64.b64encode(exact_source.encode()).decode()
                    exact = parser.signature_balance_parentheses("chain-query-base64 " + encoded)
                    self.assertIn("chain-query", parsed)
                    self.assertIn("chain-query", parser.skill_help("chain"))
                    with patch.object(bridge, "query_state", side_effect=lambda value: captured.append(value) or "CAPTURED"):
                        engine = PeTTa(petta_path=str(PETTA))
                        engine.process_metta_string((MODULE / "skills.metta").read_text())
                        result = engine.process_metta_string("!" + parsed)
                        self.assertTrue(result)
                        self.assertTrue(engine.process_metta_string("!" + exact))
                    self.assertEqual(captured, [source, exact_source])
            finally:
                parser.reload_signature_commands()


if __name__ == "__main__":
    unittest.main()
