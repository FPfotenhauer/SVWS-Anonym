#!/usr/bin/env python3
"""
Unit tests for SVWS-Anonym
"""

import unittest
from pathlib import Path
from svws_anonym import NameAnonymizer
from svws_anonym import DatabaseAnonymizer, DatabaseConfig


class FakeCursor:
    def __init__(self, dictionary=False, script=None, recorder=None):
        self.dictionary = dictionary
        self.script = script or []
        self.recorder = recorder if recorder is not None else {}
        self._last_query = None
        self._fetch_queue = []

    def queue_fetchone(self, value):
        self._fetch_queue.append(("one", value))

    def queue_fetchall(self, value):
        self._fetch_queue.append(("all", value))

    def execute(self, query, params=None):
        self._last_query = (query, params)
        # Record deletes and inserts for assertions
        if query.strip().upper().startswith("DELETE FROM"):
            table = query.strip().split()[2]
            self.recorder.setdefault("deleted", []).append(table)
        if query.strip().upper().startswith("INSERT INTO"):
            self.recorder.setdefault("insert", []).append((query, params))

        # Simple scripted responses based on query
        if "SHOW TABLES LIKE" in query:
            # Return truthy to indicate table exists
            self.queue_fetchone({"exists": True})
        elif "SELECT COUNT(*) as count FROM" in query:
            # Extract table name
            table = query.split("FROM")[1].strip()
            count = 0
            if self.script and "counts" in self.script:
                count = self.script["counts"].get(table, 0)
            self.queue_fetchone({"count": count})
        elif "SELECT SchulNr FROM EigeneSchule" in query:
            self.queue_fetchone({"SchulNr": 123456})

    def fetchone(self):
        if not self._fetch_queue:
            return None
        kind, value = self._fetch_queue.pop(0)
        return value

    def fetchall(self):
        if not self._fetch_queue:
            return []
        kind, value = self._fetch_queue.pop(0)
        return value if kind == "all" else []

    def close(self):
        pass


class FakeConnection:
    def __init__(self, script=None, recorder=None):
        self.script = script or {}
        self.recorder = recorder if recorder is not None else {}
        self._connected = True

    def is_connected(self):
        return self._connected

    def cursor(self, dictionary=False):
        return FakeCursor(dictionary=dictionary, script=self.script, recorder=self.recorder)

    def commit(self):
        self.recorder["committed"] = True

    def rollback(self):
        self.recorder["rolled_back"] = True


class DummyConfig:
    def get_connection_params(self):
        return {}


class TestNameAnonymizer(unittest.TestCase):
    """Test cases for the NameAnonymizer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.anonymizer = NameAnonymizer()
    
    def test_initialization(self):
        """Test that the anonymizer initializes correctly."""
        self.assertIsNotNone(self.anonymizer.nachnamen)
        self.assertIsNotNone(self.anonymizer.vornamen_m)
        self.assertIsNotNone(self.anonymizer.vornamen_w)
        self.assertGreater(len(self.anonymizer.nachnamen), 0)
        self.assertGreater(len(self.anonymizer.vornamen_m), 0)
        self.assertGreater(len(self.anonymizer.vornamen_w), 0)
    
    def test_anonymize_lastname(self):
        """Test last name anonymization."""
        result = self.anonymizer.anonymize_lastname("Mustermann")
        self.assertIsNotNone(result)
        self.assertNotEqual(result, "")
        self.assertIn(result, self.anonymizer.nachnamen)
    
    def test_anonymize_firstname_male(self):
        """Test male first name anonymization."""
        result = self.anonymizer.anonymize_firstname("Max", gender='m')
        self.assertIsNotNone(result)
        self.assertNotEqual(result, "")
        self.assertIn(result, self.anonymizer.vornamen_m)
    
    def test_anonymize_firstname_female(self):
        """Test female first name anonymization."""
        result = self.anonymizer.anonymize_firstname("Erika", gender='w')
        self.assertIsNotNone(result)
        self.assertNotEqual(result, "")
        self.assertIn(result, self.anonymizer.vornamen_w)
    
    def test_anonymize_firstname_random(self):
        """Test random gender first name anonymization."""
        result = self.anonymizer.anonymize_firstname("Alex", gender=None)
        self.assertIsNotNone(result)
        self.assertNotEqual(result, "")
        # Should be in either male or female list
        self.assertTrue(
            result in self.anonymizer.vornamen_m or 
            result in self.anonymizer.vornamen_w
        )
    
    def test_consistency(self):
        """Test that the same name always maps to the same anonymized name."""
        result1 = self.anonymizer.anonymize_lastname("Schmidt")
        result2 = self.anonymizer.anonymize_lastname("Schmidt")
        self.assertEqual(result1, result2)
        
        result3 = self.anonymizer.anonymize_firstname("Hans", gender='m')
        result4 = self.anonymizer.anonymize_firstname("Hans", gender='m')
        self.assertEqual(result3, result4)
    
    def test_anonymize_fullname(self):
        """Test full name anonymization."""
        first, last = self.anonymizer.anonymize_fullname("Max", "Mustermann", gender='m')
        self.assertIsNotNone(first)
        self.assertIsNotNone(last)
        self.assertIn(first, self.anonymizer.vornamen_m)
        self.assertIn(last, self.anonymizer.nachnamen)
    
    def test_empty_name(self):
        """Test handling of empty names."""
        result = self.anonymizer.anonymize_lastname("")
        self.assertEqual(result, "")
        
        result = self.anonymizer.anonymize_firstname("", gender='m')
        self.assertEqual(result, "")
    
    def test_name_mapping_persistence(self):
        """Test that name mappings are stored correctly."""
        # Anonymize a name
        original = "TestName"
        result1 = self.anonymizer.anonymize_lastname(original)
        
        # Check it's in the mapping
        self.assertIn(original, self.anonymizer.lastname_mapping)
        self.assertEqual(self.anonymizer.lastname_mapping[original], result1)
        
        # Anonymize again and verify consistency
        result2 = self.anonymizer.anonymize_lastname(original)
        self.assertEqual(result1, result2)

    def test_firstname_mapping_persistence(self):
        """Test that firstname mappings are stored correctly for gendered keys."""
        original = "TestFirst"
        # Male
        res_m1 = self.anonymizer.anonymize_firstname(original, gender='m')
        self.assertIn((original, 'm'), self.anonymizer.firstname_mapping)
        self.assertEqual(self.anonymizer.firstname_mapping[(original, 'm')], res_m1)
        res_m2 = self.anonymizer.anonymize_firstname(original, gender='m')
        self.assertEqual(res_m1, res_m2)
        # Female should map independently
        res_w1 = self.anonymizer.anonymize_firstname(original, gender='w')
        self.assertIn((original, 'w'), self.anonymizer.firstname_mapping)
        self.assertEqual(self.anonymizer.firstname_mapping[(original, 'w')], res_w1)
        res_w2 = self.anonymizer.anonymize_firstname(original, gender='w')
        self.assertEqual(res_w1, res_w2)
        # Ensure male and female mappings can differ
        self.assertIn(res_m1, self.anonymizer.vornamen_m)
        self.assertIn(res_w1, self.anonymizer.vornamen_w)


class TestNameLists(unittest.TestCase):
    """Test cases for the name list files."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.data_dir = Path(__file__).parent
    
    def test_json_files_exist(self):
        """Test that all required JSON files exist."""
        self.assertTrue((self.data_dir / "nachnamen.json").exists())
        self.assertTrue((self.data_dir / "vornamen_m.json").exists())
        self.assertTrue((self.data_dir / "vornamen_w.json").exists())
    
    def test_json_files_not_empty(self):
        """Test that JSON files are not empty."""
        import json
        
        with open(self.data_dir / "nachnamen.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertIsInstance(data, list)
            self.assertGreater(len(data), 0)
        
        with open(self.data_dir / "vornamen_m.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertIsInstance(data, list)
            self.assertGreater(len(data), 0)
        
        with open(self.data_dir / "vornamen_w.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertIsInstance(data, list)
            self.assertGreater(len(data), 0)


class TestAdminCleanup(unittest.TestCase):
    """Mock-based tests for admin tables cleanup."""

    def setUp(self):
        # Ensure DatabaseAnonymizer can be instantiated without mysql connector
        import svws_anonym as sa
        sa.MYSQL_AVAILABLE = True
        self.anonymizer = NameAnonymizer()
        self.db = DatabaseAnonymizer(DummyConfig(), self.anonymizer)

    def test_delete_general_admin_tables_recreates_admin(self):
        # Prepare fake connection with counts
        counts = {
            "Schild_Verwaltung": 2,
            "Client_Konfiguration_Global": 0,
            "Client_Konfiguration_Benutzer": 1,
            "Wiedervorlage": 3,
            "ZuordnungReportvorlagen": 0,
            "BenutzerEmail": 1,
            "ImpExp_EigeneImporte": 0,
            "ImpExp_EigeneImporte_Felder": 2,
            "ImpExp_EigeneImporte_Tabellen": 2,
            "SchuleOAuthSecrets": 1,
            "Logins": 5,
            "TextExportVorlagen": 0,
            "Credentials": 4,
            "BenutzerAllgemein": 4,
            "Benutzer": 4,
        }
        recorder = {}
        self.db.connection = FakeConnection(script={"counts": counts}, recorder=recorder)

        total_deleted = self.db.delete_general_admin_tables(dry_run=False)

        # Sum of non-special target counts plus special table counts
        expected_deleted = sum([
            counts["Schild_Verwaltung"],
            counts["Client_Konfiguration_Benutzer"],
            counts["Wiedervorlage"],
            counts["BenutzerEmail"],
            counts["ImpExp_EigeneImporte_Felder"],
            counts["ImpExp_EigeneImporte_Tabellen"],
            counts["SchuleOAuthSecrets"],
            counts["Logins"],
        ]) + counts["Credentials"] + counts["BenutzerAllgemein"] + counts["Benutzer"]

        self.assertEqual(total_deleted, expected_deleted)
        # Ensure admin entries were recreated (INSERT called for each special table)
        inserts = recorder.get("insert", [])
        self.assertTrue(any("INSERT INTO Credentials" in q for q, _ in inserts))
        self.assertTrue(any("INSERT INTO BenutzerAllgemein" in q for q, _ in inserts))
        self.assertTrue(any("INSERT INTO Benutzer (ID, Typ" in q for q, _ in inserts))
        # Ensure commit occurred
        self.assertTrue(recorder.get("committed", False))


class TestSchuleCredentialsReset(unittest.TestCase):
    """Mock-based test for SchuleCredentials reset with key generation."""

    def setUp(self):
        import svws_anonym as sa
        sa.MYSQL_AVAILABLE = True
        sa.CRYPTOGRAPHY_AVAILABLE = True
        self.anonymizer = NameAnonymizer()
        self.db = DatabaseAnonymizer(DummyConfig(), self.anonymizer)

    def test_reset_schule_credentials_inserts_keys_without_headers(self):
        counts = {"SchuleCredentials": 1}
        recorder = {}
        self.db.connection = FakeConnection(script={"counts": counts}, recorder=recorder)

        deleted_count = self.db.reset_schule_credentials(dry_run=False)
        self.assertEqual(deleted_count, 1)

        # Verify delete and insert occurred
        deleted_tables = recorder.get("deleted", [])
        self.assertIn("SchuleCredentials", deleted_tables)
        inserts = recorder.get("insert", [])
        # Expect one insert into SchuleCredentials
        sc_inserts = [(q, p) for (q, p) in inserts if "INSERT INTO SchuleCredentials" in q]
        self.assertEqual(len(sc_inserts), 1)
        query, params = sc_inserts[0]
        self.assertIsNotNone(params)
        self.assertEqual(len(params), 4)
        schulnr, public_pem, private_pem, aes_b64 = params
        self.assertEqual(schulnr, 123456)
        # Ensure PEM headers are stripped
        self.assertNotIn("-----BEGIN", public_pem or "")
        self.assertNotIn("-----END", public_pem or "")
        self.assertNotIn("-----BEGIN", private_pem or "")
        self.assertNotIn("-----END", private_pem or "")
        # AES base64 should be 44 chars for 32-byte key
        self.assertIsInstance(aes_b64, str)
        self.assertEqual(len(aes_b64), 44)
        # Commit occurred
        self.assertTrue(recorder.get("committed", False))


if __name__ == "__main__":
    unittest.main()
