#!/usr/bin/env python3
"""
Unit tests for SVWS-Anonym
"""

import unittest
from pathlib import Path
from svws_anonym import NameAnonymizer


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
        self.assertIn(original, self.anonymizer.name_mapping)
        self.assertEqual(self.anonymizer.name_mapping[original], result1)
        
        # Anonymize again and verify consistency
        result2 = self.anonymizer.anonymize_lastname(original)
        self.assertEqual(result1, result2)


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


if __name__ == "__main__":
    unittest.main()
