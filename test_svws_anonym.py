#!/usr/bin/env python3
"""
Unit tests for SVWS Anonymization Tool
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add parent directory to path for importing svws_anonym module
# This allows the test to run standalone without package installation
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from svws_anonym import SVWSAnonymizer


class TestSVWSAnonymizer(unittest.TestCase):
    """Test cases for SVWSAnonymizer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.anonymizer = SVWSAnonymizer(
            host='localhost',
            port=3306,
            user='test_user',
            password='test_password'
        )

    def test_initialization(self):
        """Test that anonymizer initializes correctly."""
        self.assertEqual(self.anonymizer.host, 'localhost')
        self.assertEqual(self.anonymizer.port, 3306)
        self.assertEqual(self.anonymizer.user, 'test_user')
        self.assertEqual(self.anonymizer.password, 'test_password')
        self.assertIsNone(self.anonymizer.connection)
        self.assertIsNotNone(self.anonymizer.fake)

    def test_anonymization_rules(self):
        """Test that anonymization rules are defined correctly."""
        rules = self.anonymizer._get_anonymization_rules()
        
        # Check that key patterns exist
        self.assertIn('nachname', rules)
        self.assertIn('vorname', rules)
        self.assertIn('email', rules)
        self.assertIn('telefon', rules)
        self.assertIn('strasse', rules)
        self.assertIn('plz', rules)
        self.assertIn('geburtsdatum', rules)
        
        # Check that rules return callable functions
        for pattern, func in rules.items():
            self.assertTrue(callable(func), f"Rule for '{pattern}' should be callable")

    def test_anonymization_rules_generate_data(self):
        """Test that anonymization rules generate appropriate data."""
        rules = self.anonymizer._get_anonymization_rules()
        
        # Test name generation
        nachname = rules['nachname']()
        self.assertIsInstance(nachname, str)
        self.assertGreater(len(nachname), 0)
        
        vorname = rules['vorname']()
        self.assertIsInstance(vorname, str)
        self.assertGreater(len(vorname), 0)
        
        # Test email generation
        email = rules['email']()
        self.assertIsInstance(email, str)
        self.assertIn('@', email)
        
        # Test PLZ generation
        plz = rules['plz']()
        self.assertIsInstance(plz, str)
        
        # Test city generation
        ort = rules['ort']()
        self.assertIsInstance(ort, str)
        self.assertGreater(len(ort), 0)

    @patch('pymysql.connect')
    def test_connect_success(self, mock_connect):
        """Test successful database connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        self.anonymizer.connect()
        
        mock_connect.assert_called_once_with(
            host='localhost',
            port=3306,
            user='test_user',
            password='test_password',
            charset='utf8mb4',
            cursorclass=unittest.mock.ANY
        )
        self.assertEqual(self.anonymizer.connection, mock_connection)

    @patch('pymysql.connect')
    def test_connect_failure(self, mock_connect):
        """Test database connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        with self.assertRaises(SystemExit):
            self.anonymizer.connect()

    def test_disconnect(self):
        """Test database disconnection."""
        mock_connection = MagicMock()
        self.anonymizer.connection = mock_connection
        
        self.anonymizer.disconnect()
        
        mock_connection.close.assert_called_once()

    @patch('pymysql.connect')
    def test_list_schemas(self, mock_connect):
        """Test listing database schemas."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock database list
        mock_cursor.fetchall.return_value = [
            {'Database': 'information_schema'},
            {'Database': 'mysql'},
            {'Database': 'schule_test'},
            {'Database': 'schule_demo'},
        ]
        
        self.anonymizer.connection = mock_connection
        schemas = self.anonymizer.list_schemas()
        
        # Should filter out system databases
        self.assertEqual(len(schemas), 2)
        self.assertIn('schule_test', schemas)
        self.assertIn('schule_demo', schemas)
        self.assertNotIn('mysql', schemas)
        self.assertNotIn('information_schema', schemas)

    @patch('pymysql.connect')
    def test_get_tables(self, mock_connect):
        """Test getting tables from a schema."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock table list
        mock_cursor.fetchall.return_value = [
            {'Tables_in_test_schema': 'schueler'},
            {'Tables_in_test_schema': 'lehrer'},
            {'Tables_in_test_schema': 'klassen'},
        ]
        
        self.anonymizer.connection = mock_connection
        tables = self.anonymizer.get_tables('test_schema')
        
        self.assertEqual(len(tables), 3)
        self.assertIn('schueler', tables)
        self.assertIn('lehrer', tables)
        self.assertIn('klassen', tables)

    def test_faker_german_locale(self):
        """Test that Faker uses German locale correctly."""
        # Generate multiple samples to check German characteristics
        for _ in range(10):
            name = self.anonymizer.fake.last_name()
            self.assertIsInstance(name, str)
            self.assertGreater(len(name), 0)
            
            city = self.anonymizer.fake.city()
            self.assertIsInstance(city, str)
            self.assertGreater(len(city), 0)
            
            plz = self.anonymizer.fake.postcode()
            self.assertIsInstance(plz, str)
            # German postal codes are 5 digits
            self.assertEqual(len(plz), 5)
            self.assertTrue(plz.isdigit())


class TestDataGeneration(unittest.TestCase):
    """Test data generation quality."""

    def setUp(self):
        """Set up test fixtures."""
        self.anonymizer = SVWSAnonymizer(
            host='localhost',
            port=3306,
            user='test',
            password='test'
        )

    def test_unique_data_generation(self):
        """Test that generated data is sufficiently varied."""
        rules = self.anonymizer._get_anonymization_rules()
        
        # Generate multiple values and ensure they're not all identical
        nachnamen = set(rules['nachname']() for _ in range(10))
        self.assertGreater(len(nachnamen), 5, "Should generate varied surnames")
        
        emails = set(rules['email']() for _ in range(10))
        self.assertGreater(len(emails), 5, "Should generate varied emails")
        
        orte = set(rules['ort']() for _ in range(10))
        self.assertGreater(len(orte), 3, "Should generate varied cities")


if __name__ == '__main__':
    unittest.main()
