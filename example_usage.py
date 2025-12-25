#!/usr/bin/env python3
"""
Example script demonstrating SVWS Anonymization Tool usage without user input.
This is useful for automated testing or scripting scenarios.
"""

import sys
from svws_anonym import SVWSAnonymizer


def example_usage():
    """
    Demonstrate how to use the SVWSAnonymizer programmatically.
    
    Note: This example uses dummy credentials and won't actually connect.
    For real usage, replace with actual database credentials.
    """
    print("=" * 60)
    print("SVWS Anonymizer - Programmatic Usage Example")
    print("=" * 60)
    print()
    
    # Example 1: Initialize the anonymizer
    print("1. Creating anonymizer instance...")
    anonymizer = SVWSAnonymizer(
        host='localhost',
        port=3306,
        user='your_username',
        password='your_password'
    )
    print("   ✓ Anonymizer created")
    print()
    
    # Example 2: Show anonymization rules
    print("2. Available anonymization rules:")
    rules = anonymizer._get_anonymization_rules()
    for i, pattern in enumerate(sorted(rules.keys())[:10], 1):
        print(f"   {i}. {pattern}")
    print(f"   ... and {len(rules) - 10} more patterns")
    print()
    
    # Example 3: Generate sample anonymized data
    print("3. Sample generated data:")
    print(f"   Nachname: {rules['nachname']()}")
    print(f"   Vorname: {rules['vorname']()}")
    print(f"   E-Mail: {rules['email']()}")
    print(f"   Telefon: {rules['telefon']()}")
    print(f"   Straße: {rules['strasse']()}")
    print(f"   Hausnummer: {rules['hausnummer']()}")
    print(f"   PLZ: {rules['plz']()}")
    print(f"   Ort: {rules['ort']()}")
    print()
    
    # Example 4: Programmatic usage (without actual connection)
    print("4. Programmatic workflow:")
    print("   - Initialize anonymizer with credentials")
    print("   - Call anonymizer.connect()")
    print("   - Get schemas with anonymizer.list_schemas()")
    print("   - Anonymize with anonymizer.anonymize_schema(schema_name)")
    print("   - Call anonymizer.disconnect()")
    print()
    
    print("=" * 60)
    print("For actual usage, run: python3 svws_anonym.py")
    print("=" * 60)


if __name__ == "__main__":
    example_usage()
