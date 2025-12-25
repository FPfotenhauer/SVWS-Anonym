#!/usr/bin/env python3
"""
Example usage of SVWS-Anonym
"""

from svws_anonym import NameAnonymizer


def main():
    print("SVWS-Anonym Beispiel / Example\n")
    print("=" * 50)
    
    # Initialize anonymizer
    anonymizer = NameAnonymizer()
    
    # Example data - typical German names
    test_data = [
        ("Max", "Mustermann", "m"),
        ("Erika", "Musterfrau", "w"),
        ("Hans", "Schmidt", "m"),
        ("Anna", "Müller", "w"),
        ("Peter", "Meyer", "m"),
        ("Maria", "Schneider", "w"),
        ("Max", "Mustermann", "m"),  # Same as first - should get same result
    ]
    
    print("\nAnonymisierung von Namen / Name Anonymization:")
    print("-" * 50)
    
    for firstname, lastname, gender in test_data:
        anon_first, anon_last = anonymizer.anonymize_fullname(
            firstname, lastname, gender
        )
        print(f"{firstname} {lastname} -> {anon_first} {anon_last}")
    
    print("\n" + "=" * 50)
    print("Hinweis: Gleiche Namen werden konsistent zugeordnet")
    print("Note: Same names are mapped consistently")


if __name__ == "__main__":
    main()
