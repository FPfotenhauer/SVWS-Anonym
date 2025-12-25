#!/usr/bin/env python3
"""
SVWS-Anonym - Anonymization tool for SVWS databases

This tool anonymizes personal data in SVWS database exports by replacing
real names with randomly generated German names from the JSON-Namen repository.
"""

import json
import random
import sys
import argparse
from pathlib import Path


class NameAnonymizer:
    """Handles name anonymization using German name lists."""
    
    def __init__(self, data_dir=None):
        """Initialize the anonymizer with name data.
        
        Args:
            data_dir: Directory containing the JSON name files.
                     If None, uses the script directory.
        """
        if data_dir is None:
            data_dir = Path(__file__).parent
        else:
            data_dir = Path(data_dir)
        
        # Load name lists
        with open(data_dir / "nachnamen.json", "r", encoding="utf-8") as f:
            self.nachnamen = json.load(f)
        
        with open(data_dir / "vornamen_m.json", "r", encoding="utf-8") as f:
            self.vornamen_m = json.load(f)
        
        with open(data_dir / "vornamen_w.json", "r", encoding="utf-8") as f:
            self.vornamen_w = json.load(f)
        
        # Track mappings to ensure consistency
        self.name_mapping = {}
    
    def anonymize_firstname(self, name, gender=None):
        """Anonymize a first name.
        
        Args:
            name: The original first name
            gender: 'm' for male, 'w' for female, None for random
            
        Returns:
            Anonymized first name
        """
        if not name or name in self.name_mapping:
            return self.name_mapping.get(name, "")
        
        # Choose name list based on gender
        if gender == 'm':
            name_list = self.vornamen_m
        elif gender == 'w':
            name_list = self.vornamen_w
        else:
            # Random gender
            name_list = random.choice([self.vornamen_m, self.vornamen_w])
        
        # Get random name
        new_name = random.choice(name_list)
        self.name_mapping[name] = new_name
        return new_name
    
    def anonymize_lastname(self, name):
        """Anonymize a last name.
        
        Args:
            name: The original last name
            
        Returns:
            Anonymized last name
        """
        if not name or name in self.name_mapping:
            return self.name_mapping.get(name, "")
        
        new_name = random.choice(self.nachnamen)
        self.name_mapping[name] = new_name
        return new_name
    
    def anonymize_fullname(self, firstname, lastname, gender=None):
        """Anonymize a full name.
        
        Args:
            firstname: The original first name
            lastname: The original last name
            gender: 'm' for male, 'w' for female, None for random
            
        Returns:
            Tuple of (anonymized_firstname, anonymized_lastname)
        """
        return (
            self.anonymize_firstname(firstname, gender),
            self.anonymize_lastname(lastname)
        )


def main():
    """Main entry point for the SVWS anonymization tool."""
    parser = argparse.ArgumentParser(
        description="SVWS-Anonym - Anonymization tool for SVWS databases"
    )
    parser.add_argument(
        "--data-dir",
        help="Directory containing JSON name files (default: script directory)",
        default=None
    )
    parser.add_argument(
        "--version",
        action="version",
        version="SVWS-Anonym 0.1.0"
    )
    
    args = parser.parse_args()
    
    try:
        anonymizer = NameAnonymizer(args.data_dir)
        print("SVWS-Anonym initialized successfully")
        print(f"Loaded {len(anonymizer.nachnamen)} last names")
        print(f"Loaded {len(anonymizer.vornamen_m)} male first names")
        print(f"Loaded {len(anonymizer.vornamen_w)} female first names")
        print("\nExample anonymizations:")
        print(f"  Max Mustermann -> {' '.join(anonymizer.anonymize_fullname('Max', 'Mustermann', 'm'))}")
        print(f"  Erika Beispiel -> {' '.join(anonymizer.anonymize_fullname('Erika', 'Beispiel', 'w'))}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
