import os
import json

class HarmonicUtils:
    def __init__(self):
        pass

    def save_graphql_query(self, query_data, file_path, backup=False):
        """Save the modified query back to file"""
        try:
            # Create backup if requested
            if backup and os.path.exists(file_path):
                backup_path = file_path + '.backup'
                os.rename(file_path, backup_path)
                print(f"Backup created: {backup_path}")
            
            # Save the modified data
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(query_data, file, indent=2, ensure_ascii=False)
            
            print(f"Query saved to: {file_path}")
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False        
        
    def load_graphql_query(self, file_path):
        """Load the GraphQL query from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None            