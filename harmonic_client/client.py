import requests
import json
import time
import random
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import os
from harmonic_client.utils import HarmonicUtils
from harmonic_client.error_notifier import HarmonicErrorNotifier

from dotenv import load_dotenv
from pathlib import Path

# Load .env from harmonic_client directory (for local development)
# In production (Cloud Run), environment variables are set directly
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    # Try loading from root directory as fallback
    root_env_path = Path(__file__).parent.parent / '.env'
    if root_env_path.exists():
        load_dotenv(dotenv_path=root_env_path)

class HarmonicClient:
    """Client for interacting with the Harmonic API"""

    def __init__(self):
        """
        Initialize the Harmonic client

        Args:
            api_token: Bearer token for API authentication
        """
        self.base_url = "https://api.harmonic.ai/graphql?GetCompanySavedSearchResults"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": os.getenv('HARMONIC_API_TOKEN'),
            "Origin": "https://console.harmonic.ai",
            "Referer": "https://console.harmonic.ai/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "x-harmonic-request-source": "frontend",
            "version": "FE"
        }
        self.error_notifier = HarmonicErrorNotifier()

    def _check_and_notify_error(self, response: requests.Response = None, data: dict = None) -> bool:
        """
        Check for API errors and send notification if needed.
        Returns True if an error was detected, False otherwise.
        """
        # Check HTTP status code errors
        if response is not None and response.status_code in [401, 403]:
            self.error_notifier.notify_auth_failure(
                f"HTTP {response.status_code}: Authentication/authorization failed"
            )
            return True

        # Check GraphQL errors in response data
        if data and "errors" in data:
            errors = data["errors"]
            error_str = json.dumps(errors) if isinstance(errors, list) else str(errors)

            # Check for auth-related error messages
            auth_keywords = ["unauthorized", "unauthenticated", "token", "expired", "invalid", "forbidden", "401", "403"]
            is_auth_error = any(keyword in error_str.lower() for keyword in auth_keywords)

            if is_auth_error:
                self.error_notifier.notify_auth_failure(error_str[:500])
            else:
                self.error_notifier.notify_api_error("GraphQL Error", error_str[:500])
            return True

        return False  
    
    def get_company_saved_search_results(self, query: str, max_pages: int = 100, batch_size: int = 200) -> Dict[str, Any]:
        """
        Get company saved search results with pagination
        
        Args:
            query: GraphQL query string or query data
            max_pages: Maximum number of pages to fetch
            batch_size: Number of companies to save per batch file
            
        Returns:
            Dictionary containing total count and all fetched companies
        """
        # Handle both string queries and dict queries
        if isinstance(query, str):
            payload = {"query": query}
        else:
            payload = query
            
        all_edges = []
        total_count = None
        page = 1
        current_batch = []
        batch_number = 1
        
        # Track time for human-like breaks
        start_time = datetime.now()
        last_long_break = start_time
        
        base_url = "https://api.harmonic.ai/graphql?GetCompanySavedSearchResults"
        
        while True:
            print(f"Fetching page {page}...")
            response = requests.post(base_url, headers=self.headers, json=payload)
            data = response.json()

            # Check for errors and notify if needed
            if self._check_and_notify_error(response=response, data=data):
                print("Error in response:", data.get("errors", f"HTTP {response.status_code}"))
                break
                
            # Extract companies and page info - correct path is getSavedSearch.results
            search_companies = data["data"]["getSavedSearch"]["results"]
            edges = search_companies["edges"]
            all_edges.extend(edges)
            current_batch.extend(edges)
            
            # Print number of companies in this page
            print(f"Number of companies in this page: {len(edges)}")
            
            # Get total count (only once)
            if total_count is None:
                total_count = search_companies.get("totalCount")
                
            # Check if we need to save a batch
            if len(current_batch) >= batch_size:
                self._save_companies_to_json(current_batch, batch_number)
                current_batch = []  # Reset for next batch
                batch_number += 1
                
            # Pagination
            page_info = search_companies["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            if page >= max_pages:
                print(f"Reached max_pages ({max_pages}). Stopping pagination.")
                break
            payload["variables"]["after"] = page_info["endCursor"]
            page += 1
            
            # Human-like delay logic
            self._apply_human_like_delay(start_time, last_long_break)
            
        # Save any remaining companies in the current batch
        if current_batch:
            self._save_companies_to_json(current_batch, batch_number)
            
        print(f"Total companies found (from totalCount): {total_count}")
        print(f"Total companies fetched: {len(all_edges)}")
        
        # Save all fetched companies to JSON file
        self._save_all_companies_to_json(all_edges)
        
        return {
            "total_count": total_count,
            "companies": all_edges,
            "pages_fetched": page
        }
    
    def _save_companies_to_json(self, companies_data: List[Dict], batch_number: int) -> None:
        """Save companies data to a JSON file"""
        filename = f"harmonic_companies_batch_{batch_number:03d}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({"companies": companies_data}, f, indent=2)
        
        print(f"Saved batch {batch_number} to {filename}")
    
    def _save_all_companies_to_json(self, all_companies: List[Dict]) -> None:
        """Save all fetched companies to a single JSON file"""
        with open("harmonic_companies_output.json", "w") as f:
            json.dump({"companies": all_companies}, f, indent=2)
        print("Saved all companies to harmonic_companies_output.json")
    
    def add_saved_search_id(self, query_data: Dict[str, Any], saved_search_id: str) -> Dict[str, Any]:
        """
        Add saved search ID to the query
        """
        if (query_data and 
            'variables' in query_data and 
            'idOrUrn' in query_data['variables']):
            
            # Convert saved_search_id to string to ensure GraphQL compatibility
            query_data['variables']['idOrUrn'] = str(saved_search_id)
            print(f"Querying saved search: {saved_search_id}")
            return query_data
        else:
            print("Invalid query structure")
            return None         

    def get_companies_by_id(self, query: str) -> Dict[str, Any]:
        """
        Get companies by id
        """
        base_url = "https://api.harmonic.ai/graphql?GetCompaniesByIds"
        response = requests.post(base_url, headers=self.headers, json=query)
        data = response.json()

        # Check for errors and notify if needed
        self._check_and_notify_error(response=response, data=data)

        # Save response to JSON file
        with open("harmonic_client/company_by_id_output.json", "w") as f:
            json.dump(data, f, indent=2)
        return data
    
    def add_company_ids(self, query_data, new_ids):
        """Add new company IDs to the existing list"""
        if query_data and 'variables' in query_data and 'ids' in query_data['variables']:
            existing_ids = query_data['variables']['ids']
            
            # Convert new_ids to list if it's a single ID
            if isinstance(new_ids, int):
                new_ids = [new_ids]
            
            # Add new IDs, avoiding duplicates
            for new_id in new_ids:
                if new_id not in existing_ids:
                    existing_ids.append(new_id)
            
            print(f"Added {len(new_ids)} new IDs. Total IDs: {len(existing_ids)}")
            return query_data
        else:
            print("Invalid query structure")
            return None 
        
    def search_people(self, query: str = "", max_pages: int = 239, batch_size: int = 200) -> Dict[str, Any]:
        """
        Search for people using the Harmonic API
        
        Args:
            query: GraphQL query string
            max_pages: Maximum number of pages to fetch
            batch_size: Number of people to save per batch file
            
        Returns:
            Dictionary containing total count and all fetched people
        """
        payload = {"query": query}
        
        all_edges = []
        total_count = None
        page = 1
        current_batch = []
        batch_number = 1
        
        # Track time for human-like breaks
        start_time = datetime.now()
        last_long_break = start_time
        
        while True:
            print(f"Fetching page {page}...")
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            data = response.json()

            # Check for errors and notify if needed
            if self._check_and_notify_error(response=response, data=data):
                print("Error in response:", data.get("errors", f"HTTP {response.status_code}"))
                break

            # Extract people and page info
            search_people = data["data"]["searchPeople"]
            edges = search_people["edges"]
            all_edges.extend(edges)
            current_batch.extend(edges)
            
            # Print number of people in this page
            print(f"Number of people in this page: {len(edges)}")
            
            # Get total count (only once)
            if total_count is None:
                total_count = search_people.get("totalCount")
                
            # Check if we need to save a batch
            if len(current_batch) >= batch_size:
                self._save_people_to_json(current_batch, batch_number)
                current_batch = []  # Reset for next batch
                batch_number += 1
                
            # Pagination
            page_info = search_people["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            if page >= max_pages:
                print(f"Reached max_pages ({max_pages}). Stopping pagination.")
                break
            payload["variables"]["after"] = page_info["endCursor"]
            page += 1
            
            # Human-like delay logic
            self._apply_human_like_delay(start_time, last_long_break)
            
        # Save any remaining people in the current batch
        if current_batch:
            self._save_people_to_json(current_batch, batch_number)
            
        print(f"Total people found (from totalCount): {total_count}")
        print(f"Total people fetched: {len(all_edges)}")
        
        # Save all fetched people to JSON file
        self._save_all_people_to_json(all_edges)
        
        return {
            "total_count": total_count,
            "people": all_edges,
            "pages_fetched": page
        }
    
    def _save_people_to_json(self, people_data: List[Dict], batch_number: int) -> None:
        """Save people data to a JSON file"""
        filename = f"harmonic_output_batch_{batch_number:03d}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({"people": people_data}, f, indent=2)
        
        print(f"Saved batch {batch_number} to {filename}")
    
    def _save_all_people_to_json(self, all_people: List[Dict]) -> None:
        """Save all fetched people to a single JSON file"""
        with open("harmonic_output.json", "w") as f:
            json.dump({"people": all_people}, f, indent=2)
        print("Saved all people to harmonic_output.json")
    
    def _apply_human_like_delay(self, start_time: datetime, last_long_break: datetime) -> None:
        """Apply human-like delays between requests"""
        current_time = datetime.now()
        time_since_last_long_break = current_time - last_long_break
        
        # Check if it's been 3 minutes since the last long break
        if time_since_last_long_break >= timedelta(minutes=3):
            # Take a longer break (30-60 seconds) to simulate human behavior
            long_delay = random.uniform(60*3, 60*5)
            print(f"Taking a longer break ({long_delay:.2f} seconds) - simulating human behavior...")
            time.sleep(long_delay)
            last_long_break = current_time
        else:
            # Regular short delay with some variation
            delay = random.uniform(2, 8)  # Slightly increased range for more variation
            print(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)


# Example usage
if __name__ == "__main__":

    client = HarmonicClient()
    utils = HarmonicUtils()

    # """Example of different ways to use the functions"""
    # file_path = "harmonic_client/payload_data/get_companies_by_id.graphql"
    # output_path = "harmonic_client/payload_data/get_companies_by_id_2.graphql"

    # # Example 1: Add single ID
    # query_data = client.load_graphql_query(file_path)
    # query_data = client.add_company_ids(query_data, [62363328])  # Assign the return value
    # client.save_graphql_query(query_data, output_path)
    # print(f"Query saved to: {output_path}")

    # response = client.get_companies_by_id(query_data)
    # print(response)

    file_path = "harmonic_client/payload_data/get_company_saved_search_results.graphql"
    output_path = "harmonic_client/payload_data/get_company_saved_search_results_2.graphql"
    query_data = utils.load_graphql_query(file_path)
    query_data = client.add_saved_search_id(query_data, 149709)  # Assign the return value
    utils.save_graphql_query(query_data, output_path)
    print(f"Query saved to: {output_path}")

    # response = client.get_company_saved_search_results(query_data)
    # print(response)
