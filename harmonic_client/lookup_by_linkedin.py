"""
Lookup Harmonic Profile by LinkedIn URL

Given a LinkedIn URL, looks up the Harmonic ID from the mapping table
and fetches all available profile data from Harmonic.

Usage:
    python3 -m harmonic_client.lookup_by_linkedin "https://www.linkedin.com/in/daniel-sun-440493171/"
    python3 -m harmonic_client.lookup_by_linkedin daniel-sun-440493171
"""

import os
import sys
import json
import re
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load .env from harmonic_client directory
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

from harmonic_client.get_full_profile import HarmonicFullProfileClient


def get_bigquery_client(project_id: str) -> bigquery.Client:
    """
    Create a BigQuery client with credentials from environment.
    Supports both file-based credentials and JSON string credentials (for serverless).
    """
    # Check for JSON credentials in environment variable (for Vercel/serverless)
    credentials_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if credentials_json:
        try:
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            return bigquery.Client(project=project_id, credentials=credentials)
        except Exception as e:
            print(f"Error parsing GOOGLE_CREDENTIALS_JSON: {e}")
            raise

    # Fall back to default credentials (local development with GOOGLE_APPLICATION_CREDENTIALS)
    return bigquery.Client(project=project_id)


class LinkedInToHarmonicLookup:
    """Looks up Harmonic data given a LinkedIn URL or ID"""

    PROJECT_ID = "chapter-one-340115"
    DATASET = "linkedin_internal"

    def __init__(self):
        self.bq_client = get_bigquery_client(self.PROJECT_ID)
        self.harmonic_client = HarmonicFullProfileClient()

    def extract_linkedin_id(self, url_or_id: str) -> str:
        """
        Extract LinkedIn ID from a URL or return as-is if already an ID.

        Examples:
            https://www.linkedin.com/in/daniel-sun-440493171/ -> daniel-sun-440493171
            https://linkedin.com/in/daniel-sun-440493171 -> daniel-sun-440493171
            daniel-sun-440493171 -> daniel-sun-440493171
        """
        # Remove whitespace
        url_or_id = url_or_id.strip()

        # If it's a URL, extract the ID
        if "linkedin.com" in url_or_id:
            # Match /in/username pattern
            match = re.search(r'/in/([^/?\s]+)', url_or_id)
            if match:
                return match.group(1)

        # Otherwise assume it's already an ID
        return url_or_id.rstrip('/')

    def lookup_harmonic_id(self, linkedin_id: str) -> Optional[Tuple[int, str]]:
        """
        Look up Harmonic ID from the mapping table.

        Returns:
            Tuple of (harmonic_id, full_name) or None if not found
        """
        query = f"""
        SELECT harmonic_id, full_name
        FROM `{self.PROJECT_ID}.{self.DATASET}.linkedin_harmonic_mapping`
        WHERE linkedin_id = @linkedin_id
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("linkedin_id", "STRING", linkedin_id)
            ]
        )

        result = self.bq_client.query(query, job_config=job_config).result()
        for row in result:
            return row.harmonic_id, row.full_name

        return None

    def fetch_harmonic_data(self, harmonic_id: int) -> Dict[str, Any]:
        """Fetch full profile from Harmonic API"""
        return self.harmonic_client.get_full_profile(harmonic_id)

    def lookup(self, url_or_id: str) -> Dict[str, Any]:
        """
        Main lookup function. Given a LinkedIn URL or ID, returns all Harmonic data.

        Args:
            url_or_id: LinkedIn URL or profile ID

        Returns:
            Dictionary with lookup results and Harmonic data
        """
        result = {
            "input": url_or_id,
            "linkedin_id": None,
            "harmonic_id": None,
            "full_name": None,
            "found_in_mapping": False,
            "education": [],
            "experience": [],
            "errors": []
        }

        # Extract LinkedIn ID
        linkedin_id = self.extract_linkedin_id(url_or_id)
        result["linkedin_id"] = linkedin_id

        # Look up in mapping table
        mapping = self.lookup_harmonic_id(linkedin_id)

        if not mapping:
            result["errors"].append(f"No Harmonic mapping found for LinkedIn ID: {linkedin_id}")
            return result

        harmonic_id, full_name = mapping
        result["harmonic_id"] = harmonic_id
        result["full_name"] = full_name
        result["found_in_mapping"] = True

        # Fetch from Harmonic API
        try:
            harmonic_data = self.fetch_harmonic_data(harmonic_id)
            result["education"] = harmonic_data.get("education", [])
            result["experience"] = harmonic_data.get("experience", [])
            if harmonic_data.get("errors"):
                result["errors"].extend(harmonic_data["errors"])
        except Exception as e:
            result["errors"].append(f"Failed to fetch from Harmonic: {str(e)}")

        return result


def format_education(edu: Dict[str, Any]) -> str:
    """Format education for display"""
    school = edu.get("school", {}).get("name", "Unknown")
    degree = edu.get("degree", "")
    field = edu.get("field", "")

    start = edu.get("startDate", "")[:4] if edu.get("startDate") else ""
    end = edu.get("endDate", "")[:4] if edu.get("endDate") else ""
    dates = f"({start}-{end})" if start or end else ""

    parts = [p for p in [degree, field] if p]
    desc = " in ".join(parts) if parts else ""

    return f"  {school} {dates}\n    {desc}" if desc else f"  {school} {dates}"


def format_experience(exp: Dict[str, Any]) -> str:
    """Format experience for display"""
    company = exp.get("company", {}).get("name", "Unknown")
    title = exp.get("title", "Unknown")
    is_current = exp.get("isCurrentPosition", False)

    start = exp.get("startDate", "")[:4] if exp.get("startDate") else ""
    end = exp.get("endDate", "")[:4] if exp.get("endDate") else ("Present" if is_current else "")
    dates = f"({start}-{end})" if start or end else ""

    current = " [CURRENT]" if is_current else ""

    return f"  {title} at {company} {dates}{current}"


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python3 -m harmonic_client.lookup_by_linkedin <linkedin_url_or_id>")
        print("")
        print("Examples:")
        print('  python3 -m harmonic_client.lookup_by_linkedin "https://www.linkedin.com/in/daniel-sun-440493171/"')
        print("  python3 -m harmonic_client.lookup_by_linkedin daniel-sun-440493171")
        sys.exit(1)

    url_or_id = sys.argv[1]
    output_json = "--json" in sys.argv

    lookup = LinkedInToHarmonicLookup()

    print("=" * 70)
    print("LinkedIn to Harmonic Lookup")
    print("=" * 70)
    print(f"Input: {url_or_id}")

    result = lookup.lookup(url_or_id)

    print(f"LinkedIn ID: {result['linkedin_id']}")

    if not result["found_in_mapping"]:
        print("\nNot found in mapping table!")
        print("Run the sync script first:")
        print(f"  python3 -m harmonic_client.sync_harmonic_to_bigquery <harmonic_id>")
        sys.exit(1)

    print(f"Harmonic ID: {result['harmonic_id']}")
    print(f"Full Name: {result['full_name']}")
    print("=" * 70)

    # Display education
    education = result.get("education", [])
    print(f"\nEDUCATION ({len(education)} entries):")
    print("-" * 50)
    for edu in education:
        print(format_education(edu))
        print()

    # Display experience
    experience = result.get("experience", [])
    print(f"EXPERIENCE ({len(experience)} entries):")
    print("-" * 50)
    for exp in experience:
        print(format_experience(exp))
        print()

    # Display errors
    if result["errors"]:
        print("ERRORS:")
        for err in result["errors"]:
            print(f"  - {err}")

    # Output JSON if requested
    if output_json:
        output_path = Path(__file__).parent / "lookup_output.json"
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nJSON saved to: {output_path}")


if __name__ == "__main__":
    main()
