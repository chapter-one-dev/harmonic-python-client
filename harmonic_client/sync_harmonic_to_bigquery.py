"""
Sync Harmonic Profile Data to BigQuery LinkedIn Tables

Fetches education, experience, and profile data from Harmonic API
and inserts into BigQuery tables if the user doesn't already exist.

Tables:
- chapter-one-340115.linkedin_internal.linkedin_education
- chapter-one-340115.linkedin_internal.linkedin_experience
- chapter-one-340115.linkedin_internal.linkedin_profile

Usage:
    python3 -m harmonic_client.sync_harmonic_to_bigquery <harmonic_id> <linkedin_id>
    python3 -m harmonic_client.sync_harmonic_to_bigquery 36601930 daniel-sun-440493171
"""

import os
import sys
import json
import re
import requests
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv

# Load .env from harmonic_client directory
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

# Import the Harmonic client
from harmonic_client.get_full_profile import HarmonicFullProfileClient
from harmonic_client.error_notifier import HarmonicErrorNotifier


class HarmonicToBigQuerySync:
    """Syncs Harmonic person data to BigQuery LinkedIn tables"""

    PROJECT_ID = "chapter-one-340115"
    DATASET = "linkedin_internal"
    HARMONIC_BASE_URL = "https://api.harmonic.ai/graphql"

    def __init__(self):
        self.harmonic_client = HarmonicFullProfileClient()
        self.bq_client = bigquery.Client(project=self.PROJECT_ID)
        self.error_notifier = HarmonicErrorNotifier()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": os.getenv('HARMONIC_API_TOKEN'),
            "Origin": "https://console.harmonic.ai",
            "Referer": "https://console.harmonic.ai/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "x-harmonic-request-source": "frontend",
        }

    def get_person_by_linkedin_url(self, linkedin_id: str) -> Optional[Tuple[int, str]]:
        """
        Look up a person in Harmonic by their LinkedIn ID.

        Uses search to find candidates, then verifies by checking their LinkedIn URL.

        Args:
            linkedin_id: The LinkedIn profile ID (e.g., 'anishjaygupta')

        Returns:
            Tuple of (harmonic_id, full_name) or None if not found
        """
        # First check if we already have a mapping
        query = """
        SELECT harmonic_id, full_name FROM `chapter-one-340115.linkedin_internal.linkedin_harmonic_mapping`
        WHERE linkedin_id = @linkedin_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("linkedin_id", "STRING", linkedin_id)
            ]
        )
        result = self.bq_client.query(query, job_config=job_config).result()
        for row in result:
            return row.harmonic_id, row.full_name

        # Try searching with the LinkedIn ID as query (often contains name)
        # e.g., "anishjaygupta" or "john-smith-123"
        search_results = self._search_people(linkedin_id)

        if not search_results:
            # Try with hyphens replaced by spaces
            search_query = linkedin_id.replace("-", " ").replace("_", " ")
            # Remove trailing numbers
            search_query = re.sub(r'\d+$', '', search_query).strip()
            if search_query:
                search_results = self._search_people(search_query)

        # Check each result to see if their LinkedIn matches
        for person in search_results[:10]:  # Limit to first 10
            harmonic_id = person.get("id")
            full_name = person.get("fullName")

            # Fetch their LinkedIn URL from Harmonic
            linkedin_info = self.get_linkedin_id_from_harmonic(harmonic_id)
            if linkedin_info:
                fetched_linkedin_id, _ = linkedin_info
                if fetched_linkedin_id and fetched_linkedin_id.lower() == linkedin_id.lower():
                    print(f"  Found match: {full_name} (Harmonic ID: {harmonic_id})")
                    return harmonic_id, full_name

        return None

    def get_linkedin_id_from_harmonic_by_search(self, linkedin_id: str) -> Optional[Tuple[int, str]]:
        """
        Alias for get_person_by_linkedin_url for backwards compatibility.

        Args:
            linkedin_id: The LinkedIn profile ID

        Returns:
            Tuple of (harmonic_id, full_name) or None if not found
        """
        return self.get_person_by_linkedin_url(linkedin_id)

    def _search_people(self, query: str) -> List[Dict]:
        """
        Search for people in Harmonic by name/query.

        Args:
            query: Search query string

        Returns:
            List of person dictionaries from search results
        """
        payload = {
            "operationName": "TypeaheadSearchWithInvestors",
            "variables": {"query": query},
            "query": """query TypeaheadSearchWithInvestors($query: String!) {
  getPeopleWithTypeahead(query: $query) {
    id
    fullName
    experience {
      company {
        id
        name
        __typename
      }
      title
      isCurrentPosition
      __typename
    }
    profilePictureUrl
    __typename
  }
}"""
        }

        try:
            response = requests.post(
                f"{self.HARMONIC_BASE_URL}?TypeaheadSearchWithInvestors",
                headers=self.headers,
                json=payload
            )
            # Check for auth errors
            if response.status_code in [401, 403]:
                self.error_notifier.notify_auth_failure(f"HTTP {response.status_code}: _search_people")
            response.raise_for_status()
            data = response.json()
            # Check for GraphQL errors
            if "errors" in data:
                error_str = json.dumps(data["errors"])[:500]
                self.error_notifier.notify_api_error("GraphQL Error", error_str)
            return data.get("data", {}).get("getPeopleWithTypeahead", [])
        except Exception as e:
            print(f"Error searching Harmonic: {e}")
            return []

    def get_linkedin_id_from_harmonic(self, harmonic_id: int) -> Optional[Tuple[str, str]]:
        """
        Fetch LinkedIn URL and ID from Harmonic for a person.

        Args:
            harmonic_id: The Harmonic person ID

        Returns:
            Tuple of (linkedin_id, full_name) or None if not found
        """
        payload = {
            "operationName": "GetPersonLinkedIn",
            "variables": {"id": harmonic_id},
            "query": """query GetPersonLinkedIn($id: Int!) {
  getPersonById(id: $id) {
    id
    fullName
    socials {
      linkedin {
        url
        __typename
      }
      __typename
    }
    __typename
  }
}"""
        }

        try:
            response = requests.post(
                f"{self.HARMONIC_BASE_URL}?GetPersonLinkedIn",
                headers=self.headers,
                json=payload
            )
            # Check for auth errors
            if response.status_code in [401, 403]:
                self.error_notifier.notify_auth_failure(f"HTTP {response.status_code}: get_linkedin_id_from_harmonic")
            response.raise_for_status()
            data = response.json()

            # Check for GraphQL errors
            if "errors" in data:
                error_str = json.dumps(data["errors"])[:500]
                self.error_notifier.notify_api_error("GraphQL Error", error_str)

            person = data.get("data", {}).get("getPersonById", {})
            full_name = person.get("fullName")

            linkedin_url = person.get("socials", {}).get("linkedin", {}).get("url")
            if linkedin_url:
                # Extract LinkedIn ID from URL
                # URL format: https://linkedin.com/in/daniel-sun-440493171
                linkedin_id = linkedin_url.rstrip("/").split("/")[-1]
                return linkedin_id, full_name

            return None
        except Exception as e:
            print(f"Error fetching LinkedIn ID from Harmonic: {e}")
            return None

    def check_user_exists(self, table_name: str, linkedin_id: str) -> bool:
        """Check if a user already has entries in a table"""
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.PROJECT_ID}.{self.DATASET}.{table_name}`
        WHERE linkedinId = @linkedin_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("linkedin_id", "STRING", linkedin_id)
            ]
        )
        result = self.bq_client.query(query, job_config=job_config).result()
        for row in result:
            return row.count > 0
        return False

    def transform_education(self, education_list: List[Dict], linkedin_id: str) -> List[Dict]:
        """Transform Harmonic education data to BigQuery schema"""
        today = datetime.now().strftime("%Y-%m-%d")
        rows = []

        for idx, edu in enumerate(education_list):
            school = edu.get("school", {})
            school_name = school.get("name", "")
            linkedin_url = school.get("linkedinUrl", "")
            logo_url = school.get("logoUrl") or ""

            # Extract school URN from LinkedIn URL if available
            # e.g., https://linkedin.com/school/mit -> urn:li:fs_miniSchool:mit
            school_urn = ""
            if linkedin_url:
                parts = linkedin_url.rstrip("/").split("/")
                if parts:
                    school_slug = parts[-1]
                    school_urn = f"urn:li:fs_miniSchool:{school_slug}"

            # Parse dates
            start_date = None
            end_date = None
            if edu.get("startDate"):
                try:
                    start_date = edu["startDate"][:10]  # Get YYYY-MM-DD
                except:
                    pass
            if edu.get("endDate"):
                try:
                    end_date = edu["endDate"][:10]
                except:
                    pass

            # Generate a unique entity URN for this education entry
            entity_urn = f"urn:li:fs_education:harmonic_{linkedin_id}_{idx}"

            row = {
                "date": today,
                "linkedinId": linkedin_id,
                "entityUrn": entity_urn,
                "school_objectUrn": school_urn or f"urn:li:school:harmonic_{idx}",
                "school_entityUrn": school_urn or f"urn:li:fs_miniSchool:harmonic_{idx}",
                "school_active": True,
                "school_schoolName": school_name,
                "school_trackingId": f"harmonic_{linkedin_id}_{idx}",
                "school_logoUrl": logo_url,
                "startDate": start_date,
                "endDate": end_date,
                "schoolName": school_name,
                "fieldOfStudy": edu.get("field") or "",
                "schoolUrn": school_urn or f"urn:li:fs_miniSchool:harmonic_{idx}",
                "degreeName": edu.get("degree"),
            }
            rows.append(row)

        return rows

    def transform_experience(self, experience_list: List[Dict], linkedin_id: str) -> List[Dict]:
        """Transform Harmonic experience data to BigQuery schema"""
        today = datetime.now().strftime("%Y-%m-%d")
        rows = []

        for idx, exp in enumerate(experience_list):
            company = exp.get("company", {})
            company_name = company.get("name", "")
            logo_url = company.get("logoUrl") or ""

            # Get company LinkedIn URL if available
            socials = company.get("socials") or {}
            linkedin_info = socials.get("linkedin") or {}
            company_linkedin_url = linkedin_info.get("url", "")

            # Parse dates - Harmonic uses ISO format
            start_date = ""
            end_date = None
            if exp.get("startDate"):
                try:
                    start_date = exp["startDate"][:10]  # YYYY-MM-DD
                except:
                    pass
            if exp.get("endDate"):
                try:
                    end_date = exp["endDate"][:10]
                except:
                    pass

            # Generate entity URN
            entity_urn = f"urn:li:fs_position:harmonic_{linkedin_id}_{idx}"

            # Get company URN from Harmonic company ID
            company_id = company.get("id")
            company_urn = f"urn:li:fs_miniCompany:harmonic_{company_id}" if company_id else None

            row = {
                "date": today,
                "linkedinId": linkedin_id,
                "entityUrn": entity_urn,
                "companyName": company_name,
                "startDate": start_date,
                "endDate": end_date,
                "description": exp.get("description"),
                "title": exp.get("title"),
                "companyUrn": company_urn,
                "companyLogoUrl": logo_url,
                "locationName": None,  # Harmonic doesn't provide this in the current query
                "geoLocationName": None,
                "geoUrn": None,
                "region": None,
            }
            rows.append(row)

        return rows

    def transform_profile(self, harmonic_id: int, linkedin_id: str,
                         education_list: List[Dict], experience_list: List[Dict]) -> Dict:
        """
        Transform Harmonic data to BigQuery profile schema.
        Since we can't get basic profile info from Harmonic API directly,
        we'll derive what we can from education/experience data.
        """
        today = datetime.now().strftime("%Y-%m-%d")

        # Try to get current position for headline
        headline = None
        for exp in experience_list:
            if exp.get("isCurrentPosition"):
                title = exp.get("title", "")
                company = exp.get("company", {}).get("name", "")
                if title and company:
                    headline = f"{title} at {company}"
                break

        # Check if likely a student (has education end date in future or recent)
        is_student = False
        current_year = datetime.now().year
        for edu in education_list:
            end_date = edu.get("endDate", "")
            if end_date:
                try:
                    end_year = int(end_date[:4])
                    if end_year >= current_year:
                        is_student = True
                        break
                except:
                    pass

        row = {
            "date": today,
            "linkedinId": linkedin_id,
            "industryName": None,
            "lastName": None,
            "locationName": None,
            "student": is_student,
            "geoCountryName": None,
            "geoCountryUrn": None,
            "geoLocationBackfilled": False,
            "elt": False,
            "industryUrn": None,
            "firstName": None,
            "entityUrn": f"urn:li:fs_profile:harmonic_{harmonic_id}",
            "geoLocation": None,
            "geoLocationName": None,
            "headline": headline,
            "displayPictureUrl": None,
            "img_100_100": None,
            "img_200_200": None,
            "img_400_400": None,
            "img_800_800": None,
            "profile_id": str(harmonic_id),
            "profile_urn": f"urn:li:fs_profile:harmonic_{harmonic_id}",
            "member_urn": f"urn:li:member:harmonic_{harmonic_id}",
            "public_id": linkedin_id,
        }

        return row

    def insert_education(self, rows: List[Dict]) -> int:
        """Insert education rows into BigQuery"""
        if not rows:
            return 0

        table_ref = f"{self.PROJECT_ID}.{self.DATASET}.linkedin_education"
        errors = self.bq_client.insert_rows_json(table_ref, rows)

        if errors:
            print(f"Education insert errors: {errors}")
            return 0

        return len(rows)

    def insert_experience(self, rows: List[Dict]) -> int:
        """Insert experience rows into BigQuery"""
        if not rows:
            return 0

        table_ref = f"{self.PROJECT_ID}.{self.DATASET}.linkedin_experience"
        errors = self.bq_client.insert_rows_json(table_ref, rows)

        if errors:
            print(f"Experience insert errors: {errors}")
            return 0

        return len(rows)

    def insert_profile(self, row: Dict) -> int:
        """Insert profile row into BigQuery"""
        table_ref = f"{self.PROJECT_ID}.{self.DATASET}.linkedin_profile"
        errors = self.bq_client.insert_rows_json(table_ref, [row])

        if errors:
            print(f"Profile insert errors: {errors}")
            return 0

        return 1

    def check_mapping_exists(self, linkedin_id: str) -> bool:
        """Check if a mapping already exists for this LinkedIn ID"""
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.PROJECT_ID}.{self.DATASET}.linkedin_harmonic_mapping`
        WHERE linkedin_id = @linkedin_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("linkedin_id", "STRING", linkedin_id)
            ]
        )
        result = self.bq_client.query(query, job_config=job_config).result()
        for row in result:
            return row.count > 0
        return False

    def insert_mapping(self, harmonic_id: int, linkedin_id: str, full_name: str, linkedin_url: str) -> int:
        """Insert or update the linkedin_harmonic_mapping table"""
        from datetime import datetime

        row = {
            "linkedin_id": linkedin_id,
            "harmonic_id": harmonic_id,
            "full_name": full_name,
            "linkedin_url": linkedin_url,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

        table_ref = f"{self.PROJECT_ID}.{self.DATASET}.linkedin_harmonic_mapping"
        errors = self.bq_client.insert_rows_json(table_ref, [row])

        if errors:
            print(f"Mapping insert errors: {errors}")
            return 0

        return 1

    def sync_person(self, harmonic_id: int, linkedin_id: str, dry_run: bool = False,
                    verbose: bool = False) -> Dict[str, Any]:
        """
        Sync a person's data from Harmonic to BigQuery.

        Args:
            harmonic_id: The Harmonic person ID
            linkedin_id: The LinkedIn profile ID (e.g., 'daniel-sun-440493171')
            dry_run: If True, don't actually insert, just show what would be inserted
            verbose: If True, print the data that would be inserted

        Returns:
            Dictionary with sync results
        """
        results = {
            "harmonic_id": harmonic_id,
            "linkedin_id": linkedin_id,
            "mapping": {"exists": False, "inserted": 0, "skipped": False},
            "education": {"exists": False, "inserted": 0, "skipped": False, "data": []},
            "experience": {"exists": False, "inserted": 0, "skipped": False, "data": []},
            "profile": {"exists": False, "inserted": 0, "skipped": False, "data": None},
            "errors": []
        }

        # Fetch data from Harmonic
        print(f"Fetching data from Harmonic for person ID: {harmonic_id}")
        try:
            profile_data = self.harmonic_client.get_full_profile(harmonic_id)
        except Exception as e:
            results["errors"].append(f"Failed to fetch from Harmonic: {str(e)}")
            return results

        education_list = profile_data.get("education", [])
        experience_list = profile_data.get("experience", [])

        print(f"  Found {len(education_list)} education entries, {len(experience_list)} experience entries")

        # Get full name for mapping (fetch from Harmonic if needed)
        linkedin_info = self.get_linkedin_id_from_harmonic(harmonic_id)
        full_name = linkedin_info[1] if linkedin_info else None
        linkedin_url = f"https://linkedin.com/in/{linkedin_id}"

        # Check and sync mapping table
        print("\nChecking mapping table...")
        if self.check_mapping_exists(linkedin_id):
            results["mapping"]["exists"] = True
            results["mapping"]["skipped"] = True
            print(f"  Mapping already exists, skipping")
        else:
            if dry_run:
                print(f"  [DRY RUN] Would insert mapping: {linkedin_id} -> {harmonic_id}")
                results["mapping"]["inserted"] = 1
            else:
                inserted = self.insert_mapping(harmonic_id, linkedin_id, full_name, linkedin_url)
                results["mapping"]["inserted"] = inserted
                print(f"  Inserted mapping: {linkedin_id} -> {harmonic_id}")

        # Check and sync education
        print("\nChecking education table...")
        if self.check_user_exists("linkedin_education", linkedin_id):
            results["education"]["exists"] = True
            results["education"]["skipped"] = True
            print(f"  User already has education entries, skipping")
        else:
            edu_rows = self.transform_education(education_list, linkedin_id)
            results["education"]["data"] = edu_rows
            if verbose and edu_rows:
                print("  Transformed education data:")
                for row in edu_rows:
                    print(f"    - {row['schoolName']}: {row['degreeName']} in {row['fieldOfStudy']}")
            if dry_run:
                print(f"  [DRY RUN] Would insert {len(edu_rows)} education rows")
                results["education"]["inserted"] = len(edu_rows)
            else:
                inserted = self.insert_education(edu_rows)
                results["education"]["inserted"] = inserted
                print(f"  Inserted {inserted} education rows")

        # Check and sync experience
        print("\nChecking experience table...")
        if self.check_user_exists("linkedin_experience", linkedin_id):
            results["experience"]["exists"] = True
            results["experience"]["skipped"] = True
            print(f"  User already has experience entries, skipping")
        else:
            exp_rows = self.transform_experience(experience_list, linkedin_id)
            results["experience"]["data"] = exp_rows
            if verbose and exp_rows:
                print("  Transformed experience data:")
                for row in exp_rows:
                    print(f"    - {row['title']} at {row['companyName']} ({row['startDate']} - {row['endDate'] or 'Present'})")
            if dry_run:
                print(f"  [DRY RUN] Would insert {len(exp_rows)} experience rows")
                results["experience"]["inserted"] = len(exp_rows)
            else:
                inserted = self.insert_experience(exp_rows)
                results["experience"]["inserted"] = inserted
                print(f"  Inserted {inserted} experience rows")

        # Check and sync profile
        print("\nChecking profile table...")
        if self.check_user_exists("linkedin_profile", linkedin_id):
            results["profile"]["exists"] = True
            results["profile"]["skipped"] = True
            print(f"  User already has profile entry, skipping")
        else:
            profile_row = self.transform_profile(harmonic_id, linkedin_id, education_list, experience_list)
            results["profile"]["data"] = profile_row
            if verbose:
                print(f"  Transformed profile data:")
                print(f"    - LinkedIn ID: {profile_row['linkedinId']}")
                print(f"    - Headline: {profile_row['headline']}")
                print(f"    - Student: {profile_row['student']}")
            if dry_run:
                print(f"  [DRY RUN] Would insert 1 profile row")
                results["profile"]["inserted"] = 1
            else:
                inserted = self.insert_profile(profile_row)
                results["profile"]["inserted"] = inserted
                print(f"  Inserted {inserted} profile row")

        return results


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python3 -m harmonic_client.sync_harmonic_to_bigquery <harmonic_id> [linkedin_id] [--dry-run] [--verbose]")
        print("")
        print("If linkedin_id is not provided, it will be looked up from Harmonic.")
        print("")
        print("Examples:")
        print("  python3 -m harmonic_client.sync_harmonic_to_bigquery 36601930")
        print("  python3 -m harmonic_client.sync_harmonic_to_bigquery 36601930 daniel-sun-440493171")
        print("  python3 -m harmonic_client.sync_harmonic_to_bigquery 36601930 --dry-run --verbose")
        sys.exit(1)

    try:
        harmonic_id = int(sys.argv[1])
    except ValueError:
        print(f"Error: '{sys.argv[1]}' is not a valid Harmonic ID (must be an integer)")
        sys.exit(1)

    dry_run = "--dry-run" in sys.argv
    verbose = "--verbose" in sys.argv or "-v" in sys.argv

    # Check if linkedin_id was provided (not a flag)
    linkedin_id = None
    for arg in sys.argv[2:]:
        if not arg.startswith("-"):
            linkedin_id = arg
            break

    print("=" * 70)
    print("Harmonic to BigQuery Sync")
    print("=" * 70)
    print(f"Harmonic ID: {harmonic_id}")

    syncer = HarmonicToBigQuerySync()

    # Look up LinkedIn ID from Harmonic if not provided
    if not linkedin_id:
        print("LinkedIn ID not provided, looking up from Harmonic...")
        result = syncer.get_linkedin_id_from_harmonic(harmonic_id)
        if result:
            linkedin_id, full_name = result
            print(f"  Found: {full_name}")
            print(f"  LinkedIn ID: {linkedin_id}")
        else:
            print("ERROR: Could not find LinkedIn ID for this person in Harmonic.")
            print("Please provide the LinkedIn ID manually.")
            sys.exit(1)

    print(f"LinkedIn ID: {linkedin_id}")
    print(f"Dry Run: {dry_run}")
    print(f"Verbose: {verbose}")
    print("=" * 70)

    results = syncer.sync_person(harmonic_id, linkedin_id, dry_run=dry_run, verbose=verbose)

    print("\n" + "=" * 70)
    print("SYNC RESULTS:")
    print("=" * 70)
    print(f"Mapping: ", end="")
    if results["mapping"]["skipped"]:
        print("SKIPPED (already exists)")
    else:
        print(f"Inserted {results['mapping']['inserted']} row")

    print(f"Education: ", end="")
    if results["education"]["skipped"]:
        print("SKIPPED (already exists)")
    else:
        print(f"Inserted {results['education']['inserted']} rows")

    print(f"Experience: ", end="")
    if results["experience"]["skipped"]:
        print("SKIPPED (already exists)")
    else:
        print(f"Inserted {results['experience']['inserted']} rows")

    print(f"Profile: ", end="")
    if results["profile"]["skipped"]:
        print("SKIPPED (already exists)")
    else:
        print(f"Inserted {results['profile']['inserted']} rows")

    if results["errors"]:
        print("\nERRORS:")
        for err in results["errors"]:
            print(f"  - {err}")

    # Save results to file
    output_path = Path(__file__).parent / "sync_results.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()
