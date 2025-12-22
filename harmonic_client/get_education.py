"""
Harmonic Person Education API Client

Fetches education information for a person from the Harmonic GraphQL API.
"""

import requests
import json
import os
import sys
from typing import Dict, Any, List
from dotenv import load_dotenv
from pathlib import Path
from harmonic_client.error_notifier import HarmonicErrorNotifier

# Load .env from harmonic_client directory
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


class HarmonicEducationClient:
    """Client for fetching person education data from Harmonic API"""

    BASE_URL = "https://api.harmonic.ai/graphql"

    def __init__(self):
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": os.getenv('HARMONIC_API_TOKEN'),
            "Origin": "https://console.harmonic.ai",
            "Referer": "https://console.harmonic.ai/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "x-harmonic-request-source": "frontend",
            "version": "FE"
        }
        self.error_notifier = HarmonicErrorNotifier()

    def _make_request(self, operation_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make a GraphQL request to Harmonic API"""
        url = f"{self.BASE_URL}?{operation_name}"
        response = requests.post(url, headers=self.headers, json=payload)

        # Check for HTTP auth errors
        if response.status_code in [401, 403]:
            self.error_notifier.notify_auth_failure(
                f"HTTP {response.status_code}: {operation_name}"
            )

        response.raise_for_status()
        data = response.json()

        # Check for GraphQL errors
        if "errors" in data:
            error_str = json.dumps(data["errors"])[:500]
            auth_keywords = ["unauthorized", "unauthenticated", "token", "expired", "forbidden"]
            if any(kw in error_str.lower() for kw in auth_keywords):
                self.error_notifier.notify_auth_failure(error_str)
            else:
                self.error_notifier.notify_api_error("GraphQL Error", error_str)

        return data

    def get_person_education(self, person_id: int) -> Dict[str, Any]:
        """
        Get education history for a person by their Harmonic ID.

        Args:
            person_id: The Harmonic person ID (e.g., 36601930)

        Returns:
            Dictionary containing the person's education data
        """
        payload = {
            "operationName": "GetPersonEducation",
            "variables": {
                "id": person_id
            },
            "query": """fragment School on School {
  name
  linkedinUrl
  logoUrl
  __typename
}

fragment Education on EducationMetadata {
  school {
    ...School
    __typename
  }
  degree
  field
  grade
  startDate
  endDate
  __typename
}

query GetPersonEducation($id: Int!) {
  getPersonById(id: $id) {
    id
    education {
      ...Education
      __typename
    }
    __typename
  }
}"""
        }

        data = self._make_request("GetPersonEducation", payload)

        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        return data.get("data", {}).get("getPersonById", {})

    def get_education_list(self, person_id: int) -> List[Dict[str, Any]]:
        """
        Get just the education list for a person.

        Args:
            person_id: The Harmonic person ID

        Returns:
            List of education entries
        """
        result = self.get_person_education(person_id)
        return result.get("education", [])


def format_education(edu: Dict[str, Any]) -> str:
    """Format an education entry for display"""
    school = edu.get("school", {})
    school_name = school.get("name", "Unknown School")
    degree = edu.get("degree", "")
    field = edu.get("field", "")

    # Parse dates
    start = edu.get("startDate", "")
    end = edu.get("endDate", "")

    start_year = start[:4] if start else ""
    end_year = end[:4] if end else ""

    date_str = ""
    if start_year and end_year:
        date_str = f" ({start_year}-{end_year})"
    elif end_year:
        date_str = f" ({end_year})"

    # Build description
    parts = []
    if degree:
        parts.append(degree)
    if field:
        parts.append(field)

    desc = " in ".join(parts) if parts else "No degree info"

    return f"  {school_name}{date_str}\n    {desc}"


def main():
    """Main function to get education by person ID"""
    # Get person ID from command line or use default
    if len(sys.argv) > 1:
        try:
            person_id = int(sys.argv[1])
        except ValueError:
            print(f"Error: '{sys.argv[1]}' is not a valid person ID (must be an integer)")
            sys.exit(1)
    else:
        person_id = 36601930  # Default: Daniel Sun (MIT)

    client = HarmonicEducationClient()

    print(f"Fetching education for person ID: {person_id}")
    print("=" * 60)

    result = client.get_person_education(person_id)
    education_list = result.get("education", [])

    print(f"\nEDUCATION ({len(education_list)} entries):")
    print("-" * 40)

    for edu in education_list:
        print(format_education(edu))
        print()

    # Save full results to file
    output_path = Path(__file__).parent / "education_output.json"
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Full results saved to: {output_path}")


if __name__ == "__main__":
    main()
