"""
Harmonic Person Experience API Client

Fetches work experience information for a person from the Harmonic GraphQL API.
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


class HarmonicExperienceClient:
    """Client for fetching person experience data from Harmonic API"""

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

    def get_person_experience(self, person_id: int) -> Dict[str, Any]:
        """
        Get work experience history for a person by their Harmonic ID.

        Args:
            person_id: The Harmonic person ID (e.g., 36601930)

        Returns:
            Dictionary containing the person's experience data
        """
        payload = {
            "operationName": "GetPersonExperience",
            "variables": {
                "id": person_id
            },
            "query": """fragment PersonExperience on ExperienceMetadata {
  title
  department
  description
  company {
    id
    logoUrl
    name
    headcount
    fundingPerEmployee
    funding {
      fundingTotal
      fundingStage
      investors {
        __typename
        ... on Company {
          companyUrn: entityUrn
          logoUrl
          name
          id
          investorUrn
          __typename
        }
        ... on Person {
          personUrn: entityUrn
          logoUrl: profilePictureUrl
          name: fullName
          id
          investorUrn
          __typename
        }
      }
      fundingRounds {
        announcementDate
        fundingAmount
        fundingCurrency
        fundingRoundType
        sourceUrl
        investors {
          entityUrn
          investor {
            __typename
            ... on Company {
              companyUrn: entityUrn
              name
              id
              __typename
            }
            ... on Person {
              personUrn: entityUrn
              name: fullName
              id
              __typename
            }
          }
          investorName
          isLead
          __typename
        }
        __typename
      }
      __typename
    }
    highlights {
      category
      text
      __typename
    }
    foundingDate {
      date
      __typename
    }
    socials {
      linkedin {
        url
        __typename
      }
      __typename
    }
    tractionMetrics {
      headcount {
        ago90d {
          percentChange
          value
          change
          __typename
        }
        metrics {
          timestamp
          metricValue
          __typename
        }
        __typename
      }
      __typename
    }
    relatedCompanies {
      priorStealthAssociation {
        emergenceDate
        previouslyKnownAs
        __typename
      }
      __typename
    }
    __typename
  }
  startDate
  endDate
  isCurrentPosition
  __typename
}

query GetPersonExperience($id: Int!) {
  getPersonById(id: $id, extended: false) {
    id
    experience {
      ...PersonExperience
      __typename
    }
    __typename
  }
}"""
        }

        data = self._make_request("GetPersonExperience", payload)

        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        return data.get("data", {}).get("getPersonById", {})

    def get_experience_list(self, person_id: int) -> List[Dict[str, Any]]:
        """
        Get just the experience list for a person.

        Args:
            person_id: The Harmonic person ID

        Returns:
            List of experience entries
        """
        result = self.get_person_experience(person_id)
        return result.get("experience", [])


def format_experience(exp: Dict[str, Any]) -> str:
    """Format an experience entry for display"""
    company = exp.get("company", {})
    company_name = company.get("name", "Unknown Company")
    title = exp.get("title", "Unknown Title")
    is_current = exp.get("isCurrentPosition", False)

    # Parse dates
    start = exp.get("startDate", "")
    end = exp.get("endDate", "")

    start_year = start[:4] if start else ""
    end_year = end[:4] if end else "Present" if is_current else ""

    date_str = ""
    if start_year and end_year:
        date_str = f" ({start_year}-{end_year})"
    elif start_year:
        date_str = f" ({start_year}-)"
    elif end_year:
        date_str = f" (-{end_year})"

    # Location
    location = exp.get("location", {})
    loc_parts = []
    if location:
        if location.get("city"):
            loc_parts.append(location.get("city"))
        if location.get("state"):
            loc_parts.append(location.get("state"))
        if location.get("country"):
            loc_parts.append(location.get("country"))
    loc_str = ", ".join(loc_parts) if loc_parts else ""

    # Current indicator
    current_indicator = " [CURRENT]" if is_current else ""

    result = f"  {title} at {company_name}{date_str}{current_indicator}"
    if loc_str:
        result += f"\n    Location: {loc_str}"

    return result


def main():
    """Main function to get experience by person ID"""
    # Get person ID from command line or use default
    if len(sys.argv) > 1:
        try:
            person_id = int(sys.argv[1])
        except ValueError:
            print(f"Error: '{sys.argv[1]}' is not a valid person ID (must be an integer)")
            sys.exit(1)
    else:
        person_id = 36601930  # Default: Daniel Sun

    client = HarmonicExperienceClient()

    print(f"Fetching experience for person ID: {person_id}")
    print("=" * 60)

    result = client.get_person_experience(person_id)
    experience_list = result.get("experience", [])

    print(f"\nEXPERIENCE ({len(experience_list)} entries):")
    print("-" * 40)

    for exp in experience_list:
        print(format_experience(exp))
        print()

    # Save full results to file
    output_path = Path(__file__).parent / "experience_output.json"
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Full results saved to: {output_path}")


if __name__ == "__main__":
    main()
