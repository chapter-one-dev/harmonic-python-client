"""
Harmonic Person Profile API Client

Fetches profile information for a person from the Harmonic GraphQL API.
"""

import requests
import json
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from pathlib import Path
from harmonic_client.error_notifier import HarmonicErrorNotifier

# Load .env from harmonic_client directory
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


class HarmonicProfileClient:
    """Client for fetching person profile data from Harmonic API"""

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
        Get experience/work history for a person by their Harmonic ID.

        Args:
            person_id: The Harmonic person ID (e.g., 91458553)

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

    def get_full_profile(self, person_id: int) -> Dict[str, Any]:
        """
        Get all available profile information for a person.

        Args:
            person_id: The Harmonic person ID

        Returns:
            Dictionary containing all profile data
        """
        profile = {
            "person_id": person_id,
            "experience": None,
        }

        # Get experience
        try:
            experience_data = self.get_person_experience(person_id)
            profile["experience"] = experience_data.get("experience", [])
        except Exception as e:
            print(f"Error fetching experience: {e}")

        return profile


def main():
    """Example usage"""
    client = HarmonicProfileClient()

    # Example: Get experience for person ID 91458553
    person_id = 91458553

    print(f"Fetching profile for person ID: {person_id}")
    print("-" * 50)

    # Get experience data
    experience_data = client.get_person_experience(person_id)

    # Pretty print the result
    print(json.dumps(experience_data, indent=2))

    # Save to file
    output_path = Path(__file__).parent / "profile_output.json"
    with open(output_path, "w") as f:
        json.dump(experience_data, f, indent=2)
    print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    main()
