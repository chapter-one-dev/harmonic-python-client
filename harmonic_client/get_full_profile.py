"""
Harmonic Full Person Profile API Client

Fetches ALL available information for a person from the Harmonic GraphQL API:
- Basic profile info (name, picture, socials, location)
- Education history
- Work experience
"""

import requests
import json
import os
import sys
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from pathlib import Path
from harmonic_client.error_notifier import HarmonicErrorNotifier

# Load .env from harmonic_client directory
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


class HarmonicFullProfileClient:
    """Client for fetching complete person profile data from Harmonic API"""

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

    def get_education(self, person_id: int) -> List[Dict[str, Any]]:
        """Get education history"""
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

        result = data.get("data", {}).get("getPersonById", {})
        return result.get("education", [])

    def get_experience(self, person_id: int) -> List[Dict[str, Any]]:
        """Get work experience history"""
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

        result = data.get("data", {}).get("getPersonById", {})
        return result.get("experience", [])

    def get_person_highlights(self, person_id: int) -> List[str]:
        """
        Get person highlights (tags) like "Top University", "Current Student", etc.

        Args:
            person_id: The Harmonic person ID

        Returns:
            List of highlight category strings
        """
        payload = {
            "operationName": "GetPersonProfileHeader",
            "variables": {"id": person_id},
            "query": """query GetPersonProfileHeader($id: Int!) {
  getPersonById(id: $id) {
    id
    fullName
    highlights {
      category
      text
      __typename
    }
    __typename
  }
}"""
        }

        data = self._make_request("GetPersonProfileHeader", payload)

        if "errors" in data:
            return []

        person = data.get("data", {}).get("getPersonById", {})
        highlights = person.get("highlights", [])
        # Return deduplicated category names
        categories = [h.get("category", "") for h in highlights if h.get("category")]
        return list(dict.fromkeys(categories))

    def get_full_profile(self, person_id: int) -> Dict[str, Any]:
        """
        Get ALL available profile information for a person.

        Args:
            person_id: The Harmonic person ID

        Returns:
            Dictionary containing all profile data:
            - highlights: list of highlight categories (e.g., "Top University")
            - education: list of education entries
            - experience: list of work experience entries (includes company details)
        """
        profile = {
            "person_id": person_id,
            "highlights": [],
            "education": [],
            "experience": [],
            "errors": []
        }

        # Get highlights
        try:
            profile["highlights"] = self.get_person_highlights(person_id)
        except Exception as e:
            profile["errors"].append(f"Highlights: {str(e)}")

        # Get education
        try:
            profile["education"] = self.get_education(person_id)
        except Exception as e:
            profile["errors"].append(f"Education: {str(e)}")

        # Get experience
        try:
            profile["experience"] = self.get_experience(person_id)
        except Exception as e:
            profile["errors"].append(f"Experience: {str(e)}")

        return profile


def format_education(edu: Dict[str, Any]) -> str:
    """Format an education entry for display"""
    school = edu.get("school", {})
    school_name = school.get("name", "Unknown School")
    degree = edu.get("degree", "")
    field = edu.get("field", "")

    start = edu.get("startDate", "")
    end = edu.get("endDate", "")
    start_year = start[:4] if start else ""
    end_year = end[:4] if end else ""

    date_str = ""
    if start_year and end_year:
        date_str = f" ({start_year}-{end_year})"
    elif end_year:
        date_str = f" ({end_year})"

    parts = []
    if degree:
        parts.append(degree)
    if field:
        parts.append(field)
    desc = " in ".join(parts) if parts else "No degree info"

    return f"  {school_name}{date_str}\n    {desc}"


def format_experience(exp: Dict[str, Any]) -> str:
    """Format an experience entry for display"""
    company = exp.get("company", {})
    company_name = company.get("name", "Unknown Company")
    title = exp.get("title", "Unknown Title")
    is_current = exp.get("isCurrentPosition", False)

    start = exp.get("startDate", "")
    end = exp.get("endDate", "")
    start_year = start[:4] if start else ""
    end_year = end[:4] if end else "Present" if is_current else ""

    date_str = ""
    if start_year and end_year:
        date_str = f" ({start_year}-{end_year})"
    elif start_year:
        date_str = f" ({start_year}-)"

    current_indicator = " [CURRENT]" if is_current else ""

    # Company details
    funding = company.get("funding", {})
    stage = funding.get("fundingStage", "")
    headcount = company.get("headcount")

    details = []
    if stage:
        details.append(f"Stage: {stage}")
    if headcount:
        details.append(f"Headcount: {headcount}")

    result = f"  {title} at {company_name}{date_str}{current_indicator}"
    if details:
        result += f"\n    {', '.join(details)}"

    return result


def main():
    """Main function to get full profile by person ID"""
    if len(sys.argv) > 1:
        try:
            person_id = int(sys.argv[1])
        except ValueError:
            print(f"Error: '{sys.argv[1]}' is not a valid person ID (must be an integer)")
            sys.exit(1)
    else:
        person_id = 36601930  # Default: Daniel Sun

    client = HarmonicFullProfileClient()

    print(f"Fetching FULL profile for person ID: {person_id}")
    print("=" * 70)

    profile = client.get_full_profile(person_id)

    # Display highlights
    highlights = profile.get("highlights", [])
    print(f"\nHIGHLIGHTS ({len(highlights)} tags):")
    print("-" * 50)
    if highlights:
        print(f"  {', '.join(highlights)}")
    else:
        print("  (none)")

    # Display education
    education = profile.get("education", [])
    print(f"\nEDUCATION ({len(education)} entries):")
    print("-" * 50)
    for edu in education:
        print(format_education(edu))
        print()

    # Display experience
    experience = profile.get("experience", [])
    print(f"EXPERIENCE ({len(experience)} entries):")
    print("-" * 50)
    for exp in experience:
        print(format_experience(exp))
        print()

    # Display any errors
    errors = profile.get("errors", [])
    if errors:
        print("ERRORS:")
        print("-" * 50)
        for err in errors:
            print(f"  - {err}")

    # Save full results to file
    output_path = Path(__file__).parent / "full_profile_output.json"
    with open(output_path, "w") as f:
        json.dump(profile, f, indent=2)
    print(f"Full results saved to: {output_path}")


if __name__ == "__main__":
    main()
