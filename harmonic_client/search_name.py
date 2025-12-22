"""
Harmonic Name Search API Client

Search for people, companies, and investors by name using the Harmonic GraphQL API.
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


class HarmonicSearchClient:
    """Client for searching Harmonic data by name"""

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

    def search(self, query: str) -> Dict[str, Any]:
        """
        Search for people, companies, and investors by name.

        Args:
            query: The name to search for (e.g., "Daniel Sun")

        Returns:
            Dictionary containing companies, people, and investors matching the query
        """
        payload = {
            "operationName": "TypeaheadSearchWithInvestors",
            "variables": {
                "query": query
            },
            "query": """query TypeaheadSearchWithInvestors($query: String!) {
  ...TypeaheadCompaniesWithBase
  ...TypeaheadPeople
  ...TypeaheadInvestor
}

fragment TypeaheadCompaniesWithBase on Query {
  getCompaniesWithTypeahead(query: $query) {
    ...BaseCompany
    __typename
  }
  __typename
}

fragment BaseCompany on Company {
  id
  entityUrn
  name
  logoUrl
  description
  shortDescription
  userNotes
  teamNotes
  website {
    url
    domain
    __typename
  }
  location {
    state
    city
    country
    __typename
  }
  foundingDate {
    date
    __typename
  }
  funding {
    lastFundingType
    lastFundingAt
    fundingStage
    fundingTotal
    __typename
  }
  headcount
  webTraffic
  socials {
    ...Socials
    __typename
  }
  initializedDate
  updatedAt
  legal_name: legalName
  external_description: externalDescription
  logo_url: logoUrl
  created_at: initializedDate
  redirectUrn
  __typename
}

fragment Socials on Socials {
  linkedin {
    status
    url
    followerCount
    __typename
  }
  crunchbase {
    status
    url
    followerCount
    __typename
  }
  pitchbook {
    status
    url
    followerCount
    __typename
  }
  angellist {
    status
    url
    followerCount
    __typename
  }
  twitter {
    status
    url
    followerCount
    __typename
  }
  facebook {
    status
    url
    likeCount
    __typename
  }
  instagram {
    url
    followerCount
    status
    __typename
  }
  indeed {
    url
    followerCount
    status
    __typename
  }
  youtube {
    url
    followerCount
    status
    __typename
  }
  monster {
    url
    followerCount
    status
    __typename
  }
  stackoverflow {
    url
    followerCount
    status
    __typename
  }
  __typename
}

fragment TypeaheadPeople on Query {
  getPeopleWithTypeahead(query: $query) {
    ...GetPeopleWithTypeahead
    __typename
  }
  __typename
}

fragment GetPeopleWithTypeahead on Person {
  id
  fullName
  experience {
    company {
      id
      name
      __typename
    }
    title
    department
    isCurrentPosition
    __typename
  }
  watchlists {
    id
    __typename
  }
  profilePictureUrl
  __typename
}

fragment TypeaheadInvestor on Query {
  getInvestorsWithTypeahead(query: $query) {
    investors {
      ...GetInvestorWithTypeahead
      __typename
    }
    __typename
  }
  __typename
}

fragment GetInvestorWithTypeahead on InternalInvestor {
  entityUrn
  details {
    ... on Company {
      id
      name
      logoUrl
      location {
        city
        state
        country
        __typename
      }
      website {
        domain
        __typename
      }
      __typename
    }
    ... on Person {
      id
      name: fullName
      logoUrl: profilePictureUrl
      location {
        city
        state
        country
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}"""
        }

        data = self._make_request("TypeaheadSearchWithInvestors", payload)

        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        return data.get("data", {})

    def search_people(self, query: str) -> List[Dict[str, Any]]:
        """Search for people only and return the list"""
        results = self.search(query)
        return results.get("getPeopleWithTypeahead", [])

    def search_companies(self, query: str) -> List[Dict[str, Any]]:
        """Search for companies only and return the list"""
        results = self.search(query)
        return results.get("getCompaniesWithTypeahead", [])

    def search_investors(self, query: str) -> List[Dict[str, Any]]:
        """Search for investors only and return the list"""
        results = self.search(query)
        investors_data = results.get("getInvestorsWithTypeahead", {})
        return investors_data.get("investors", [])


def format_person(person: Dict[str, Any]) -> str:
    """Format a person result for display"""
    name = person.get("fullName", "Unknown")
    person_id = person.get("id", "N/A")

    # Get current position
    current_positions = [
        exp for exp in person.get("experience", [])
        if exp.get("isCurrentPosition")
    ]

    if current_positions:
        pos = current_positions[0]
        company = pos.get("company", {}).get("name", "Unknown")
        title = pos.get("title", "Unknown")
        position_str = f"{title} at {company}"
    else:
        position_str = "No current position"

    return f"  [{person_id}] {name} - {position_str}"


def format_company(company: Dict[str, Any]) -> str:
    """Format a company result for display"""
    name = company.get("name", "Unknown")
    company_id = company.get("id", "N/A")
    location = company.get("location")

    loc_str = ""
    if location:
        parts = [location.get("city"), location.get("state"), location.get("country")]
        loc_str = ", ".join([p for p in parts if p])

    funding = company.get("funding", {})
    stage = funding.get("fundingStage", "Unknown")

    return f"  [{company_id}] {name} ({stage}) - {loc_str or 'No location'}"


def format_investor(investor: Dict[str, Any]) -> str:
    """Format an investor result for display"""
    details = investor.get("details", {})
    name = details.get("name", "Unknown")
    investor_id = details.get("id", "N/A")
    entity_urn = investor.get("entityUrn", "")

    location = details.get("location")
    loc_str = ""
    if location:
        parts = [location.get("city"), location.get("state"), location.get("country")]
        loc_str = ", ".join([p for p in parts if p])

    return f"  [{investor_id}] {name} - {loc_str or 'No location'} ({entity_urn})"


def main():
    """Main function to search by name"""
    # Get search query from command line or use default
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = "Daniel Sun"

    client = HarmonicSearchClient()

    print(f"Searching for: '{query}'")
    print("=" * 60)

    results = client.search(query)

    # Display people results
    people = results.get("getPeopleWithTypeahead", [])
    print(f"\nPEOPLE ({len(people)} results):")
    print("-" * 40)
    for person in people[:20]:  # Limit display to 20
        print(format_person(person))
    if len(people) > 20:
        print(f"  ... and {len(people) - 20} more")

    # Display company results
    companies = results.get("getCompaniesWithTypeahead", [])
    print(f"\nCOMPANIES ({len(companies)} results):")
    print("-" * 40)
    for company in companies[:10]:  # Limit display to 10
        print(format_company(company))
    if len(companies) > 10:
        print(f"  ... and {len(companies) - 10} more")

    # Display investor results
    investors_data = results.get("getInvestorsWithTypeahead", {})
    investors = investors_data.get("investors", [])
    print(f"\nINVESTORS ({len(investors)} results):")
    print("-" * 40)
    for investor in investors:
        print(format_investor(investor))

    # Save full results to file
    output_path = Path(__file__).parent / "search_output.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nFull results saved to: {output_path}")


if __name__ == "__main__":
    main()
