import json
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

@dataclass
class CompanyData:
    company_id: Optional[int] = None
    entity_urn: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    short_description: Optional[str] = None
    headcount: Optional[int] = None
    funding_stage: Optional[str] = None
    funding_total: Optional[int] = None
    last_funding_type: Optional[str] = None
    last_funding_date: Optional[str] = None
    founding_date: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    logo_url: Optional[str] = None
    website_url: Optional[str] = None
    website_domain: Optional[str] = None
    redirect_urn: Optional[str] = None
    user_notes: Optional[str] = None
    team_notes: Optional[str] = None
    legal_name: Optional[str] = None
    external_description: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    ceo_ids: Optional[List[int]] = None
    ceo_linkedin_urls: Optional[List[str]] = None
    ceo_names: Optional[List[str]] = None
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the dataclass to a dictionary for CSV/JSON export"""
        return {
            'company_id': self.company_id,
            'entity_urn': self.entity_urn,
            'name': self.name,
            'description': self.description,
            'short_description': self.short_description,
            'headcount': self.headcount,
            'funding_stage': self.funding_stage,
            'funding_total': self.funding_total,
            'last_funding_type': self.last_funding_type,
            'last_funding_date': self.last_funding_date,
            'founding_date': self.founding_date,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'logo_url': self.logo_url,
            'website_url': self.website_url,
            'website_domain': self.website_domain,
            'redirect_urn': self.redirect_urn,
            'user_notes': self.user_notes,
            'team_notes': self.team_notes,
            'legal_name': self.legal_name,
            'external_description': self.external_description,
            'city': self.city,
            'state': self.state,
            'country': self.country,
            'ceo_ids': self.ceo_ids,
            'ceo_linkedin_urls': self.ceo_linkedin_urls,
            'ceo_names': self.ceo_names,
        }

class HarmonicParser:
    def __init__(self):
        pass

    def parse_company_to_dataclass(self, company_data: Dict[str, Any]) -> CompanyData:
        """
        Parse raw company data into a CompanyData dataclass matching BigQuery schema
        
        Args:
            company_data: Raw company data from the API response
            
        Returns:
            CompanyData dataclass instance
        """
        # Set created_at to current date and time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Extract funding data
        funding_data = company_data.get('funding', {})
        
        # Extract website data
        website_data = company_data.get('website', {})
        
        # Extract location data
        location_data = company_data.get('location', {})
        
        # Extract CEO data from person_relationships_founders_and_ceos
        ceo_ids = []
        ceo_linkedin_urls = []
        ceo_names = []
        
        person_relationships = company_data.get('person_relationships_founders_and_ceos', [])
        for person in person_relationships:
            ceo_ids.append(person.get('id'))
            ceo_names.append(person.get('fullName'))
            
            # Extract LinkedIn URL from socials
            socials = person.get('socials', {})
            if socials and 'linkedin' in socials and socials['linkedin'].get('url'):
                ceo_linkedin_urls.append(socials['linkedin'].get('url'))
            # Don't append None - only append actual URLs
        
        # Filter out None values from all lists to ensure BigQuery compatibility
        ceo_ids = [id for id in ceo_ids if id is not None]
        ceo_names = [name for name in ceo_names if name is not None]
        # ceo_linkedin_urls is already filtered above
        
        # Create the company dataclass with exact field mapping
        company = CompanyData(
            company_id=company_data.get('id'),
            entity_urn=company_data.get('entityUrn'),
            name=company_data.get('name'),
            description=company_data.get('description'),
            short_description=company_data.get('shortDescription'),
            headcount=company_data.get('headcount'),
            funding_stage=funding_data.get('fundingStage'),
            funding_total=funding_data.get('fundingTotal'),
            last_funding_type=funding_data.get('lastFundingType'),
            last_funding_date=funding_data.get('lastFundingAt'),
            founding_date=company_data.get('foundingDate', {}).get('date') if company_data.get('foundingDate') else None,
            created_at=company_data.get('initializedDate'),  # Changed from created_at to initializedDate
            updated_at=company_data.get('updatedAt'),
            logo_url=company_data.get('logoUrl'),  # Changed from logo_url to logoUrl
            website_url=website_data.get('url'),
            website_domain=website_data.get('domain'),
            redirect_urn=company_data.get('redirectUrn'),
            user_notes=company_data.get('userNotes'),
            team_notes=company_data.get('teamNotes'),
            legal_name=company_data.get('legal_name'),
            external_description=company_data.get('external_description'),
            city=location_data.get('city'),
            state=location_data.get('state'),
            country=location_data.get('country'),
            ceo_ids=ceo_ids,  # List of all CEO IDs
            ceo_linkedin_urls=ceo_linkedin_urls,  # List of all CEO LinkedIn URLs
            ceo_names=ceo_names,  # List of all CEO names
            timestamp=timestamp
        )
        
        return company

    def parse_harmonic_response_to_dataclasses(self, response_data: Dict[str, Any]) -> List[CompanyData]:
        """
        Parse the Harmonic API response and extract company information as dataclasses
        
        Args:
            response_data: The raw response from the Harmonic API
            
        Returns:
            List of CompanyData dataclass instances
        """
        companies = []
        
        # Handle getCompaniesByIds response structure
        if 'data' in response_data and 'getCompaniesByIds' in response_data['data']:
            companies_data = response_data['data']['getCompaniesByIds']
            for company_data in companies_data:
                company = self.parse_company_to_dataclass(company_data)
                companies.append(company)
        
        # Handle getSavedSearch response structure (corrected path)
        elif 'data' in response_data and 'getSavedSearch' in response_data['data']:
            results = response_data['data']['getSavedSearch']['results']
            edges = results.get('edges', [])
            
            for edge in edges:
                company_data = edge['node']['entity']
                company = self.parse_company_to_dataclass(company_data)
                companies.append(company)
        
        return companies

    def extract_company_ids(self, response_data: Dict[str, Any]) -> List[int]:
        """
        Extract only the company IDs from the Harmonic API response
        
        Args:
            response_data: The raw response from the Harmonic API or paginated response
            
        Returns:
            List of company IDs as integers
        """
        company_ids = []
        
        # Handle paginated response structure (new format)
        if 'companies' in response_data and isinstance(response_data['companies'], list):
            edges = response_data['companies']
            for edge in edges:
                if 'node' in edge and 'entity' in edge['node']:
                    company_data = edge['node']['entity']
                    company_id = company_data.get('id')
                    if company_id:
                        company_ids.append(company_id)  # Keep as integer
        
        # Handle original GraphQL response structure
        elif 'data' in response_data and 'getSavedSearch' in response_data['data']:
            results = response_data['data']['getSavedSearch']['results']
            edges = results.get('edges', [])
            
            for edge in edges:
                if 'node' in edge and 'entity' in edge['node']:
                    company_data = edge['node']['entity']
                    company_id = company_data.get('id')
                    if company_id:
                        company_ids.append(company_id)  # Keep as integer
        
        return company_ids

    def save_to_csv(self, companies: List[Dict[str, Any]], filename: str = None):
        """
        Save the parsed company data to a CSV file
        
        Args:
            companies: List of company dictionaries
            filename: Output CSV filename (optional)
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"harmonic_client/harmonic_companies_{timestamp}.csv"
        
        if not companies:
            print("No companies to save")
            return
        
        # Get all possible field names from all companies
        fieldnames = set()
        for company in companies:
            fieldnames.update(company.keys())
        
        fieldnames = sorted(list(fieldnames))
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for company in companies:
                # Ensure all fields are present (fill missing ones with None)
                row = {field: company.get(field) for field in fieldnames}
                writer.writerow(row)
        
        print(f"Saved {len(companies)} companies to {filename}")

    def save_to_json(self, companies: List[Dict[str, Any]], filename: str = None):
        """
        Save the parsed company data to a JSON file
        
        Args:
            companies: List of company dictionaries
            filename: Output JSON filename (optional)
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"harmonic_client/harmonic_companies_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(companies, jsonfile, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(companies)} companies to {filename}")

    def print_summary(self, companies: List[Dict[str, Any]]):
        """
        Print a summary of the parsed data
        
        Args:
            companies: List of company dictionaries
        """
        if not companies:
            print("No companies found")
            return
        
        print(f"\n=== HARMONIC COMPANIES SUMMARY ===")
        print(f"Total companies: {len(companies)}")
        
        # Count by funding stage
        funding_stages = {}
        for company in companies:
            stage = company.get('funding_stage', 'Unknown')
            funding_stages[stage] = funding_stages.get(stage, 0) + 1
        
        print(f"\nFunding Stages:")
        for stage, count in sorted(funding_stages.items()):
            print(f"  {stage}: {count}")
        
        # Count by country
        countries = {}
        for company in companies:
            country = company.get('country', 'Unknown')
            countries[country] = countries.get(country, 0) + 1
        
        print(f"\nCountries:")
        for country, count in sorted(countries.items()):
            print(f"  {country}: {count}")
        
        # Count by city
        cities = {}
        for company in companies:
            city = company.get('city', 'Unknown')
            cities[city] = cities.get(city, 0) + 1
        
        print(f"\nTop Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {city}: {count}")
        
        # Sample companies
        print(f"\nSample Companies:")
        for i, company in enumerate(companies[:5]):
            print(f"  {i+1}. {company.get('name', 'Unknown')} - {company.get('city', 'Unknown')}, {company.get('country', 'Unknown')}")
            print(f"     Funding: {company.get('funding_stage', 'Unknown')} | Headcount: {company.get('headcount', 'Unknown')}")

    def main(self):
        """
        Main function to demonstrate usage
        """
        # Example usage with your data
        print("Harmonic Data Parser")
        print("====================")
        
        # You would typically load this from your API response
        # For now, you can paste your response data here or load from a file
        
        # Example: Load from a JSON file
        with open('harmonic_client/output.json', 'r') as f:
            response_data = json.load(f)

        # Parse the data
        companies = self.parse_harmonic_response_to_dataclasses(response_data)
        
        # Print summary
        self.print_summary(companies)
        
        # Save to files
        self.save_to_csv(companies)
        self.save_to_json(companies)

if __name__ == "__main__":
    parser = HarmonicParser()
    parser.main()