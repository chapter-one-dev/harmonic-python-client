# Harmonic Python Client

A Python client for interacting with the [Harmonic](https://harmonic.ai) API.

## Installation

```bash
pip install git+https://github.com/chapter-one-dev/harmonic-python-client.git
```

## Configuration

Set the following environment variables (or create a `.env` file):

```bash
HARMONIC_API_TOKEN=Bearer <your-token>
```

## Usage

### Get Full Profile (highlights, experience, education)

```python
from harmonic_client import HarmonicFullProfileClient

client = HarmonicFullProfileClient()

# Get full profile by Harmonic person ID
profile = client.get_full_profile(179915866)

print(f"Highlights: {profile['highlights']}")
print(f"Education: {profile['education']}")
print(f"Experience: {profile['experience']}")
```

### Get Person Highlights Only

```python
from harmonic_client import HarmonicFullProfileClient

client = HarmonicFullProfileClient()
highlights = client.get_person_highlights(179915866)
# Returns: ['Top University', 'Current Student']
```

### Search for Companies

```python
from harmonic_client import HarmonicClient

client = HarmonicClient()
# Use with saved search IDs
```

## Modules

- `HarmonicClient` - Main client for company searches and saved searches
- `HarmonicFullProfileClient` - Get person profiles with highlights, education, experience
- `HarmonicParser` - Parse Harmonic API responses
- `HarmonicUtils` - Utility functions for loading GraphQL queries

## Development

```bash
# Clone the repo
git clone https://github.com/chapter-one-dev/harmonic-python-client.git
cd harmonic-python-client

# Install in development mode
pip install -e ".[dev]"
```

## License

MIT
