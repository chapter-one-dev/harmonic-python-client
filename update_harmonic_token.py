#!/usr/bin/env python3
"""
Update Harmonic API Token in Google Cloud Secret Manager

This script updates the HARMONIC_API_TOKEN secret used by Cloud Run jobs.
Cloud Run jobs automatically pick up the latest version of the secret.

Usage:
    python update_harmonic_token.py "Bearer eyJhbGciOiJSUzI1..."

    # Or pipe from clipboard on macOS:
    pbpaste | python update_harmonic_token.py

    # Or interactively:
    python update_harmonic_token.py
    (paste token and press Enter)
"""

import sys
import subprocess
import re


# Configuration
PROJECT_ID = "chapter-one-340115"
SECRET_NAME = "harmonic-api-token"

# Cloud Run jobs that use this secret (for reference)
JOBS_USING_SECRET = [
    "harmonic-saved-searches",
]


def validate_token(token: str) -> str:
    """Validate and normalize the token format."""
    token = token.strip()

    # Remove quotes if present
    if (token.startswith('"') and token.endswith('"')) or \
       (token.startswith("'") and token.endswith("'")):
        token = token[1:-1]

    # Ensure it starts with "Bearer "
    if not token.startswith("Bearer "):
        if token.startswith("eyJ"):
            # Looks like a JWT without "Bearer " prefix
            token = f"Bearer {token}"
        else:
            raise ValueError("Token must start with 'Bearer ' or be a valid JWT")

    # Basic JWT structure validation (Bearer + 3 base64 parts separated by dots)
    jwt_part = token[7:]  # Remove "Bearer "
    parts = jwt_part.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid JWT format: expected 3 parts separated by dots")

    return token


def update_secret(token: str) -> bool:
    """Update the secret in Google Cloud Secret Manager."""
    print(f"Updating secret '{SECRET_NAME}' in project '{PROJECT_ID}'...")

    # Add new version of the secret
    cmd = [
        "gcloud", "secrets", "versions", "add", SECRET_NAME,
        f"--project={PROJECT_ID}",
        "--data-file=-"
    ]

    try:
        result = subprocess.run(
            cmd,
            input=token.encode(),
            capture_output=True,
            text=False
        )

        if result.returncode != 0:
            print(f"Error: {result.stderr.decode()}")
            return False

        # Parse the version number from output
        output = result.stderr.decode() + result.stdout.decode()
        version_match = re.search(r"versions/(\d+)", output)
        version = version_match.group(1) if version_match else "latest"

        print(f"✓ Secret updated successfully (version: {version})")
        return True

    except FileNotFoundError:
        print("Error: gcloud CLI not found. Please install Google Cloud SDK.")
        return False
    except Exception as e:
        print(f"Error updating secret: {e}")
        return False


def list_affected_jobs():
    """List Cloud Run jobs that will use the updated token."""
    print("\nCloud Run jobs using this secret:")
    for job in JOBS_USING_SECRET:
        print(f"  - {job}")
    print("\nThese jobs will automatically use the new token on next execution.")


def main():
    # Get token from argument, stdin, or prompt
    if len(sys.argv) > 1:
        token = " ".join(sys.argv[1:])
    elif not sys.stdin.isatty():
        # Reading from pipe
        token = sys.stdin.read()
    else:
        # Interactive prompt
        print("Paste the Bearer token (press Enter when done):")
        token = input()

    try:
        token = validate_token(token)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Show token preview (first/last few chars)
    jwt_part = token[7:]
    preview = f"{jwt_part[:20]}...{jwt_part[-20:]}"
    print(f"\nToken preview: Bearer {preview}")
    print(f"Token length: {len(token)} characters")

    # Confirm update
    if sys.stdin.isatty():
        confirm = input("\nUpdate secret? [y/N]: ").strip().lower()
        if confirm != 'y':
            print("Aborted.")
            sys.exit(0)

    # Update the secret
    if update_secret(token):
        list_affected_jobs()
        print("\n✓ Done!")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
