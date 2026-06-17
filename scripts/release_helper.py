import os
import sys
import json
import re
import subprocess
from pathlib import Path

def parse_semver(version_str):
    # Match standard semver: X.Y.Z
    match = re.match(r'^v?(\d+)\.(\d+)\.(\d+)$', version_str.strip())
    if not match:
        return None
    return tuple(int(x) for x in match.groups())

def format_semver(parts):
    return f"{parts[0]}.{parts[1]}.{parts[2]}"

def main():
    # 1. Parse Event Payload
    event_path = os.getenv("GITHUB_EVENT_PATH")
    if not event_path:
        print("Error: GITHUB_EVENT_PATH is not set.", file=sys.stderr)
        sys.exit(1)
        
    try:
        with open(event_path, 'r') as f:
            event = json.load(f)
    except Exception as e:
        print(f"Error: Failed to read event JSON: {e}", file=sys.stderr)
        sys.exit(1)
        
    release = event.get("release", {})
    tag_name = release.get("tag_name", "")
    if not tag_name:
        print("Error: Release tag name is missing from event payload.", file=sys.stderr)
        sys.exit(1)
        
    tag_version_parts = parse_semver(tag_name)
    if not tag_version_parts:
        print(f"Error: Release tag '{tag_name}' is not a valid semantic version (X.Y.Z).", file=sys.stderr)
        sys.exit(1)
        
    tag_version = format_semver(tag_version_parts)
    
    # 2. Read current version from pyproject.toml
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        print("Error: pyproject.toml not found.", file=sys.stderr)
        sys.exit(1)
        
    try:
        content = pyproject_path.read_text()
        # Find version line: version = "..."
        version_match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if not version_match:
            print("Error: Could not find version in pyproject.toml", file=sys.stderr)
            sys.exit(1)
        current_version_str = version_match.group(1)
    except Exception as e:
        print(f"Error reading pyproject.toml: {e}", file=sys.stderr)
        sys.exit(1)
        
    current_version_parts = parse_semver(current_version_str)
    if not current_version_parts:
        print(f"Error: Current version '{current_version_str}' in pyproject.toml is not a valid semver.", file=sys.stderr)
        sys.exit(1)
        
    current_version = format_semver(current_version_parts)
    
    # 3. Parse bump flag from release body or title
    body = release.get("body", "") or ""
    title = release.get("title", "") or ""
    
    # Look for [bump: type] or bump: type (case insensitive)
    # Types: patch, minor, major, none, skip
    bump_regex = re.compile(r'(?:\[bump:\s*|bump:\s*)(patch|minor|major|none|skip)\]?', re.IGNORECASE)
    
    # Search title first, then body
    bump_match = bump_regex.search(title) or bump_regex.search(body)
    
    if not bump_match:
        # Check if version is already bumped
        if current_version == tag_version:
            # If the version in pyproject.toml matches the tag version,
            # we can assume no bump is needed (skip)
            bump_type = "none"
        else:
            raise ValueError(
                f"Missing version bump flag in the release title or body. "
                f"Please specify '[bump: patch]', '[bump: minor]', '[bump: major]', or '[bump: none]'."
            )
    else:
        bump_type = bump_match.group(1).lower()
        if bump_type == 'skip':
            bump_type = 'none'
        
    # Calculate expected target version
    c_major, c_minor, c_patch = current_version_parts
    if bump_type == 'major':
        expected_version_parts = (c_major + 1, 0, 0)
    elif bump_type == 'minor':
        expected_version_parts = (c_major, c_minor + 1, 0)
    elif bump_type == 'patch':
        expected_version_parts = (c_major, c_minor, c_patch + 1)
    else:  # none
        expected_version_parts = current_version_parts
        
    expected_version = format_semver(expected_version_parts)
    
    # 4. Strict Validation
    if current_version == tag_version:
        # Version already matches, no bump needed.
        bump_needed = False
        target_version = tag_version
    else:
        # Compare expected version with tag version
        if tag_version != expected_version:
            raise ValueError(
                f"Version mismatch! Release tag is '{tag_name}' (parsed as '{tag_version}'), "
                f"but based on bump flag '{bump_type}' and current version '{current_version}', "
                f"the expected version is '{expected_version}'."
            )
        bump_needed = (bump_type != 'none')
        target_version = expected_version
        
    # Output to GitHub Actions environment
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"bump_needed={'true' if bump_needed else 'false'}\n")
            f.write(f"bump_type={bump_type}\n")
            f.write(f"new_version={target_version}\n")
            
    print("--- Validation Successful ---")
    print(f"Current version: {current_version}")
    print(f"Bump flag:       {bump_type}")
    print(f"Target version:  {target_version}")
    print(f"Bump needed:     {bump_needed}")

if __name__ == "__main__":
    github_repository = os.getenv("GITHUB_REPOSITORY", "")
    github_run_id = os.getenv("GITHUB_RUN_ID", "")
    tag_name = "unknown"
    
    try:
        # Parse tag name early for error reporting
        event_path = os.getenv("GITHUB_EVENT_PATH")
        if event_path and Path(event_path).exists():
            with open(event_path, 'r') as f:
                event = json.load(f)
                tag_name = event.get("release", {}).get("tag_name", "unknown")
    except Exception:
        pass

    try:
        main()
    except Exception as e:
        error_msg = str(e)
        print(f"Validation Error: {error_msg}", file=sys.stderr)
        
        # Notify maintainer by creating an issue via 'gh' CLI
        if github_repository:
            title = f"Deployment Failed for Release {tag_name}"
            body = f"""### ❌ Deployment Failed

The publication workflow failed during validation or version bumping.

**Error Details:**
```
{error_msg}
```

**Workflow Run:** https://github.com/{github_repository}/actions/runs/{github_run_id}

Please check the release configuration and re-run/re-publish.
"""
            try:
                subprocess.run([
                    "gh", "issue", "create",
                    "--title", title,
                    "--body", body,
                    "--label", "bug"
                ], check=True)
                print("Successfully filed a notification issue.")
            except Exception as notify_err:
                print(f"Failed to create notification issue: {notify_err}", file=sys.stderr)
            
        sys.exit(1)
