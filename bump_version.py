# bump_version.py
import re
from pathlib import Path

def bump_version():
    pyproject_path = Path('pyproject.toml')
    pyproject = pyproject_path.read_text()

    version_match = re.search(r'version\s*=\s*\"(.*?)\"', pyproject)
    if version_match:
        version = version_match.group(1)
        major, minor, patch = map(int, version.split('.'))

        # Increment patch version
        patch += 1
        new_version = f'{major}.{minor}.{patch}'

        # Update pyproject.toml with new version
        new_pyproject = re.sub(r'version\s*=\s*\"(.*?)\"', f'version = \"{new_version}\"', pyproject)
        pyproject_path.write_text(new_pyproject)

        # Write new version to a file
        with open('new_version.txt', 'w') as version_file:
            version_file.write(new_version)

        return new_version

if __name__ == "__main__":
    bump_version()