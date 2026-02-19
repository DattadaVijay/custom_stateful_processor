#!/usr/bin/env python3
"""
Generate release notes from git commit history.

Usage:
    python generate_release_notes.py [since-tag] [--output FILE]

Examples:
    python generate_release_notes.py                    # All commits, print to stdout
    python generate_release_notes.py v0.4.5            # Since tag v0.4.5
    python generate_release_notes.py --output notes.md # Write to file
    python generate_release_notes.py v0.4.5 --output src/docs/RELEASE_NOTES.md
"""
import subprocess
import re
import argparse
from datetime import datetime
from pathlib import Path

CATEGORIES = {
    "feat": "✨ Features",
    "fix": "🐛 Bug Fixes", 
    "docs": "📚 Documentation",
    "perf": "⚡ Performance",
    "refactor": "♻️ Refactoring",
    "test": "🧪 Testing",
    "chore": "🔧 Maintenance",
}

# Path to the main release notes file
RELEASE_NOTES_PATH = Path(__file__).parent.parent / "docs" / "RELEASE_NOTES.md"


def get_current_version() -> str:
    """Extract current version from pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    if pyproject_path.exists():
        content = pyproject_path.read_text()
        match = re.search(r'version\s*=\s*"([^"]+)"', content)
        if match:
            return match.group(1)
    return "unknown"


def generate_notes(since_tag: str = None, version: str = None) -> str:
    """Generate release notes from git commits."""
    cmd = ["git", "log", "--oneline", "--no-merges"]
    if since_tag:
        cmd.append(f"{since_tag}..HEAD")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    commits = result.stdout.strip().split("\n")
    
    categorized = {cat: [] for cat in CATEGORIES.values()}
    categorized["🔄 Other"] = []
    
    for commit in commits:
        if not commit:
            continue
        match = re.match(r"^[a-f0-9]+ (\w+)(\(.+\))?: (.+)$", commit)
        if match:
            type_, scope, message = match.groups()
            category = CATEGORIES.get(type_, "🔄 Other")
            scope_prefix = f"**{scope[1:-1]}**: " if scope else ""
            categorized[category].append(f"- {scope_prefix}{message}")
        else:
            parts = commit.split(' ', 1)
            if len(parts) > 1:
                categorized["🔄 Other"].append(f"- {parts[1]}")
    
    version_str = version or get_current_version()
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    notes = [f"# Release Notes - v{version_str} ({date_str})\n"]
    
    for category, items in categorized.items():
        if items:
            notes.append(f"\n## {category}\n")
            notes.extend(items)
    
    return "\n".join(notes)


def main():
    parser = argparse.ArgumentParser(
        description="Generate release notes from git commit history.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "since_tag", 
        nargs="?", 
        default=None,
        help="Generate notes since this git tag (e.g., v0.4.5)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (default: print to stdout)"
    )
    parser.add_argument(
        "--version", "-v",
        type=str,
        default=None,
        help="Version string to use in header (default: read from pyproject.toml)"
    )
    
    args = parser.parse_args()
    
    notes = generate_notes(args.since_tag, args.version)
    
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(notes)
        print(f"Release notes written to: {output_path}")
    else:
        print(notes)


if __name__ == "__main__":
    main()