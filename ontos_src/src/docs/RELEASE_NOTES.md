# Ontos Release Notes

This document contains the release history for Ontos. For a complete list of changes, run:

```bash
python src/scripts/generate_release_notes.py [since-tag]
```

---

## Version 0.4.6 (January 2026)

### ✨ Features

- **Multi-Platform Connector Architecture**: Pluggable connectors for Unity Catalog, Snowflake, Kafka, and Power BI
- **Unified Asset Type System**: Platform-agnostic asset types for consistent governance across systems
- **Unity Catalog Metrics**: First-class support for UC metrics in datasets and data products
- **Declarative Pipelines**: Support for both DLT and Spark Declarative Pipelines (SDP)
- **Dataset Instance Roles**: Added "Undefined" role option for dataset instances
- **Full Internationalization**: Asset types localized in 6 languages (EN, DE, ES, FR, IT, JA)

### 🐛 Bug Fixes

- Fixed asset type display names to use "Unity Catalog" instead of "UC" abbreviation

### 📚 Documentation

- Updated USER-GUIDE.md with multi-platform support documentation
- Added comprehensive asset type documentation

---

## Version 0.4.5

### ✨ Features

- Initial multi-platform dataset support
- Data Contract Server configuration for external systems
- ODCS-compliant server type definitions

---

## Version 0.4.0

### ✨ Features

- Data Products and Data Contracts management
- Dataset instances with environment tracking
- Business glossary integration
- Compliance policy engine with DSL
- Data asset review workflows

---

## Generating Release Notes

To generate release notes from git commits:

```bash
# Generate notes since a specific tag
python src/scripts/generate_release_notes.py v0.4.5

# Generate all notes
python src/scripts/generate_release_notes.py
```

The script categorizes commits by type (feat, fix, docs, perf, refactor, test, chore) based on conventional commit messages.

