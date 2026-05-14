"""Shared fixtures for ddt unit tests."""
import pytest


# Sample raw records matching the GitHub repos API shape
GITHUB_REPO_RECORDS = [
    {
        "id": 1,
        "name": "tapestry3",
        "full_name": "apache/tapestry3",
        "description": "Mirror of Apache Tapestry 3",
        "html_url": "https://github.com/apache/tapestry3",
        "language": "Java",
        "stargazers_count": 5,
        "forks_count": 11,
        "created_at": "2009-03-27T15:41:52Z",
        "updated_at": "2025-04-14T03:46:39Z",
        "owner": {"login": "apache", "id": 47359},
        "topics": ["java", "tapestry", "web-framework"],
    },
    {
        "id": 2,
        "name": "apr-iconv",
        "full_name": "apache/apr-iconv",
        "description": None,     # null description
        "html_url": "https://github.com/apache/apr-iconv",
        "language": None,         # null language
        "stargazers_count": 3,
        "forks_count": 2,
        "created_at": "2009-03-27T15:41:52Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "owner": {"login": "apache", "id": 47359},
        "topics": [],
    },
]
