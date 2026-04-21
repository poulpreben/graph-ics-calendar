"""graph-ics-calendar: Microsoft 365 calendar ICS proxy."""

from dotenv import load_dotenv

# Load .env on package import so any entry point (CLI, uvicorn, pytest) sees
# the same variables. Existing process env wins over .env.
load_dotenv()

__all__ = ["__version__"]
__version__ = "0.1.0"
