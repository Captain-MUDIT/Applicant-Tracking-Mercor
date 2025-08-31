import os
import json
from datetime import datetime
from pyairtable import Table

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")   # set in environment
BASE_ID = "appEryE1iXzG6VZWM"
APPLICANTS_TABLE = "Applicants"
PERSONAL_TABLE = "Personal Details"
WORK_TABLE = "Work Experience"
SALARY_TABLE = "Salary Preferences"

# connect to tables
applicants = Table(AIRTABLE_API_KEY, BASE_ID, APPLICANTS_TABLE)
personal = Table(AIRTABLE_API_KEY, BASE_ID, PERSONAL_TABLE)
work = Table(AIRTABLE_API_KEY, BASE_ID, WORK_TABLE)
salary = Table(AIRTABLE_API_KEY, BASE_ID, SALARY_TABLE)


# Helper function: calculate years
def calculate_years(start, end=None):
    """Calculate duration in years given start and end dates."""
    if not start:
        return 0
    try:
        d1 = datetime.strptime(start, "%Y-%m-%d")
        if end:
            d2 = datetime.strptime(end, "%Y-%m-%d")
        else:
            d2 = datetime.today()
        return round((d2 - d1).days / 365, 1)
    except Exception:
        return 0


# Compress one applicant
def compress_applicant(applicant_id):
    """Collect child data and save compressed JSON for one applicant"""

    # 1. Fetch Personal Details (one-to-one)
    personal_records = personal.all(formula=f"{{Applicant}} = '{applicant_id}'")
    personal_data = {}
    if personal_records:
        f = personal_records[0]["fields"]
        personal_data = {
            "Full Name": f.get("Full Name"),
            "Location": f.get("Location")
        }

    # 2. Fetch Work Experience (one-to-many, with years only)
    work_records = work.all(formula=f"{{Applicant}} = '{applicant_id}'")
    work_data = [
        {
            "company": r["fields"].get("Company"),
            "title": r["fields"].get("Title"),
            "years": calculate_years(
                r["fields"].get("Start"),
                r["fields"].get("End")
            )
        }
        for r in work_records
    ]

    # 3. Fetch Salary Preferences (one-to-one, only selected fields)
    salary_records = salary.all(formula=f"{{Applicant}} = '{applicant_id}'")
    salary_data = {}
    if salary_records:
        f = salary_records[0]["fields"]
        salary_data = {
            "Preferred Rate": f.get("Preferred Rate"),
            "Currency": f.get("Currency"),
            "Availability": f.get("Availability")
        }

    # 4. Build compressed JSON
    compressed = {
        "personal": personal_data,
        "experience": work_data,
        "salary": salary_data
    }

    # 5. Save JSON back into Applicants table
    applicants.update(applicant_id, {"Compressed JSON": json.dumps(compressed)})

    print(f"Compressed Applicant {applicant_id} → JSON saved.")


# Run for all applicants
if __name__ == "__main__":
    for row in applicants.all():
        compress_applicant(row["id"])

