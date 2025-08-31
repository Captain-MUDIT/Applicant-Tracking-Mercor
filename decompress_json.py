import os
import json
from pyairtable import Table

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
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


# Decompress one applicant
def decompress_applicant(applicant_id):
    """Read JSON from Applicants and upsert child records."""

    # 1. Get compressed JSON from Applicants
    row = applicants.get(applicant_id)
    compressed_json = row["fields"].get("Compressed JSON")

    if not compressed_json:
        print(f"⚠️ No JSON found for {applicant_id}")
        return

    data = json.loads(compressed_json)

    # 2. Personal Details (overwrite existing or create new)
    personal_records = personal.all(formula=f"{{Applicant}} = '{applicant_id}'")
    if data.get("personal"):
        personal_data = {
            "Full Name": data["personal"].get("Full Name"),
            "Location": data["personal"].get("Location"),
            "Applicant": [applicant_id]
        }
        if personal_records:
            personal.update(personal_records[0]["id"], personal_data)
        else:
            personal.create(personal_data)

    # 3. Work Experience (delete old, insert fresh)
    for rec in work.all(formula=f"{{Applicant}} = '{applicant_id}'"):
        work.delete(rec["id"])

    for exp in data.get("experience", []):
        work.create({
            "Company": exp.get("company"),
            "Title": exp.get("title"),
            "Years": exp.get("years"),
            "Applicant": [applicant_id]
        })

    # 4. Salary Preferences (overwrite existing or create new)
    salary_records = salary.all(formula=f"{{Applicant}} = '{applicant_id}'")
    if data.get("salary"):
        salary_data = {
            "Preferred Rate": data["salary"].get("Preferred Rate"),
            "Currency": data["salary"].get("Currency"),
            "Availability": data["salary"].get("Availability"),
            "Applicant": [applicant_id]
        }
        if salary_records:
            salary.update(salary_records[0]["id"], salary_data)
        else:
            salary.create(salary_data)

    print(f"Decompressed Applicant {applicant_id} → child tables updated.")

# Run for all applicants
if __name__ == "__main__":
    for row in applicants.all():
        decompress_applicant(row["id"])

