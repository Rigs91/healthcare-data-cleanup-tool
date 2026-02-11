#!/usr/bin/env python3
"""
Generate a large, intentionally messy, multidomain healthcare dataset in CSV format.

The dataset blends EHR, claims, labs, pharmacy, SDOH, and device telemetry fields,
with deliberate quality issues to stress-test cleanup and profiling pipelines.
"""

from __future__ import annotations

import argparse
import csv
import random
import string
import time
from datetime import date, datetime, timedelta
from pathlib import Path


MISSING_TOKENS = ["", " ", "N/A", "NULL", "?", "unknown", "na", "none"]
BOOL_TOKENS = ["Y", "N", "Yes", "No", "true", "false", "1", "0", "T", "F", "Unknown"]
SEX_TOKENS = ["M", "F", "Male", "Female", "U", "Unknown", "Non-binary", "X"]

FIRST_NAMES = [
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "William",
    "Elizabeth",
    "David",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
]
LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
]
CITIES = [
    "Boston",
    "Chicago",
    "Dallas",
    "Phoenix",
    "Atlanta",
    "Seattle",
    "Denver",
    "Miami",
    "Detroit",
    "Portland",
]
STATES = ["CA", "TX", "FL", "NY", "WA", "AZ", "IL", "GA", "OH", "PA", "NC", "MI"]

PAYERS = [
    "Aetna",
    "Cigna",
    "United",
    "Humana",
    "BCBS",
    "Medicare",
    "Medicaid",
    "Self Pay",
]
ENCOUNTER_TYPES = ["Inpatient", "Outpatient", "Emergency", "Observation", "Telehealth"]
ADMISSION_TYPES = ["Urgent", "Elective", "Emergency", "Newborn", "Trauma", "Unknown"]
CLAIM_STATUS = ["paid", "denied", "pending", "partial", "void", "reversed"]
ELIGIBILITY_STATUS = ["active", "inactive", "termed", "pending", "retro-denied"]
DENIAL_REASONS = ["CO-16", "CO-29", "CO-50", "PR-1", "M15", "N130", "none"]

ICD10_CODES = ["E11.9", "I10", "J45.909", "M54.5", "N18.3", "F41.1", "K21.9", "R07.9"]
ICD10_BAD = ["ZZZ", "123.45", "A", "I1O", "E1199X", "??", ""]
CPT_CODES = ["99213", "99214", "93000", "80053", "85025", "81001", "90658", "36415"]
CPT_BAD = ["99A13", "ABCDE", "12", "000000", "CPT?", ""]
LOINC_CODES = ["4548-4", "718-7", "6690-2", "2951-2", "2085-9", "777-3", "1975-2"]
LOINC_BAD = ["LOINC", "12-XYZ", "999999", "?", ""]
NDC_CODES = [
    "0002-8215-01",
    "0013-0554-46",
    "0054-0456-25",
    "54868-5882-0",
    "68382-1145-1",
]
NDC_BAD = ["NDC", "123", "00-00-00", "ABCDE", ""]
RXNORM = ["860975", "314077", "1049625", "617314", "197361", "1243029"]
RXNORM_BAD = ["RX", "12A45", "?", ""]

LAB_NAMES = ["A1C", "CBC", "CMP", "LIPID", "TSH", "CRP", "BMP", "UA"]
LAB_UNITS = ["mg/dL", "mmol/L", "g/dL", "IU/L", "x10^3/uL", "%", "mEq/L", "NULL"]
ABNORMAL_FLAGS = ["H", "L", "N", "A", "critical", "?", ""]

ROUTES = ["PO", "IV", "IM", "SC", "PR", "TOPICAL", "UNKNOWN"]
FREQUENCIES = ["BID", "TID", "QHS", "daily", "q6h", "PRN", "QOD"]

DEVICE_TYPES = ["watch", "glucometer", "bp-cuff", "pulse-ox", "scale", "sleep-band"]
SOURCE_SYSTEMS = ["Epic", "Cerner", "Athena", "ClaimsGateway", "LabHub", "PharmSwitch", "IoTBridge"]

SYMPTOMS = [
    "fatigue",
    "cough",
    "shortness of breath",
    "headache",
    "chest pain",
    "dizziness",
    "nausea",
    "abdominal pain",
    "back pain",
    "rash",
]
PLANS = [
    "follow-up in two weeks",
    "increase hydration",
    "start medication trial",
    "repeat labs in 48 hours",
    "urgent specialist referral",
    "home monitoring advised",
    "return if symptoms worsen",
    "diet and exercise counseling",
]

FIELDNAMES = [
    "Record ID",
    "sourceSystem",
    "Ingestion Batch",
    "Patient ID",
    "Member-ID",
    "MRN#",
    "Pt Name",
    "DOB",
    "Sex@Birth",
    "Gender Identity",
    "Phone Number",
    "E-mail",
    "SSN",
    "Address Line 1",
    "City",
    "State",
    "ZIP Code",
    "County",
    "Race/Ethnicity",
    "Preferred Language",
    "Marital Status",
    "Deceased?",
    "Death Date",
    "Encounter ID",
    "Encounter Date",
    "Discharge Date",
    "Admission Type",
    "Encounter Type",
    "Facility ID",
    "Provider NPI",
    "Primary ICD10",
    "Secondary ICD10",
    "Procedure CPT",
    "DRG Code",
    "Payer Name",
    "Plan ID",
    "Eligibility Status",
    "Prior Auth Req",
    "Claim ID",
    "Claim Status",
    "Billed Amount",
    "Allowed Amount",
    "Paid Amount",
    "Coinsurance %",
    "Patient Resp",
    "Denial Reason",
    "Lab Order ID",
    "LOINC",
    "Lab Name",
    "Specimen Collected",
    "Result Value",
    "Result Unit",
    "Reference Range",
    "Abnormal Flag",
    "Rx Order ID",
    "NDC",
    "RxNorm",
    "Medication Name",
    "Dose",
    "Dose Unit",
    "Route",
    "Frequency",
    "Days Supply",
    "Refill Count",
    "Prescriber NPI",
    "Housing Instability",
    "Food Insecurity",
    "Transport Barrier",
    "Employment Status",
    "Income Bracket",
    "Device ID",
    "Device Type",
    "Reading Timestamp",
    "Heart Rate",
    "SpO2",
    "Systolic BP",
    "Diastolic BP",
    "Glucose mg/dL",
    "Step Count",
    "Sleep Hours",
    "Clinical Note",
    "Raw Payload",
    "Consent Signed",
    "Data Share Opt-In",
    "Last Updated",
    "Source Lineage",
]


def maybe_missing(value: str, p: float = 0.03) -> str:
    if random.random() < p:
        return random.choice(MISSING_TOKENS)
    return value


def random_digits(length: int) -> str:
    return "".join(random.choices(string.digits, k=length))


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(0, delta)))


def noisy_date(value: date, include_time: bool = False, invalid_rate: float = 0.03) -> str:
    if random.random() < invalid_rate:
        return random.choice(["99/99/9999", "2027-13-40", "13-13-2013", "0000-00-00", ""])
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y", "%Y/%m/%d", "%m-%d-%y"]
    rendered = value.strftime(random.choice(formats))
    if include_time and random.random() < 0.35:
        hh = random.randint(0, 23)
        mm = random.randint(0, 59)
        ss = random.randint(0, 59)
        if random.random() < 0.5:
            rendered = f"{rendered} {hh:02d}:{mm:02d}:{ss:02d}"
        else:
            rendered = f"{rendered}T{hh:02d}:{mm:02d}:{ss:02d}Z"
    return rendered


def noisy_bool() -> str:
    return random.choice(BOOL_TOKENS)


def noisy_money(value: float) -> str:
    style = random.randint(0, 7)
    if style == 0:
        return f"{value:.2f}"
    if style == 1:
        return f"${value:,.2f}"
    if style == 2:
        return f" {value:.2f} "
    if style == 3:
        return f"{value:,.0f}"
    if style == 4:
        return f"({abs(value):,.2f})" if value < 0 else f"{value:,.2f}"
    if style == 5:
        return f"{value:.2f}%"
    if style == 6:
        return f"{value:,.2f} USD"
    return random.choice(["-", "n/a", "NULL", f"{value:.3f}"])


def noisy_percent(value: float) -> str:
    style = random.randint(0, 5)
    if style == 0:
        return f"{value:.2f}"
    if style == 1:
        return f"{value:.1f}%"
    if style == 2:
        return f"{value / 100:.3f}"
    if style == 3:
        return f" {value:.2f} "
    if style == 4:
        return random.choice(["N/A", "NULL", "?", "unknown"])
    return f"{value:.0f}%"


def choose_code(valid: list[str], invalid: list[str], invalid_rate: float = 0.15) -> str:
    return random.choice(invalid) if random.random() < invalid_rate else random.choice(valid)


def generate_name() -> str:
    if random.random() < 0.02:
        return random.choice(["", "UNKNOWN", "Test Patient", "DOE, JOHN", "MISSING"])
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def generate_phone() -> str:
    digits = random_digits(10)
    variants = [
        f"({digits[:3]}) {digits[3:6]}-{digits[6:]}",
        f"{digits[:3]}-{digits[3:6]}-{digits[6:]}",
        f"+1{digits}",
        f"{digits[:3]}.{digits[3:6]}.{digits[6:]}",
        digits,
    ]
    if random.random() < 0.08:
        variants.extend(["555-010", "N/A", "000-000-0000", ""])
    return random.choice(variants)


def generate_ssn() -> str:
    if random.random() < 0.1:
        return random.choice(["999-99-9999", "000-00-0000", "###-##-####", "N/A", ""])
    raw = random_digits(9)
    return f"{raw[:3]}-{raw[3:5]}-{raw[5:]}"


def generate_note(patient_name: str) -> str:
    symptom1 = random.choice(SYMPTOMS)
    symptom2 = random.choice(SYMPTOMS)
    plan = random.choice(PLANS)
    if random.random() < 0.3:
        severity = random.choice(["mild", "moderate", "severe", "intermittent"])
        return (
            f"Patient {patient_name} reports {severity} {symptom1} with intermittent {symptom2}; "
            f"vitals reviewed, medication reconciliation incomplete, {plan}."
        )
    return (
        f"Chief complaint includes {symptom1}; secondary issue {symptom2}. "
        f"Care team documented barriers to follow-up and recommended {plan}."
    )


def generate_payload(record_id: int, patient_id: str, claim_id: str, device_id: str) -> str:
    risk = random.choice(["low", "medium", "high", "critical", "unknown"])
    confidence = random.uniform(0.2, 0.99)
    tags = random.sample(
        ["fall-risk", "polypharmacy", "chronic-care", "readmission-risk", "social-needs", "duplicate-record"],
        k=3,
    )
    return (
        '{"record":'
        + str(record_id)
        + ',"patient":"'
        + patient_id
        + '","claim":"'
        + claim_id
        + '","device":"'
        + device_id
        + '","risk":"'
        + risk
        + '","confidence":"'
        + f"{confidence:.3f}"
        + '","tags":"'
        + "|".join(tags)
        + '","pipeline":"v1.3.7","notes":"raw-source-map unresolved, schema drift observed"}'
    )


def generate_row(
    record_id: int,
    patient_pool: list[str],
    claim_pool: list[str],
    base_batch: str,
) -> dict[str, str]:
    if patient_pool and random.random() < 0.12:
        patient_id = random.choice(patient_pool)
    else:
        patient_id = f"PT-{random_digits(8)}"
        patient_pool.append(patient_id)

    if claim_pool and random.random() < 0.1:
        claim_id = random.choice(claim_pool)
    else:
        claim_id = f"CLM-{random_digits(10)}"
        claim_pool.append(claim_id)

    member_id = f"MBR-{random_digits(9)}"
    mrn = f"MRN{random_digits(7)}"
    encounter_id = f"ENC-{random_digits(10)}"
    lab_order_id = f"LAB-{random_digits(9)}"
    rx_order_id = f"RXO-{random_digits(9)}"
    facility_id = f"FAC-{random_digits(5)}"
    plan_id = f"PLAN-{random_digits(6)}"
    device_id = f"DEV-{random_digits(10)}"

    provider_npi = random_digits(10)
    prescriber_npi = random_digits(10)
    city = random.choice(CITIES)
    state = random.choice(STATES)
    zip_code = random_digits(5)
    county = f"{city} County"

    dob_date = random_date(date(1930, 1, 1), date(2024, 12, 31))
    enc_date = random_date(date(2018, 1, 1), date(2026, 12, 31))
    discharge_delta = random.randint(-2, 10) if random.random() < 0.12 else random.randint(0, 10)
    discharge_date = enc_date + timedelta(days=discharge_delta)
    specimen_date = enc_date + timedelta(hours=random.randint(0, 48))
    read_ts = datetime.combine(enc_date, datetime.min.time()) + timedelta(
        hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)
    )

    billed = random.uniform(20, 250000)
    allowed = billed * random.uniform(0.15, 1.25)
    paid = allowed * random.uniform(-0.1, 1.05)
    patient_resp = billed - paid
    coinsurance = random.uniform(0, 100)

    glucose = random.uniform(40, 350)
    if random.random() < 0.03:
        glucose = random.uniform(-20, 800)

    heart_rate = random.randint(35, 180)
    if random.random() < 0.02:
        heart_rate = random.randint(0, 260)

    systolic = random.randint(85, 190)
    diastolic = random.randint(45, 130)
    spo2 = random.randint(75, 100)
    step_count = random.randint(0, 45000)
    sleep_hours = random.uniform(0, 14)

    patient_name = generate_name()
    note = generate_note(patient_name if patient_name else "UNKNOWN")
    denial_reason = random.choice(DENIAL_REASONS)
    if random.choice(CLAIM_STATUS) == "paid":
        denial_reason = random.choice(["none", "", "N/A"])

    row = {
        "Record ID": f"R-{record_id:010d}",
        "sourceSystem": random.choice(SOURCE_SYSTEMS),
        "Ingestion Batch": base_batch,
        "Patient ID": patient_id,
        "Member-ID": member_id,
        "MRN#": mrn,
        "Pt Name": patient_name,
        "DOB": noisy_date(dob_date, include_time=False, invalid_rate=0.04),
        "Sex@Birth": random.choice(SEX_TOKENS),
        "Gender Identity": random.choice(["man", "woman", "nonbinary", "trans", "unknown", "declined"]),
        "Phone Number": generate_phone(),
        "E-mail": f"{patient_id.lower()}@example.org" if random.random() > 0.12 else random.choice(["", "bad-email@", "N/A"]),
        "SSN": generate_ssn(),
        "Address Line 1": f"{random.randint(10, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Cedar', 'Maple'])} St",
        "City": city,
        "State": state,
        "ZIP Code": zip_code if random.random() > 0.08 else random.choice([f"{zip_code}-{random_digits(4)}", "12A45", "00000"]),
        "County": county,
        "Race/Ethnicity": random.choice(
            ["White", "Black", "Asian", "Hispanic", "Native", "Other", "Unknown", "Declined", "MULTI"]
        ),
        "Preferred Language": random.choice(["English", "Spanish", "French", "Arabic", "Mandarin", "Unknown"]),
        "Marital Status": random.choice(["S", "M", "D", "W", "Partnered", "Unknown"]),
        "Deceased?": noisy_bool(),
        "Death Date": noisy_date(random_date(date(2015, 1, 1), date(2026, 12, 31)), invalid_rate=0.25),
        "Encounter ID": encounter_id,
        "Encounter Date": noisy_date(enc_date, include_time=True, invalid_rate=0.03),
        "Discharge Date": noisy_date(discharge_date, include_time=True, invalid_rate=0.04),
        "Admission Type": random.choice(ADMISSION_TYPES),
        "Encounter Type": random.choice(ENCOUNTER_TYPES),
        "Facility ID": facility_id,
        "Provider NPI": provider_npi if random.random() > 0.07 else random.choice(["12345", "NPI?", "", "0000000000"]),
        "Primary ICD10": choose_code(ICD10_CODES, ICD10_BAD, invalid_rate=0.18),
        "Secondary ICD10": choose_code(ICD10_CODES, ICD10_BAD, invalid_rate=0.3),
        "Procedure CPT": choose_code(CPT_CODES, CPT_BAD, invalid_rate=0.16),
        "DRG Code": random.choice(["470", "291", "640", "177", "871", "945", "XXX"]),
        "Payer Name": random.choice(PAYERS),
        "Plan ID": plan_id,
        "Eligibility Status": random.choice(ELIGIBILITY_STATUS),
        "Prior Auth Req": noisy_bool(),
        "Claim ID": claim_id,
        "Claim Status": random.choice(CLAIM_STATUS),
        "Billed Amount": noisy_money(billed),
        "Allowed Amount": noisy_money(allowed),
        "Paid Amount": noisy_money(paid),
        "Coinsurance %": noisy_percent(coinsurance),
        "Patient Resp": noisy_money(patient_resp),
        "Denial Reason": denial_reason,
        "Lab Order ID": lab_order_id,
        "LOINC": choose_code(LOINC_CODES, LOINC_BAD, invalid_rate=0.2),
        "Lab Name": random.choice(LAB_NAMES),
        "Specimen Collected": noisy_date(specimen_date, include_time=True, invalid_rate=0.05),
        "Result Value": random.choice(
            [
                f"{random.uniform(0.1, 500):.2f}",
                f"<{random.randint(1, 10)}",
                f">{random.randint(150, 500)}",
                "hemolyzed",
                "invalid",
                "POS",
                "NEG",
            ]
        ),
        "Result Unit": random.choice(LAB_UNITS),
        "Reference Range": random.choice(["4.0-6.0", "70-110", "3.5-5.2", "10-20", "N/A", ""]),
        "Abnormal Flag": random.choice(ABNORMAL_FLAGS),
        "Rx Order ID": rx_order_id,
        "NDC": choose_code(NDC_CODES, NDC_BAD, invalid_rate=0.17),
        "RxNorm": choose_code(RXNORM, RXNORM_BAD, invalid_rate=0.15),
        "Medication Name": random.choice(
            ["metformin", "lisinopril", "atorvastatin", "albuterol", "amlodipine", "insulin glargine", "aspirin"]
        ),
        "Dose": random.choice(["5", "10", "20", "40", "0.5", "1", "2", "N/A"]),
        "Dose Unit": random.choice(["mg", "mcg", "mL", "tablet", "units", "NULL"]),
        "Route": random.choice(ROUTES),
        "Frequency": random.choice(FREQUENCIES),
        "Days Supply": random.choice(["7", "14", "30", "60", "90", "?", "NULL"]),
        "Refill Count": random.choice(["0", "1", "2", "3", "5", "N/A", "-1"]),
        "Prescriber NPI": prescriber_npi if random.random() > 0.06 else random.choice(["", "ABC", "999"]),
        "Housing Instability": noisy_bool(),
        "Food Insecurity": noisy_bool(),
        "Transport Barrier": noisy_bool(),
        "Employment Status": random.choice(["employed", "unemployed", "retired", "student", "disabled", "unknown"]),
        "Income Bracket": random.choice(
            ["<25k", "25k-50k", "50k-100k", "100k-200k", "200k+", "unknown", "DECLINED"]
        ),
        "Device ID": device_id,
        "Device Type": random.choice(DEVICE_TYPES),
        "Reading Timestamp": noisy_date(read_ts.date(), include_time=True, invalid_rate=0.04),
        "Heart Rate": str(heart_rate),
        "SpO2": str(spo2),
        "Systolic BP": str(systolic),
        "Diastolic BP": str(diastolic),
        "Glucose mg/dL": f"{glucose:.1f}",
        "Step Count": str(step_count if random.random() > 0.05 else random.randint(-2000, 100000)),
        "Sleep Hours": f"{sleep_hours:.2f}",
        "Clinical Note": note,
        "Raw Payload": generate_payload(record_id, patient_id, claim_id, device_id),
        "Consent Signed": noisy_bool(),
        "Data Share Opt-In": noisy_bool(),
        "Last Updated": noisy_date(random_date(date(2022, 1, 1), date(2026, 12, 31)), include_time=True, invalid_rate=0.02),
        "Source Lineage": ";".join(
            random.sample(
                ["ehr_export.csv", "claims_feed.tsv", "lab_delta.jsonl", "rx_extract.csv", "device_dump.ndjson"],
                k=random.randint(2, 4),
            )
        ),
    }

    for key in row:
        if random.random() < 0.012:
            row[key] = random.choice(MISSING_TOKENS)
        elif random.random() < 0.008 and isinstance(row[key], str):
            row[key] = f" {row[key]} "

    for key in ("Patient ID", "Claim ID", "Encounter ID", "Lab Order ID", "Rx Order ID"):
        row[key] = maybe_missing(row[key], p=0.005)

    return row


def generate_dataset(output_path: Path, target_mb: int, seed: int, status_every: int) -> tuple[int, int]:
    random.seed(seed)
    target_bytes = target_mb * 1024 * 1024
    output_path.parent.mkdir(parents=True, exist_ok=True)

    batch_stamp = datetime.utcnow().strftime("BATCH-%Y%m%d-%H%M%S")
    patient_pool: list[str] = []
    claim_pool: list[str] = []

    row_count = 0
    started = time.time()
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()

        while True:
            row_count += 1
            writer.writerow(generate_row(row_count, patient_pool, claim_pool, batch_stamp))

            if row_count % 5000 == 0:
                handle.flush()
                size = output_path.stat().st_size
                if row_count % status_every == 0:
                    elapsed = max(0.001, time.time() - started)
                    mb = size / (1024 * 1024)
                    rate = mb / elapsed
                    print(
                        f"[progress] rows={row_count:,} size={mb:,.1f} MB "
                        f"target={target_mb} MB rate={rate:.2f} MB/s"
                    )
                if size >= target_bytes:
                    break

    final_size = output_path.stat().st_size
    return row_count, final_size


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a multidomain stress-test CSV dataset.")
    parser.add_argument(
        "--output",
        default="stress_multidomain_650mb.csv",
        help="Output CSV path. Default writes to the project root filename.",
    )
    parser.add_argument("--target-mb", type=int, default=650, help="Target file size in MB (default: 650).")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument(
        "--status-every",
        type=int,
        default=50000,
        help="Print progress every N rows (default: 50000).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output).resolve()

    print(f"Generating multidomain stress CSV: {output_path}")
    rows, size = generate_dataset(
        output_path=output_path,
        target_mb=args.target_mb,
        seed=args.seed,
        status_every=max(5000, args.status_every),
    )
    print(f"Done. rows={rows:,} size_bytes={size:,} size_mb={size / (1024 * 1024):.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
