import os
import logging
import argparse
import pandas as pd
from dataclasses import dataclass
from sqlmodel import Session
from util import util, db_utils, prw_meta_utils
from prw_common.model.prw_panel_model import *
from prw_common.cli_utils import cli_parser

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
DATASET_ID = "patient_panel"

# Logging definitions
logging.basicConfig(level=logging.INFO)
SHOW_SQL_IN_LOG = False


# -------------------------------------------------------
# Types
# -------------------------------------------------------
@dataclass
class SrcData:
    patients_df: pd.DataFrame
    encounters_df: pd.DataFrame


@dataclass
class OutData:
    patients_panels_df: pd.DataFrame


# -------------------------------------------------------
# Extract
# -------------------------------------------------------
def read_source_tables(session: Session) -> SrcData:
    """
    Read source tables from the warehouse DB
    """
    logging.info("Reading source tables")
    patients_df = pd.read_sql_table("prw_patients", session.bind)
    encounters_df = pd.read_sql_table("prw_encounters", session.bind)
    return SrcData(patients_df=patients_df, encounters_df=encounters_df)


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
PROVIDER_TO_LOCATION = {
    "ADKINS, BENJAMIN J": "CC WPL PULLMAN FAMILY MEDICINE",
    "AIYENOWO, JOSEPH O": "CC WPL PALOUSE MED PRIMARY CARE", 
    "BRODSKY, KAZ B": "CC WPL PULLMAN FAMILY MEDICINE",
    "BURKE, MORGAN ELIZABETH": "CC WPL PALOUSE MED PRIMARY CARE",
    "CARGILL, TERESA": "CC WPL PULLMAN FAMILY MEDICINE",
    "DAVIS, JENNIFER": "CC WPL PULLMAN FAMILY MEDICINE",
    "FOSBACK, STEPHANIE M": "CC WPL PALOUSE MED PRIMARY CARE",
    "FROSTAD, MICHAEL": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "GARCIA, CLARA E": "CC WPL PALOUSE MED PRIMARY CARE",
    "GORDON, METHUEL": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "GREGORY, NANCY": "CC WPL PALOUSE MED PRIMARY CARE",
    "GUIDA, KIMBERLEY": "CC WPL PULLMAN FAMILY MEDICINE",
    "HALL, STEPHEN": "CC WPL PALOUSE MED PRIMARY CARE",
    "HARRIS, BRENNA R": "CC WPL FM RESIDENCY CLINIC",
    "HATLEY, SHANNON M": "CC WPL PULLMAN FAMILY MEDICINE",
    "HOOVER, MARK A": "CC WPL PALOUSE MED PRIMARY CARE",
    "HOWELL, RICHARD L": "CC WPL PALOUSE MED PRIMARY CARE",
    "HRYNIEWICZ, KATHRYN": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "IACOBELLI, CHRISTOPHER J": "CC WPL PALOUSE MED PRIMARY CARE",
    "LEE, JONATHAN": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "LEE, JONATHAN J": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "LEIDER, MORGAN": "CC WPL PALOUSE MED PRIMARY CARE",
    "MADER, KELSEY": "CC WPL FM RESIDENCY CLINIC",
    "OLAWUYI, DAMOLA BOLUTIFE": "CC WPL FM RESIDENCY CLINIC",
    "PERIN, KARLY": "CC WPL FM RESIDENCY CLINIC",
    "RINALDI, MACKENZIE CLAIRE": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "SANGHA, DILDEEP": "CC WPL PULLMAN FAMILY MEDICINE",
    "SHAKIR, TUARUM N": "CC WPL FM RESIDENCY CLINIC",
    "SHIELDS, MARICARMEN": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "SHONK, JENNIFER L": "CC WPL FM RESIDENCY CLINIC",
    "SMITH, ANGELINE ELIZABETH": "CC WPL PULLMAN FAMILY MEDICINE",
    "STANCIL, LAKEESHA": "CC WPL PULLMAN FAMILY MEDICINE",
    "THOMPSON, MOLLY": "CC WPL FM RESIDENCY CLINIC",
    "TRICOLA, KASSANDRA M": "CC WPL PALOUSE MED PRIMARY CARE",
    "WARD, JEFFREY LOREN": "CC WPL FM RESIDENCY CLINIC",
    "WEBB, DRUE": "CC WPL PULLMAN FAMILY MEDICINE",
    "WEBBER, MOLLY": "CC WPL FM RESIDENCY CLINIC",
    "WHITE, MALIA": "CC WPL PULLMAN FAMILY MEDICINE",
    "YOUNES, MOHAMMED": "CC WPL FM RESIDENCY CLINIC",
}
WELL_ENCOUNTER_TYPES = [
    "CC WELL BABY",
    "CC WELL CHILD",
    "CC WELLNESS",
    "CC MEDICARE ANNUAL WELLNESS",
    "CC PHYSICAL",
    "CC WELL WOMEN",
    "CC DOT PHYSICAL",
    "CC MEDICARE SUB AN WELL",
    "CC SPORTS PHYSICAL",
    "CC FAA PHYSICAL",
]
WELL_DX_STRINGS = [
    "well baby",
    "well child",
    "well adolescent",
    "well woman",
    "well man",
    "wellness visit",
    "annual wellness",
    "well exam",
    "wellness exam",
]
WELL_DX_REGEX = "|".join(f"[{code}]" for code in WELL_DX_STRINGS)


def transform_add_peds_panels(src: SrcData):
    """
    Add panel data (panel_location, panel_provider) to patients_df in place
    """
    logging.info("Adding panel information for peds")
    patients_df, encounters_df = src.patients_df, src.encounters_df

    # Filter out patients that already have a panel provider or location
    if "panel_provider" not in patients_df.columns:
        patients_df["panel_provider"] = pd.NA
    if "panel_location" not in patients_df.columns:
        patients_df["panel_location"] = pd.NA
    patients_df = patients_df[
        (patients_df["panel_provider"].isna()) & (patients_df["panel_location"].isna())
    ]

    # Limit encounters to 3 years
    three_years_ago = pd.Timestamp.now() - pd.DateOffset(years=3)
    encounters_df = encounters_df[encounters_df["encounter_date"] >= three_years_ago]

    # Mark encounters in the dept in CC WPL PALOUSE PEDIATRICS PULLMAN or CC WPL PALOUSE PEDIATRICS MOSCOW
    encounters_df["is_peds_encounter"] = (encounters_df["dept"] == "CC WPL PALOUSE PEDIATRICS PULLMAN") | (
        encounters_df["dept"] == "CC WPL PALOUSE PEDIATRICS MOSCOW"
    )

    # Mark well visits by visit type or diagnoses
    encounters_df["is_well_visit"] = encounters_df["encounter_type"].isin(
        WELL_ENCOUNTER_TYPES
    ) | encounters_df["diagnoses"].str.match(WELL_DX_REGEX, case=False)

    # ------------------------------------------------
    # Empanel by location:
    # 1. At least 3 visits in the last 2 years, and the last 3 were at peds
    # 2. Last well visit was in the last 2 years AND it was at peds AND at least one of the last 3 visits was at peds
    # 3. No well visit in 2 years AND at least 3 visits in the last 1 year AND majority with peds AND at least one of the last 3 visits was at peds
    # 4. REMOVE those < 3yo that have not had any appt at peds for 15 months
    # For now, we do not empanel to a specific provider
    # ------------------------------------------------

    # Get reference timestamps
    now = pd.Timestamp.now()
    two_years_ago = now - pd.DateOffset(years=2)
    one_year_ago = now - pd.DateOffset(years=1)
    fifteen_months_ago = now - pd.DateOffset(months=15)

    # Filter encounters by time periods we'll need
    recent_encounters = encounters_df[
        encounters_df["encounter_date"] >= two_years_ago
    ].copy()
    last_year_encounters = encounters_df[
        encounters_df["encounter_date"] >= one_year_ago
    ].copy()

    def get_last_n_encounters(df, prw_id, n=3):
        """Helper function to get the last n encounters for a patient"""
        return (
            df[df["prw_id"] == prw_id]
            .sort_values("encounter_date", ascending=False)
            .head(n)
        )

    def meets_rule_1(prw_id):
        """At least 3 visits in last 2 years, and last 3 were at peds"""
        patient_encounters = recent_encounters[recent_encounters["prw_id"] == prw_id]
        if patient_encounters["is_peds_encounter"].sum() < 3:
            return False

        last_three = get_last_n_encounters(patient_encounters, prw_id, 3)
        return all(last_three["is_peds_encounter"])

    def meets_rule_2(prw_id):
        """Last well visit was in last 2 years AND at peds AND one of last 3 visits at peds"""
        patient_encounters = recent_encounters[recent_encounters["prw_id"] == prw_id]

        # Get last well visit
        well_visits = patient_encounters[patient_encounters["is_well_visit"]]
        if len(well_visits) == 0:
            return False

        last_well = well_visits.sort_values("encounter_date", ascending=False).iloc[0]
        if not last_well["is_peds_encounter"]:
            return False

        # Check if at least one of last 3 visits was at peds
        last_three = get_last_n_encounters(patient_encounters, prw_id, 3)
        return any(last_three["is_peds_encounter"])

    def meets_rule_3(prw_id):
        """No well visit in 2 years AND 3+ visits in last year AND majority peds AND one of last 3 at peds"""
        # Check if there are any well visits in last 2 years
        recent_well_visits = recent_encounters[
            (recent_encounters["prw_id"] == prw_id)
            & (recent_encounters["is_well_visit"])
        ]
        if len(recent_well_visits) > 0:
            return False

        # Check last year's visits
        last_year_patient = last_year_encounters[
            last_year_encounters["prw_id"] == prw_id
        ]
        if len(last_year_patient) < 3:
            return False

        # Check if majority are peds
        peds_visits = sum(last_year_patient["is_peds_encounter"])
        if peds_visits <= len(last_year_patient) / 2:
            return False

        # Check if at least one of last 3 visits was at peds
        last_three = get_last_n_encounters(encounters_df, prw_id, 3)
        return any(last_three["is_peds_encounter"])

    def should_remove_by_rule_4(prw_id):
        """Remove if < 3yo and no peds appointment in 15 months"""
        patient = patients_df[patients_df["prw_id"] == prw_id].iloc[0]

        # Check if patient is under 3
        if patient["age"] >= 3:
            return False

        # Check for any peds appointments in last 15 months
        recent_peds = encounters_df[
            (encounters_df["prw_id"] == prw_id)
            & (encounters_df["encounter_date"] >= fifteen_months_ago)
            & (encounters_df["is_peds_encounter"])
        ]
        return len(recent_peds) == 0

    # Apply rules to each patient
    empaneled_patients = []
    for prw_id in patients_df["prw_id"]:
        if meets_rule_1(prw_id) or meets_rule_2(prw_id) or meets_rule_3(prw_id):
            if not should_remove_by_rule_4(prw_id):
                empaneled_patients.append(prw_id)

    # Update panel_location for empaneled patients
    mask = patients_df["prw_id"].isin(empaneled_patients)
    patients_df.loc[mask, "panel_location"] = "Palouse Pediatrics"

    logging.info(f"Added {len(empaneled_patients)} pediatric panel assignments")


def transform_add_other_panels(src: SrcData):
    """
    Add panel data (panel_location, panel_provider) to patients_df in place

    Use encounter data from src.encounters_df, data model defined in prw_common.model.prw_model.PrwEncounter,
    to calculate the paneled provider. Implement the 4 cut method from CCI:
    1st Cut Patients who have seen only one provider in the past year - Assigned to that provider
    2nd Cut Patients who have seen multiple providers, but one provider the majority of the time
            in the past year - Assigned to the majority provider
    3rd Cut Patients who have seen two or more providers equally in the past year (No majority
            provider can be determined) - Assigned to the provider who performed the last well exam
    4th Cut Patients who have seen multiple providers - Assigned to the last provider seen
    """
    logging.info("Adding panel information")
    patients_df, encounters_df = src.patients_df, src.encounters_df

    # Filter out patients that already have a panel provider or location
    patients_df = patients_df[
        (patients_df["panel_provider"].isna()) & (patients_df["panel_location"].isna())
    ]

    # Initialize panel columns
    logging.info(f"Number of patients: {len(patients_df)}")

    # Filter to encounters in the past 2 years
    two_years_ago = pd.Timestamp.now() - pd.DateOffset(years=2)
    recent_encounters = encounters_df[
        encounters_df["encounter_date"] >= two_years_ago
    ].copy()

    # Filter to recognized providers (those in the PROVIDER_TO_LOCATION map)
    recent_encounters = recent_encounters[
        recent_encounters["service_provider"].isin(PROVIDER_TO_LOCATION.keys())
    ]

    # Get counts of providers per patient
    provider_counts = (
        recent_encounters.groupby(["prw_id", "service_provider"])
        .size()
        .reset_index(name="visits")
    )
    patient_provider_counts = (
        provider_counts.groupby("prw_id").size().reset_index(name="provider_count")
    )

    # 1st Cut: Patients who have seen only one provider
    single_provider_patients = patient_provider_counts[
        patient_provider_counts["provider_count"] == 1
    ]
    single_provider_assignments = provider_counts[
        provider_counts["prw_id"].isin(single_provider_patients["prw_id"])
    ][["prw_id", "service_provider"]]

    # 2nd Cut: Patients with a majority provider
    multi_provider_patients = patient_provider_counts[
        patient_provider_counts["provider_count"] > 1
    ]
    majority_assignments = []
    logging.info(f"1st cut assignments: {len(single_provider_assignments)}")

    for patient_id in multi_provider_patients["prw_id"]:
        patient_visits = provider_counts[provider_counts["prw_id"] == patient_id]
        total_visits = patient_visits["visits"].sum()
        max_visits_for_one_provider = patient_visits["visits"].max()

        # Check if any provider has more than 50% of visits
        if max_visits_for_one_provider > total_visits / 2:
            majority_provider = patient_visits.loc[
                patient_visits["visits"].idxmax(), "service_provider"
            ]
            majority_assignments.append(
                {"prw_id": patient_id, "service_provider": majority_provider}
            )

    majority_assignments_df = pd.DataFrame(
        majority_assignments, columns=["prw_id", "service_provider"]
    )
    logging.info(f"2nd cut assignments: {len(majority_assignments_df)}")

    # 3rd Cut: Assign to provider of last well visit for remaining patients
    patients_after_2nd_cut = multi_provider_patients[
        ~multi_provider_patients["prw_id"].isin(majority_assignments_df["prw_id"])
    ]

    well_visits = recent_encounters[
        recent_encounters["prw_id"].isin(patients_after_2nd_cut["prw_id"])
        & (
            recent_encounters["encounter_type"].isin(WELL_ENCOUNTER_TYPES)
            | recent_encounters["diagnoses"].str.match(
                WELL_DX_REGEX, case=False
            )
        )
    ].sort_values("encounter_date", ascending=False)

    last_well_assignments = (
        well_visits.groupby("prw_id").first()[["service_provider"]].reset_index()
    )
    logging.info(f"3rd cut assignments: {len(last_well_assignments)}")

    # 4th Cut: Assign remaining patients to last provider seen
    patients_after_3rd_cut = patients_after_2nd_cut[
        ~patients_after_2nd_cut["prw_id"].isin(last_well_assignments["prw_id"])
    ]

    last_provider_seen = (
        recent_encounters[
            recent_encounters["prw_id"].isin(patients_after_3rd_cut["prw_id"])
        ]
        .sort_values("encounter_date", ascending=False)
        .groupby("prw_id")
        .first()[["service_provider"]]
        .reset_index()
    )
    logging.info(f"4th cut assignments: {len(last_provider_seen)}")

    # Combine all assignments
    all_assignments = pd.concat(
        [
            single_provider_assignments,
            majority_assignments_df,
            last_well_assignments,
            last_provider_seen,
        ]
    )
    logging.info(
        f"Total assignments: {len(all_assignments)} {len(all_assignments)/len(patients_df)*100:.2f}%"
    )

    # Merge all_assignments back into patients_df
    all_assignments = patients_df.merge(
        all_assignments, on="prw_id", how="left", suffixes=("", "_new")
    )
    patients_df["panel_provider"] = all_assignments["service_provider"]

    # Map providers to locations
    patients_df["panel_location"] = patients_df["panel_provider"].map(
        PROVIDER_TO_LOCATION
    )

    print(
        "\nData Sample:\n-----------------------------------------------------------------------------------\n",
        patients_df[["prw_id", "panel_location", "panel_provider"]].head(),
        "\n-----------------------------------------------------------------------------------\n",
    )


def keep_panel_data(src: SrcData) -> OutData:
    """
    Keep only panel data
    """
    return OutData(
        patients_panels_df=src.patients_df[
            ["prw_id", "panel_location", "panel_provider"]
        ]
    )


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = cli_parser(
        description="Recalculate patient panel data in-place in PRH warehouse.",
        require_prw=True,
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    db_url = args.prw

    logging.info(f"Using PRW DB: {util.mask_pw(db_url)}")

    # Get connection to DB
    prw_engine = db_utils.get_db_connection(db_url, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        util.error_exit("ERROR: cannot open output DB (see above). Terminating.")
    prw_session = Session(prw_engine)

    # Extract source tables into memory
    src = read_source_tables(prw_session)
    if src is None:
        util.error_exit("ERROR: failed to read source data (see above)")

    # Transform data
    transform_add_peds_panels(src)
    transform_add_other_panels(src)
    out = keep_panel_data(src)

    # Create tables if they do not exist
    logging.info("Creating tables")
    PrwMetaModel.metadata.create_all(prw_engine)
    PrwPatientPanel.metadata.create_all(prw_engine)

    # Write into DB
    db_utils.clear_tables_and_insert_data(
        prw_session,
        [
            db_utils.TableData(table=PrwPatientPanel, df=out.patients_panels_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    prw_meta_utils.write_meta(prw_session, DATASET_ID)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()
    logging.info("Done")


if __name__ == "__main__":
    main()
