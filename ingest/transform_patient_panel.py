import os
import logging
import pandas as pd
from dataclasses import dataclass
from sqlmodel import Session
from util import util, prw_meta_utils
from prw_common.model.prw_panel_model import *
from prw_common.cli_utils import cli_parser
from prw_common.db_utils import (
    TableData,
    get_db_connection,
    mask_conn_pw,
    clear_tables_and_insert_data,
)

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

    # Convert encounter_date from YYYYMMDD int to datetime
    encounters_df["encounter_date"] = pd.to_datetime(
        encounters_df["encounter_date"].astype(str), format="%Y%m%d"
    )

    return SrcData(patients_df=patients_df, encounters_df=encounters_df)


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
PEDS_LOCATIONS = [
    "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "CC WPL PALOUSE PEDIATRICS MOSCOW",
]
PROVIDER_TO_LOCATION = {
    # Palouse Pediatrics
    "FROSTAD, MICHAEL": "Palouse Pediatrics",
    "GORDON, METHUEL": "Palouse Pediatrics",
    "HRYNIEWICZ, KATHRYN": "Palouse Pediatrics",
    "LEE, JONATHAN": "Palouse Pediatrics",
    "LEE, JONATHAN J": "Palouse Pediatrics",
    "RINALDI, MACKENZIE CLAIRE": "Palouse Pediatrics",
    "SHIELDS, MARICARMEN": "Palouse Pediatrics",
    # Pullman Family Medicine
    "ADKINS, BENJAMIN J": "Pullman Family Medicine",
    "BRODSKY, KAZ B": "Pullman Family Medicine",
    "CARGILL, TERESA": "Pullman Family Medicine",
    "DAVIS, JENNIFER": "Pullman Family Medicine",
    "GUIDA, KIMBERLEY": "Pullman Family Medicine",
    "SANGHA, DILDEEP": "Pullman Family Medicine",
    "SMITH, ANGELINE ELIZABETH": "Pullman Family Medicine",
    # Palouse Medical
    "HALL, STEPHEN": "Palouse Medical",
    "IACOBELLI, CHRISTOPHER J": "Palouse Medical",
    "TRICOLA, KASSANDRA M": "Palouse Medical",
    "GARCIA, CLARA E": "Palouse Medical",
    "FOSBACK, STEPHANIE M": "Palouse Medical",
    "GREGORY, NANCY": "Palouse Medical",
    "HOWELL, RICHARD L": "Palouse Medical",
    "AIYENOWO, JOSEPH O": "Palouse Medical",
    "BURKE, MORGAN ELIZABETH": "Palouse Medical",
    "HOOVER, MARK A": "Palouse Medical",
    "LEIDER, MORGAN": "Palouse Medical",
    # Residency
    "HARRIS, BRENNA R": "Residency",
    "MADER, KELSEY": "Residency",
    "SHAKIR, TUARUM N": "Residency",
    "THOMPSON, MOLLY": "Residency",
    "WARD, JEFFREY LOREN": "Residency",
    "OLAWUYI, DAMOLA BOLUTIFE": "Residency",
    "PERIN, KARLY": "Residency",
    "WEBB, DRUE": "Residency",
    "WEBBER, MOLLY": "Residency",
    "YOUNES, MOHAMMED": "Residency",
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


def transform_filter_encounters(src: SrcData):
    """
    Only look at actual office visits that were completed to calculate panel info
    """
    src.encounters_df = src.encounters_df[
        # Filter out non-office visits types - manually reviewed from unique values in column
        ~src.encounters_df["encounter_type"].isin(
            [
                "CC CLINICAL SUPPORT",
                "CC NURSE VISIT",
                "CC LAB",
                "CC ANTICOAGULATION",
                "COVID-19 VACCINE",
                "PHS SILENT US",
            ]
        )
        &
        # Only retain completed encounters
        (src.encounters_df["appt_status"] == "Completed")
    ]


def transform_add_peds_panels(src: SrcData):
    """
    Add panel data (panel_location, panel_provider) to patients_df in place
    """
    logging.info("Adding panel information for peds")
    patients_df, encounters_df = src.patients_df, src.encounters_df

    # Filter out patients that already have a panel provider or location
    patients_df = patients_df[
        (patients_df["panel_provider"].isna()) & (patients_df["panel_location"].isna())
    ]

    # Limit encounters to 3 years
    three_years_ago = pd.Timestamp.now() - pd.DateOffset(years=3)
    encounters_df = encounters_df[encounters_df["encounter_date"] >= three_years_ago]

    # Mark encounters in the dept in CC WPL PALOUSE PEDIATRICS PULLMAN or CC WPL PALOUSE PEDIATRICS MOSCOW
    encounters_df["is_peds_encounter"] = encounters_df["dept"].isin(PEDS_LOCATIONS)

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

    # Prepare data structures for vectorized operations
    # Create a DataFrame with patient IDs to track rule results
    patient_ids = patients_df["prw_id"].unique()
    results_df = pd.DataFrame({"prw_id": patient_ids})
    results_df["meets_rule_1"] = False
    results_df["meets_rule_2"] = False
    results_df["meets_rule_3"] = False
    results_df["should_remove_rule_4"] = False

    # Group encounters by patient for efficient processing
    recent_by_patient = dict(list(recent_encounters.groupby("prw_id")))
    last_year_by_patient = dict(list(last_year_encounters.groupby("prw_id")))
    all_by_patient = dict(list(encounters_df.groupby("prw_id")))

    # Process rule 1 for all patients
    logging.info("Calculating rule 1")
    for i, prw_id in enumerate(patient_ids):
        if prw_id in recent_by_patient:
            patient_encounters = recent_by_patient[prw_id]
            if patient_encounters["is_peds_encounter"].sum() >= 3:
                # Get last three encounters
                last_three = patient_encounters.sort_values(
                    "encounter_date", ascending=False
                ).head(3)
                if all(last_three["is_peds_encounter"]):
                    results_df.loc[results_df["prw_id"] == prw_id, "meets_rule_1"] = (
                        True
                    )
                    results_df.loc[
                        results_df["prw_id"] == prw_id, "assignment_details"
                    ] = "Rule 1: At least 3 visits in the last 2 years, and the last 3 were at peds"
    logging.info(f"Rule 1 assignments: {results_df['meets_rule_1'].sum()}")

    # Process rule 2 for all patients
    logging.info("Calculating rule 2")
    for i, prw_id in enumerate(patient_ids):
        if prw_id in recent_by_patient:
            patient_encounters = recent_by_patient[prw_id]
            # Get well visits
            well_visits = patient_encounters[patient_encounters["is_well_visit"]]

            if len(well_visits) > 0:
                # Get the most recent well visit
                last_well = well_visits.sort_values(
                    "encounter_date", ascending=False
                ).iloc[0]

                if last_well["is_peds_encounter"]:
                    # Check if at least one of last 3 visits was at peds
                    last_three = patient_encounters.sort_values(
                        "encounter_date", ascending=False
                    ).head(3)
                    if any(last_three["is_peds_encounter"]):
                        results_df.loc[
                            results_df["prw_id"] == prw_id, "meets_rule_2"
                        ] = True
                        results_df.loc[
                            results_df["prw_id"] == prw_id, "assignment_details"
                        ] = "Rule 2: Last well visit was in the last 2 years AND it was at peds AND at least one of the last 3 visits was at peds"
    logging.info(f"Rule 2 assignments: {results_df['meets_rule_2'].sum()}")

    # Process rule 3 for all patients
    logging.info("Calculating rule 3")
    for i, prw_id in enumerate(patient_ids):
        if prw_id in recent_by_patient and prw_id in last_year_by_patient:
            recent_patient = recent_by_patient[prw_id]
            last_year_patient = last_year_by_patient[prw_id]

            # Check if there are any well visits in last 2 years
            recent_well_visits = recent_patient[recent_patient["is_well_visit"]]

            if len(recent_well_visits) == 0 and len(last_year_patient) >= 3:
                # Check if majority are peds
                peds_visits = last_year_patient["is_peds_encounter"].sum()

                if peds_visits > len(last_year_patient) / 2:
                    # Check if at least one of last 3 visits was at peds
                    if prw_id in all_by_patient:
                        last_three = last_year_patient.sort_values(
                            "encounter_date", ascending=False
                        ).head(3)
                        if any(last_three["is_peds_encounter"]):
                            results_df.loc[
                                results_df["prw_id"] == prw_id, "meets_rule_3"
                            ] = True
                            results_df.loc[
                                results_df["prw_id"] == prw_id, "assignment_details"
                            ] = "Rule 3: No well visit in 2 years AND at least 3 visits in the last 1 year AND majority with peds AND at least one of the last 3 visits was at peds"
    logging.info(f"Rule 3 assignments: {results_df['meets_rule_3'].sum()}")

    # Process rule 4 for all patients
    logging.info("Calculating rule 4")
    # Create a Series mapping prw_id to age for faster lookup
    age_map = patients_df.set_index("prw_id")["age"]

    for i, prw_id in enumerate(patient_ids):
        # Check if patient is under 3
        if prw_id in age_map and age_map[prw_id] < 3:
            # Check for any peds appointments in last 15 months
            if prw_id in recent_by_patient:
                recent_peds = recent_by_patient[prw_id][
                    (recent_by_patient[prw_id]["encounter_date"] >= fifteen_months_ago)
                    & (recent_by_patient[prw_id]["is_peds_encounter"])
                ]
                if len(recent_peds) == 0:
                    results_df.loc[
                        results_df["prw_id"] == prw_id, "should_remove_rule_4"
                    ] = True
    logging.info(f"Rule 4 removals: {results_df['should_remove_rule_4'].sum()}")

    # Combine all rules to get final empaneled patients
    results_df["should_empanel"] = (
        results_df["meets_rule_1"]
        | results_df["meets_rule_2"]
        | results_df["meets_rule_3"]
    ) & ~results_df["should_remove_rule_4"]

    # Get list of empaneled patients
    empaneled_patients = results_df[results_df["should_empanel"]]["prw_id"].tolist()

    # Update panel_location for empaneled patients
    mask = src.patients_df["prw_id"].isin(empaneled_patients)
    src.patients_df.loc[mask, "panel_location"] = "Palouse Pediatrics"

    # Copy assignment details for empaneled patients
    details_df = results_df[results_df["should_empanel"]][
        ["prw_id", "assignment_details"]
    ]
    src.patients_df = src.patients_df.drop("assignment_details", axis=1).merge(
        details_df, on="prw_id", how="left"
    )

    logging.info(f"Added {len(empaneled_patients)} pediatric panel assignments")
    print(
        "\nData Sample:\n-----------------------------------------------------------------------------------\n",
        src.patients_df[src.patients_df["panel_location"].notna()][
            ["prw_id", "panel_location", "panel_provider", "assignment_details"]
        ].head(),
        "\n-----------------------------------------------------------------------------------\n",
    )


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

    # Filter out patients and encounters that already have a panel provider or location
    unassigned_mask = (patients_df["panel_provider"].isna()) & (
        patients_df["panel_location"].isna()
    )
    unassigned_patients_df = patients_df[unassigned_mask]
    encounters_df = encounters_df[
        encounters_df["prw_id"].isin(unassigned_patients_df["prw_id"])
    ]

    # Initialize panel columns
    logging.info(f"Number of unassigned patients: {len(unassigned_patients_df)}")

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
    single_provider_assignments["assignment_details"] = (
        "1st cut: Patients who have seen only one provider"
    )

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
    majority_assignments_df["assignment_details"] = (
        "2nd cut: Patients with a majority provider"
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
            | recent_encounters["diagnoses"].str.match(WELL_DX_REGEX, case=False)
        )
    ].sort_values("encounter_date", ascending=False)

    last_well_assignments = (
        well_visits.groupby("prw_id").first()[["service_provider"]].reset_index()
    )
    last_well_assignments["assignment_details"] = (
        "3rd cut: Assign to provider of last well visit"
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
    last_provider_seen["assignment_details"] = (
        "4th cut: Assign remaining patients to last provider seen"
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
        f"Total assignments: {len(all_assignments)} {len(all_assignments)/len(unassigned_patients_df)*100:.2f}%"
    )

    # Merge all_assignments back into patients_df
    all_assignments = src.patients_df.merge(
        all_assignments, on="prw_id", how="left", suffixes=("", "_new")
    )

    # Only update panel_provider and panel_location where it hasn't been set
    src.patients_df.loc[unassigned_mask, "panel_provider"] = all_assignments.loc[
        unassigned_mask, "service_provider"
    ]
    src.patients_df.loc[unassigned_mask, "panel_location"] = src.patients_df.loc[
        unassigned_mask, "panel_provider"
    ].map(PROVIDER_TO_LOCATION)
    src.patients_df.loc[unassigned_mask, "assignment_details"] = all_assignments.loc[
        unassigned_mask, "assignment_details_new"
    ]

    print(
        "\nData Sample:\n-----------------------------------------------------------------------------------\n",
        src.patients_df.loc[
            unassigned_mask,
            ["prw_id", "panel_location", "panel_provider", "assignment_details"],
        ].head(),
        "\n-----------------------------------------------------------------------------------\n",
    )


def keep_panel_data(src: SrcData) -> OutData:
    """
    Keep only panel data
    """
    return OutData(
        patients_panels_df=src.patients_df[
            ["prw_id", "panel_location", "panel_provider", "assignment_details"]
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

    logging.info(f"Using PRW DB: {mask_conn_pw(db_url)}")

    # Get connection to DB
    prw_engine = get_db_connection(db_url, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        util.error_exit("ERROR: cannot open output DB (see above). Terminating.")
    prw_session = Session(prw_engine)

    # Extract source tables into memory
    src = read_source_tables(prw_session)
    if src is None:
        util.error_exit("ERROR: failed to read source data (see above)")

    # Add panel columns
    if "panel_provider" not in src.patients_df.columns:
        src.patients_df["panel_provider"] = pd.NA
    if "panel_location" not in src.patients_df.columns:
        src.patients_df["panel_location"] = pd.NA
    if "assignment_details" not in src.patients_df.columns:
        src.patients_df["assignment_details"] = pd.NA

    # Transform data
    transform_filter_encounters(src)
    transform_add_peds_panels(src)
    transform_add_other_panels(src)
    out = keep_panel_data(src)

    # Create tables if they do not exist
    logging.info("Creating tables")
    PrwMetaModel.metadata.create_all(prw_engine)
    PrwPatientPanel.metadata.create_all(prw_engine)

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=PrwPatientPanel, df=out.patients_panels_df),
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
