import os
import logging
import argparse
import pandas as pd
from dataclasses import dataclass
from sqlmodel import Session
from util import util, db_utils, prw_meta
from prw_model.prw_panel_model import *

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
DATASET_ID = "patient_panel"

# Default output to local SQLite DB.
DEFAULT_PRW_DB_ODBC = "sqlite:///prw.sqlite3"

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
    "Sangha, Dildeep [6229238]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Olawuyi, Damola Bolutife [7191596]": "CC WPL FM RESIDENCY CLINIC",
    "Davis, Jennifer [54070483]": "CC WPL PALOUSE HEALTH CENTER",
    "Boyd, Jeana M [6628044]": "CC WPL PULLMAN FAMILY MEDICINE",
    "White, Malia [80012005]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Hatley, Shannon M [6134031]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Adkins, Benjamin J [50032100]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Guida, Kimberley [50032826]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Ward, Jeffrey Loren [5746915]": "CC WPL FM RESIDENCY CLINIC",
    "Harris, Brenna R [54981938]": "CC WPL FM RESIDENCY CLINIC",
    "Brodsky, Kaz B [55037680]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Smith, Angeline Elizabeth [5656055]": "CC WPL PALOUSE HEALTH CENTER",
    "Cargill, Teresa [55064229]": "CC WPL PULLMAN FAMILY MEDICINE",
    "Thompson, Molly [55040931]": "CC WPL FM RESIDENCY CLINIC",
    "Perin, Karly [7950541]": "CC WPL FM RESIDENCY CLINIC",
    "Younes, Mohammed [5772847]": "CC WPL FM RESIDENCY CLINIC",
    "Mader, Kelsey [S6148634]": "CC WPL FM RESIDENCY CLINIC",
    "Shakir, Tuarum N [6411322]": "CC WPL FM RESIDENCY CLINIC",
    "Frostad, Michael [50032808]": "CC WPL PALOUSE PEDIATRICS MOSCOW",
    "Hryniewicz, Kathryn [54977206]": "CC WPL PALOUSE PEDIATRICS MOSCOW",
    "Rinaldi, Mackenzie Claire [N9170397]": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "Shields, Maricarmen [55020855]": "CC WPL PALOUSE PEDIATRICS MOSCOW",
    "Lee, Jonathan [X9162396]": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "Gordon, Methuel [54062579]": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "Manderville, Tracy [8570166]": "CC WPL PALOUSE PEDIATRICS PULLMAN",
    "Clinic, Pullman Fam Med Residency [75007538]": "CC WPL FM RESIDENCY CLINIC",
}
WELL_ENCOUNTER_TYPES = [
    "CC PHYSICAL [70000003]",
    "CC SPORTS  [7600052]",
    "CC WELL CH [71511022]",
    "CC WELL CH [71511022]",
    "CCWELLBABY [71311001]",
    "MEDICARE WEL [3674]",
    "NEWOB [70000012]",
    "SUB AN WELL [945]",
    "Well Women [71511038]",
    "WELLNESS [3597]",
]


def transform_to_panels(src: SrcData) -> OutData:
    """
    Transform source data into panel data
    """
    logging.info("Transforming data")
    patients_df, encounters_df = src.patients_df, src.encounters_df

    # Use encounter data from src.encounters_df, data model defined in prw_model.prw_model.PrwEncounter,
    # to calculate the paneled provider. Implement the 4 cut method from CCI:
    # 1st Cut Patients who have seen only one provider in the past year - Assigned to that provider
    # 2nd Cut Patients who have seen multiple providers, but one provider the majority of the time
    #         in the past year - Assigned to the majority provider
    # 3rd Cut Patients who have seen two or more providers equally in the past year (No majority
    #         provider can be determined) - Assigned to the provider who performed the last well exam
    # 4th Cut Patients who have seen multiple providers - Assigned to the last provider seen

    # Filter to encounters in the past year
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
    recent_encounters = encounters_df[
        encounters_df["encounter_date"] >= one_year_ago
    ].copy()

    # Filter to recognized providers (those in the PROVIDER_TO_LOCATION map)
    recent_encounters = recent_encounters[
        recent_encounters["service_provider"].isin(PROVIDER_TO_LOCATION.keys())
    ]

    # Initialize panel assignments
    patients_panels_df = patients_df[["prw_id"]].copy()
    patients_panels_df["panel_provider"] = None
    patients_panels_df["panel_location"] = None
    logging.info(f"Number of patients: {len(patients_panels_df)}")

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

    majority_assignments_df = pd.DataFrame(majority_assignments)
    logging.info(f"2nd cut assignments: {len(majority_assignments_df)}")

    # 3rd Cut: Assign to provider of last well visit for remaining patients
    patients_after_2nd_cut = multi_provider_patients[
        ~multi_provider_patients["prw_id"].isin(majority_assignments_df["prw_id"])
    ]

    well_visits = recent_encounters[
        recent_encounters["encounter_type"].isin(WELL_ENCOUNTER_TYPES)
        & recent_encounters["prw_id"].isin(patients_after_2nd_cut["prw_id"])
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
        f"Total assignments: {len(all_assignments)} {len(all_assignments)/len(patients_panels_df)*100:.2f}%"
    )

    # Update panel assignments
    patients_panels_df = patients_panels_df.merge(
        all_assignments, on="prw_id", how="left"
    )

    # Map providers to locations
    patients_panels_df["panel_location"] = patients_panels_df["service_provider"].map(
        PROVIDER_TO_LOCATION
    )
    patients_panels_df["panel_provider"] = patients_panels_df["service_provider"]

    # Keep only panel columns
    patients_panels_df = patients_panels_df[
        ["prw_id", "panel_location", "panel_provider"]
    ]
    print(
        "\nData Sample:\n-----------------------------------------------------------------------------------\n",
        patients_panels_df.head(),
        "\n-----------------------------------------------------------------------------------\n",
    )

    return OutData(patients_panels_df=patients_panels_df)


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Recalculate patient panel data in-place in PRH warehouse."
    )
    parser.add_argument(
        "-db",
        help='DB connection string including credentials. Look for Azure SQL connection string in Settings > Connection strings, eg. "mssql+pyodbc:///?odbc_connect=Driver={ODBC Driver 18 for SQL Server};Server=tcp:{your server name},1433;Database={your db name};Uid={your user};Pwd={your password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"',
        default=DEFAULT_PRW_DB_ODBC,
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    db_url = args.db

    logging.info(f"DB: {util.mask_pw(db_url)}")

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
    out = transform_to_panels(src)

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
    prw_meta.write_meta(prw_session, DATASET_ID)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()
    logging.info("Done")


if __name__ == "__main__":
    main()
