import re
import logging
import argparse
from sqlmodel import Session, select
from util import util, db_utils, prw_meta_utils
from prw_common.model.prw_model import *
from sqlalchemy import update
from sqlalchemy.sql.expression import bindparam
from prw_common.cli_utils import cli_parser
# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
DATASET_ID = "CLEAN_ENCOUNTERS"

# Logging definitions
logging.basicConfig(level=logging.INFO)
SHOW_SQL_IN_LOG = False


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
def clean_encounters(prw_session: Session):
    """
    Updates db tables directly to clean source data:
    1. Remove trailing " [<id>]" in dept, encounter_type, service_provider, billing_provider
    """
    logging.info("Cleaning encounters table")
    COLUMNS_TO_CLEAN = ['dept', 'encounter_type', 'service_provider', 'billing_provider']   

    # Use SQLAlchemy core for more efficient bulk updates
    trailing_id_re = re.compile(r"\s*\[[^\]]*\]\s*$")
    table = PrwEncounter.__table__
    
    # Process in batches, typically 500-2k rows is ideal
    batch_size = 1000
    last_id = 0
    total_processed = 0
    while True:
        # Get a batch of records to clean
        batch = prw_session.exec(
            select(PrwEncounter)
            .where(PrwEncounter.id > last_id)
            .order_by(PrwEncounter.id)
            .limit(batch_size)
        ).all()        

        if not batch:
            break
            
        # Trim "[id]" from values in the specified columns; only update if changed
        updates = []
        for encounter in batch:
            update_data = {}
            for field in COLUMNS_TO_CLEAN:
                original = getattr(encounter, field)
                if original:
                    cleaned = trailing_id_re.sub("", original)
                    update_data[field] = cleaned
                else:
                    update_data[field] = ""
            if update_data:
                update_data['updt_id'] = encounter.id
                updates.append(update_data)
        
        # Execute the upddate batch
        if updates:
            stmt = update(table).where(table.c.id == bindparam('updt_id'))
            for field in COLUMNS_TO_CLEAN:
                stmt = stmt.values(**{field: bindparam(field)})
            
            prw_session.exec(stmt, params=updates)
            prw_session.commit()

        # Remove processed batch to free resources and log progress
        total_processed += len(batch)
        last_id = batch[-1].id
        logging.info(f"Processed batch of {len(batch)} records, {total_processed} total")
        prw_session.expire_all()


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = cli_parser(
        description="Clean up raw encounter data in PRH warehouse.",
        require_prw=True,
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    db_url = args.prw

    logging.info(f"DB: {util.mask_pw(db_url)}")

    # Get connection to DB
    prw_engine = db_utils.get_db_connection(db_url, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        util.error_exit("ERROR: cannot open output DB (see above). Terminating.")
    prw_session = Session(prw_engine)

    # Clean up data
    clean_encounters(prw_session)

    # Update last modified time
    prw_meta_utils.write_meta(prw_session, DATASET_ID)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()
    logging.info("Done")


if __name__ == "__main__":
    main()
