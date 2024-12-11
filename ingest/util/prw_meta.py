import logging
from datetime import datetime
from sqlmodel import Session, select, delete
from prw_model import prw_meta_model


def write_meta(session: Session, dataset: str, modified: dict = None):
    """
    Populate the meta and sources_meta tables with updated times
    """
    logging.info(
        f"Writing metadata to: {prw_meta_model.PrwMeta.__tablename__}, {prw_meta_model.PrwSourcesMeta.__tablename__}"
    )

    # Store now as the last ingest time, replace existing records
    # Update existing record if it exists, otherwise create new
    stmt = select(prw_meta_model.PrwMeta).where(
        prw_meta_model.PrwMeta.dataset == dataset
    )
    existing = session.exec(stmt).first()
    if existing:
        existing.modified = datetime.now()
    else:
        meta = prw_meta_model.PrwMeta(dataset=dataset, modified=datetime.now())
        session.add(meta)

    # Store last modified timestamps for ingested files, updating if already exists
    if modified is not None:
        for file, modified_time in modified.items():
            stmt = select(prw_meta_model.PrwSourcesMeta).where(
                prw_meta_model.PrwSourcesMeta.source == file
            )
            existing = session.exec(stmt).first()
            if existing:
                existing.modified = modified_time
            else:
                sources_meta = prw_meta_model.PrwSourcesMeta(
                    source=file, modified=modified_time
                )
                session.add(sources_meta)
