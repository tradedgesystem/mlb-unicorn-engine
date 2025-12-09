from datetime import date
from typing import List

from sqlalchemy.orm import Session

from backend.db import models


def fetch_top50_for_date(session: Session, run_date: date) -> List[models.UnicornTop50Daily]:
    return (
        session.query(models.UnicornTop50Daily)
        .filter(models.UnicornTop50Daily.run_date == run_date)
        .order_by(models.UnicornTop50Daily.rank)
        .all()
    )
