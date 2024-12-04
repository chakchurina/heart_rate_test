from datetime import datetime

from sqlalchemy import (
    CheckConstraint, Column, Date, DateTime, Float, ForeignKey,
    Index, Integer, MetaData, String, Table, and_, create_engine,
    func, select, text
)
from sqlalchemy.sql import Select


engine = create_engine(
    'postgresql://username:password@host:port/database_name',
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800
)

metadata = MetaData()

users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('gender', String, CheckConstraint("gender IN ('male', 'female', 'other')")),
    Column('birth_date', Date),
    Index('idx_users_birth_date', 'birth_date')
)

# sqlalchemy doesn't support partitions creation so this should be done separately:))
heart_rates_partitioned = Table(
    'heart_rates_partitioned', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
    Column('timestamp', DateTime, nullable=False),
    Column('heart_rate', Float, nullable=False),
    Index('idx_heart_rates_timestamp', 'timestamp')
)

metadata.create_all(engine)


def query_users(
    min_age: int, min_avg_heart_rate: float, date_from: datetime, date_to: datetime
) -> Select:
    """
    Query users with a minimum age and average heart
    rate above a threshold within a time range.

    Args:
        min_age (int): Minimum user age.
        min_avg_heart_rate (float): Minimum average heart rate.
        date_from (datetime): Start of the time range.
        date_to (datetime): End of the time range.

    Returns:
        Select: SQLAlchemy query object.
    """

    if min_age < 0:
        raise ValueError("Minimum age cannot be negative")
    if min_avg_heart_rate < 0:
        raise ValueError("Minimum average heart rate cannot be negative")
    if date_from >= date_to:
        raise ValueError("Start date must be earlier than end date")

    avg_heart_rate = func.avg(heart_rates_partitioned.c.heart_rate).label('avg_heart_rate')

    heart_rate_avg_subquery = (
        select(
            heart_rates_partitioned.c.user_id,
            avg_heart_rate
        )
        .where(heart_rates_partitioned.c.timestamp.between(date_from, date_to))
        .group_by(heart_rates_partitioned.c.user_id)
        .having(avg_heart_rate > min_avg_heart_rate)
        .alias("heart_rate_avg")
    )

    query = (
        select(users)
        .join(heart_rate_avg_subquery, users.c.id == heart_rate_avg_subquery.c.user_id)
        .where(
            users.c.birth_date <= func.current_date() - text(f"INTERVAL '{min_age} years'")
        )
    )

    return query


def query_top(user_id: int, date_from: datetime, date_to: datetime, n: int = 10) -> Select:
    """
    Retrieve the top n average heart rates per hour for a user
    within a time range.

    Args:
        user_id (int): User ID.
        date_from (datetime): Start of the time range.
        date_to (datetime): End of the time range.
        n (int): Top n of results to return (default: 10).

    Returns:
        Select: SQLAlchemy query object with the results.
    """
    if n <= 0:
        raise ValueError("Limit must be a positive integer")
    if date_from >= date_to:
        raise ValueError("Start date must be earlier than end date")

    hourly_trunc = func.date_trunc('hour', heart_rates_partitioned.c.timestamp).label('hour_slot')
    avg_heart_rate = func.avg(heart_rates_partitioned.c.heart_rate).label('avg_heart_rate')

    filters = and_(
        heart_rates_partitioned.c.user_id == user_id,
        heart_rates_partitioned.c.timestamp.between(date_from, date_to)
    )

    query = (
        select(hourly_trunc, avg_heart_rate)
        .where(filters)
        .group_by(hourly_trunc)
        .order_by(avg_heart_rate.desc())
        .limit(n)
    )

    return query
