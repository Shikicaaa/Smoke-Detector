from datetime import datetime
from typing import Optional, TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.database.sensor_data import SensorData

if TYPE_CHECKING:
    from backend.services.gql.schema import SensorDataInput


def _row_to_type(record: SensorData):
    from backend.services.gql.schema import SensorDataType
    return SensorDataType(
        time=record.time,
        temperature_c=record.temperature_c,
        humidity_percent=record.humidity_percent,
        tvoc_ppb=record.tvoc_ppb,
        eco2_ppm=record.eco2_ppm,
        raw_h2=record.raw_h2,
        raw_ethanol=record.raw_ethanol,
        pressure_hpa=record.pressure_hpa,
        pm10=record.pm10,
        pm25=record.pm25,
        nc05=record.nc05,
        nc10=record.nc10,
        nc25=record.nc25,
        fire_alarm=record.fire_alarm,
    )


async def get_latest(db: AsyncSession):
    stmt = select(SensorData).order_by(SensorData.time.desc()).limit(1)
    result = await db.execute(stmt)
    record = result.scalar_one_or_none()
    return _row_to_type(record) if record else None


async def get_sensor_data(
    db: AsyncSession,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    limit: int,
):
    stmt = select(SensorData).order_by(SensorData.time.desc()).limit(limit)
    if start_time:
        stmt = stmt.where(SensorData.time >= start_time)
    if end_time:
        stmt = stmt.where(SensorData.time <= end_time)

    result = await db.execute(stmt)
    return [_row_to_type(r) for r in result.scalars().all()]


async def get_aggregated(
    db: AsyncSession,
    bucket_interval: str,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    limit: int,
):
    from backend.services.gql.schema import AggregationType

    where_parts = []
    if start_time:
        where_parts.append("time >= :start_time")
    if end_time:
        where_parts.append("time <= :end_time")

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    query = text(f"""
        SELECT
            time_bucket(CAST(:bucket_interval AS INTERVAL), time) AS bucket,
            AVG(temperature_c) AS avg_temperature_c,
            AVG(humidity_percent) AS avg_humidity_percent,
            AVG(pressure_hpa) AS avg_pressure_hpa,
            AVG(tvoc_ppb) AS avg_tvoc_ppb,
            AVG(eco2_ppm) AS avg_eco2_ppm,
            AVG(pm25) AS avg_pm25,
            AVG(pm10) AS avg_pm10,
            COUNT(*) FILTER (WHERE fire_alarm) AS fire_alarm_count
        FROM sensor_data
        {where_sql}
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT :limit
    """)

    params: dict = {"bucket_interval": bucket_interval, "limit": limit}
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time

    result = await db.execute(query, params)
    rows = result.mappings().all()

    return [
        AggregationType(
            bucket=r["bucket"],
            avg_temperature_c=r["avg_temperature_c"],
            avg_humidity_percent=r["avg_humidity_percent"],
            avg_pressure_hpa=r["avg_pressure_hpa"],
            avg_tvoc_ppb=r["avg_tvoc_ppb"],
            avg_eco2_ppm=r["avg_eco2_ppm"],
            avg_pm25=r["avg_pm25"],
            avg_pm10=r["avg_pm10"],
            fire_alarm_count=r["fire_alarm_count"],
        )
        for r in rows
    ]


async def ingest_single(db: AsyncSession, data: "SensorDataInput"):
    record = SensorData(**{
        k: v for k, v in vars(data).items()
    })
    db.add(record)
    await db.commit()
    await db.refresh(record)
    return _row_to_type(record)


async def ingest_batch(db: AsyncSession, data: list["SensorDataInput"]) -> int:
    records = [SensorData(**{k: v for k, v in vars(d).items()}) for d in data]
    db.add_all(records)
    await db.commit()
    return len(records)