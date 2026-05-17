from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert

from backend.models.database.core import DBSession
from backend.models.database.sensor_data import SensorData

from backend.models.rest.schemas import (
    SensorDataBatchIngest,
    SensorDataIngest,
    SensorDataResponse,
    AggregationResponse,
    SelectiveResponse,
)

router = APIRouter(prefix="/api/v1/sensor", tags=["Sensor Data"])


@router.post(
    "/ingest",
    response_model=SensorDataResponse,
    status_code=201,
)
async def ingest_sensor_data(payload: SensorDataIngest, db: DBSession):
    stmt = insert(SensorData).values(**payload.model_dump())

    stmt = stmt.on_conflict_do_nothing(index_elements=['time'])

    await db.execute(stmt)
    await db.commit()


    return payload


@router.post(
    "/ingest/batch",
    status_code=201,
)
async def ingest_sensor_data_batch(payload: SensorDataBatchIngest, db: DBSession):

    records = [SensorData(**r.model_dump()) for r in payload.records]
    db.add_all(records)
    await db.commit()

    return {"ingested": len(records)}


@router.get(
    "/latest",
    response_model=SensorDataResponse,
    status_code=200
)
async def get_latest_sensor_data(db: DBSession):
    
    result = await db.execute(
        select(SensorData).order_by(SensorData.time.desc()).limit(1)
    )
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="No sensor data found")
    return record


@router.get(
    "/range",
    response_model=list[SensorDataResponse],
    status_code=200
)
async def get_sensor_data_in_range(
    db: DBSession,
    start_time: datetime = Query(..., description="Start time for filtering"),
    end_time: datetime = Query(..., description="End time for filtering"),
    limit: int = Query(100, description="Maximum number of records to return", ge=1, le=10_000)
):
    result = await db.execute(
        select(SensorData)
        .where(SensorData.time >= start_time, SensorData.time <= end_time)
        .order_by(SensorData.time.desc())
        .limit(limit)
    )
    records = result.scalars().all()
    return records


@router.get(
    "/selective",
    response_model=list[SelectiveResponse],
    status_code=200
)
async def get_selective_sensor_data(
    db: DBSession,
    start_time: Optional[datetime] = Query(None, description="Start time for filtering"),
    end_time: Optional[datetime] = Query(None, description="End time for filtering"),
    limit: int = Query(100, description="Maximum number of records to return", ge=1, le=10_000)
):
    statement = select(
        SensorData.time,
        SensorData.temperature_c,
        SensorData.humidity_percent
    ).order_by(SensorData.time.desc()).limit(limit)

    if start_time:
        statement = statement.where(SensorData.time >= start_time)
    if end_time:
        statement = statement.where(SensorData.time <= end_time)
    
    result = await db.execute(statement)
    records = result.mappings().all()

    return [SelectiveResponse(**r) for r in records]


@router.get(
    "/aggregate",
    response_model=list[AggregationResponse],
    status_code=200
)
async def get_aggregated_sensor_data(
    db: DBSession,
    bucket_interval: str = Query("1 hour", description="Time interval for bucketing (e.g., '1 hour', '30 minutes')"),
    start_time: Optional[datetime] = Query(None, description="Start time for filtering"),
    end_time: Optional[datetime] = Query(None, description="End time for filtering"),
    limit: int = Query(100, description="Maximum number of buckets to return", ge=1, le=10_000)
):
    where_clauses = []
    if start_time:
        where_clauses.append(f"time >= :start_time")
    if end_time:
        where_clauses.append(f"time <= :end_time")
    
    where_sql = ("WHERE" + " AND ".join(where_clauses)) if where_clauses else ""

    query = text(f"""
        SELECT time_bucket(CAST(CAST(:bucket_interval AS TEXT) AS INTERVAL), time) AS bucket,
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

    params = {
        "bucket_interval": bucket_interval,
        "limit": limit,
    }
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time
    
    result = await db.execute(query, params)
    records = result.mappings().all()
    return [AggregationResponse(**r) for r in records]