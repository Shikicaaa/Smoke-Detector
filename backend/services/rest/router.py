from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy import select, func, text

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
