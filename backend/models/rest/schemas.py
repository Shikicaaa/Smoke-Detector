from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class SensorDataResponse(BaseModel):
    time: datetime
    temperature_c: Optional[float] = None
    humidity_percent: Optional[float] = None
    tvoc_ppb: Optional[int] = None
    eco2_ppm: Optional[int] = None
    raw_h2: Optional[int] = None
    raw_ethanol: Optional[int] = None
    pressure_hpa: Optional[float] = None
    pm10: Optional[float] = None
    pm25: Optional[float] = None
    nc05: Optional[float] = None
    nc10: Optional[float] = None
    nc25: Optional[float] = None
    fire_alarm: Optional[bool] = None

    model_config = {"from_attributes": True}


class SensorDataIngest(BaseModel):
    time: datetime
    temperature_c: Optional[float] = None
    humidity_percent: Optional[float] = None
    tvoc_ppb: Optional[int] = None
    eco2_ppm: Optional[int] = None
    raw_h2: Optional[int] = None
    raw_ethanol: Optional[int] = None
    pressure_hpa: Optional[float] = None
    pm10: Optional[float] = None
    pm25: Optional[float] = None
    nc05: Optional[float] = None
    nc10: Optional[float] = None
    nc25: Optional[float] = None
    fire_alarm: Optional[bool] = None


class SensorDataBatchIngest(BaseModel):
    records: list[SensorDataIngest]


class AggregationResponse(BaseModel):
    bucket: datetime
    avg_temperature_c: Optional[float] = None
    avg_humidity_percent: Optional[float] = None
    avg_pressure_hpa: Optional[float] = None
    avg_tvoc_ppb: Optional[float] = None
    avg_eco2_ppm: Optional[float] = None
    avg_pm25: Optional[float] = None
    avg_pm10: Optional[float] = None
    fire_alarm_count: Optional[int] = None


class SelectiveResponse(BaseModel):
    time: datetime
    temperature_c: Optional[float] = None
    humidity_percent: Optional[float] = None