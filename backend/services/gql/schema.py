from datetime import datetime
from typing import Optional
import strawberry
from strawberry.types import Info
from strawberry.schema.config import StrawberryConfig

from backend.services.gql import resolvers

@strawberry.type
class SensorDataType:
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


@strawberry.type
class AggregationType:
    bucket: datetime
    avg_temperature_c: Optional[float] = None
    avg_humidity_percent: Optional[float] = None
    avg_pressure_hpa: Optional[float] = None
    avg_tvoc_ppb: Optional[float] = None
    avg_eco2_ppm: Optional[float] = None
    avg_pm25: Optional[float] = None
    avg_pm10: Optional[float] = None
    fire_alarm_count: Optional[int] = None


@strawberry.type
class IngestResult:
    success: bool
    inserted: int


@strawberry.input
class SensorDataInput:
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



@strawberry.type
class Query:

    @strawberry.field(
        description="Fetch any combination of sensor fields. "
                    "Client selects only the fields it needs — no over-fetching."
    )
    async def sensor_data(
        self,
        info: Info,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[SensorDataType]:
        db = info.context["db"]
        return await resolvers.get_sensor_data(db, start_time, end_time, limit)

    @strawberry.field(description="Get the single most recent sensor reading.")
    async def latest(self, info: Info) -> Optional[SensorDataType]:
        db = info.context["db"]
        return await resolvers.get_latest(db)

    @strawberry.field(
        description="Time-bucket aggregation over a historical range. "
                    "bucket_interval accepts PostgreSQL interval strings: "
                    "'1 hour', '30 minutes', '1 day', etc."
    )
    async def aggregate(
        self,
        info: Info,
        bucket_interval: str = "1 hour",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[AggregationType]:
        db = info.context["db"]
        return await resolvers.get_aggregated(db, bucket_interval, start_time, end_time, limit)


@strawberry.type
class Mutation:

    @strawberry.mutation(
        description="Ingest a single sensor reading."
    )
    async def ingest(self, info: Info, data: SensorDataInput) -> SensorDataType:
        db = info.context["db"]
        return await resolvers.ingest_single(db, data)

    @strawberry.mutation(
        description="Batch ingest multiple sensor readings in one request."
    )
    async def ingest_batch(self, info: Info, data: list[SensorDataInput]) -> IngestResult:
        db = info.context["db"]
        inserted = await resolvers.ingest_batch(db, data)
        return IngestResult(success=True, inserted=inserted)


schema = strawberry.Schema(query=Query, mutation=Mutation, config=StrawberryConfig(auto_camel_case=False))