from datetime import datetime
from sqlalchemy import Column, Integer, Double, Boolean, DateTime
from models.database.core import Base

class SensorData(Base):
    __tablename__ = "sensor_data"

    time = Column(
        DateTime(timezone=True),
        primary_key=True,
        nullable=False,
    )

    temperature_c = Column(Double)
    humidity_percent = Column(Double)
    tvoc_ppb = Column(Integer)
    eco2_ppm = Column(Integer)
    raw_h2 = Column(Integer)
    raw_ethanol = Column(Integer)
    pressure_hpa = Column(Double)
    pm10 = Column(Double)
    pm25 = Column(Double)
    nc05 = Column(Double)
    nc10 = Column(Double)
    nc25 = Column(Double)
    fire_alarm = Column(Boolean)