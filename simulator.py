import asyncio
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from backend.services.env import DATABASE_URL
from backend.models.database.sensor_data import SensorData

LOCAL_DB_URL = DATABASE_URL.replace("@db:", "@localhost:")

engine = create_async_engine(LOCAL_DB_URL, echo=False)
SessionLocal = async_sessionmaker(bind=engine)

async def run_simulation():
    is_fire = True
    fire_duration_left = 60

    async with SessionLocal() as db:
        print("Started (Ctrl + C to stop)...")
        
        while True:
            now = datetime.now(timezone.utc)

            if not is_fire and random.random() < 0.00001:
                print("\n🔥 WARNING: Fire detected!")
                is_fire = True
                fire_duration_left = random.randint(30, 120)

            if is_fire:
                temp = round(random.uniform(66.0, 150.0), 2)
                hum = round(random.uniform(5.0, 15.0), 2)
                tvoc = random.randint(2100, 15000)
                eco2 = random.randint(3100, 10000)
                pm25 = round(random.uniform(155.0, 600.0), 2)
                pm10 = round(random.uniform(310.0, 1000.0), 2)
                raw_ethanol = random.randint(30000, 45000)
                fire_alarm = True

                fire_duration_left -= 1
                if fire_duration_left <= 0:
                    print("\nCondition stabilized. Fire is extinguished. Returning to normal.")
                    is_fire = False
            else:
                temp = round(random.uniform(20.0, 25.0), 2)
                hum = round(random.uniform(40.0, 50.0), 2)
                tvoc = random.randint(0, 100)
                eco2 = random.randint(400, 600)
                pm25 = round(random.uniform(1.0, 15.0), 2)
                pm10 = round(random.uniform(5.0, 20.0), 2)
                raw_ethanol = random.randint(15000, 20000)
                fire_alarm = False

            nc05 = round(pm25 * 2.5, 2)
            nc10 = round(pm10 * 1.5, 2)
            nc25 = round(pm10 * 0.5, 2)

            record = SensorData(
                time=now,
                temperature_c=temp,
                humidity_percent=hum,
                tvoc_ppb=tvoc,
                eco2_ppm=eco2,
                raw_h2=random.randint(12000, 15000),
                raw_ethanol=raw_ethanol,
                pressure_hpa=round(random.uniform(930.0, 945.0), 2),
                pm10=pm10,
                pm25=pm25,
                nc05=nc05,
                nc10=nc10,
                nc25=nc25,
                fire_alarm=fire_alarm
            )

            db.add(record)
            await db.commit()

            status_icon = "🔥 FIRE" if fire_alarm else "✅ OK"
            print(f"[{now.strftime('%H:%M:%S')}] {status_icon} | Temp: {temp}°C | eCO2: {eco2}ppm | PM2.5: {pm25} | TVOC: {tvoc}")

            await asyncio.sleep(1)

def main():
    try:
        asyncio.run(run_simulation())
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")

if __name__ == "__main__":
    main()