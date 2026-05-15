from fastapi import FastAPI
from backend.services.rest.router import router as sensor_router

def register_routes(app: FastAPI):
    app.include_router(sensor_router)