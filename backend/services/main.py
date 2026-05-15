from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.services import api


allowed_origins = [
    "*",
    # "http://localhost:3000",
]

app = FastAPI(
    title="IoT Smoke Detection API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# router = api.register_routes()

api.register_routes(app)