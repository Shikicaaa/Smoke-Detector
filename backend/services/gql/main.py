from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from backend.models.database.core import AsyncSessionLocal
from backend.services.gql.schema import schema


async def get_context():
    async with AsyncSessionLocal() as db:
        yield {"db": db}


graphql_router = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphql_ide="graphiql",
)

app = FastAPI(
    title="IoT Sensor Data — GraphQL",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graphql_router, prefix="/graphql")