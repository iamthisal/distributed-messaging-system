import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from leader_routes import router as leader_router
import leader_election as le

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(le.bootstrap())
    yield

app = FastAPI(title="Member 4 - Consensus Node", lifespan=lifespan)
app.include_router(leader_router, prefix="/leader")

@app.get("/")
def root():
    return {"node": "Member 4 - Consensus", "node_id": le.NODE_ID, "status": "running"}
