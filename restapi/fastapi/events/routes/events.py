from fastapi import APIRouter
from models.events import Event

event_router = APIRouter(
    tags=["Events"]
)

@event_router.get("/", response_model=list())
async def getEvents()->list():
    events = list()
    events.append(Event(date="20230611",user="park"))
    return events