from fastapi import FastAPI
from typing import List

from routes.events import event_router

import uvicorn

app = FastAPI()
app.include_router(event_router,prefix="/event")

if __name__=="__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)