from fastapi import FastAPI, Depends, HTTPException
from auth import verify_token, create_access_token
from config import get_wireguard_config
from pydantic import BaseModel

app = FastAPI()


class ClientConfigRequest(BaseModel):
    user_name: str
    token: str


@app.post("/get-config")
async def get_config(request: ClientConfigRequest):
    await verify_token(request.token)

    config = await get_wireguard_config(request.user_name)
    return config


@app.post("/login")
async def login(user_name: str):
    access_token = await create_access_token(data={"user_name": user_name})
    return {"access_token": access_token, "token_type": "bearer"}
