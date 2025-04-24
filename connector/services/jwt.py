import jwt
import os
from fastapi import HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv
import jwt.exceptions

#load_dotenv()
#SECRET_KEY = os.getenv("SECRET_KEY")
#ALGORITHM = os.getenv("ALGORITHM", "HS256")
#ACCESS_TOKEN_EXPIRE_MINUTES = os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30)
#REFRESH_TOKEN_EXPIRE_MINUTES = os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", 43200) #1 month


class JWTService:
    def __init__(self):
        SECRET_KEY = os.getenv("SECRET_KEY")
        ALGORITHM = os.getenv("ALGORITHM", "HS256")
        ACCESS_TOKEN_EXPIRE_MINUTES = os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30)
        REFRESH_TOKEN_EXPIRE_MINUTES = os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", 43200) #1 month
        self.secret_key = SECRET_KEY
        self.algorithm = ALGORITHM
        self.access_token_expire = int(ACCESS_TOKEN_EXPIRE_MINUTES)
        self.refresh_token_expire = int(REFRESH_TOKEN_EXPIRE_MINUTES)
    
    async def create_token(self, data: dict, expires_delta: timedelta = timedelta(0)):
        if (expires_delta == timedelta(0)):
            expires_delta = timedelta(minutes=self.access_token_expire)
        to_encode = data.copy()
        expire = datetime.now() + expires_delta
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    async def verify_token(self, token: str):
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            return "Token expired"
        except Exception as e:
            return f"Invalid token"
    
    async def refresh_tokens(self, token:str):
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            access_token = await self.create_token(payload, timedelta(minutes=self.access_token_expire))
            refresh_token = await self.create_token(payload, timedelta(minutes=self.refresh_token_expire))
            return {"access_token": access_token, "refresh_token": refresh_token}
        except jwt.ExpiredSignatureError:
            return "Token expired"
        except Exception as e:
            return f"Invalid token"


#async def create_access_token(data: dict, expires_delta: timedelta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)):
#    to_encode = data.copy()
#    expire = datetime.now() + expires_delta
#    to_encode.update({"exp": expire})
#    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
#    return encoded_jwt


#async def verify_token(token: str):
#    try:
#        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#        return payload
#    except jwt.PyJWTError:
#        raise HTTPException(status_code=401, detail="Invalid token")
