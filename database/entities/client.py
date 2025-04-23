from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel


class Client(BaseModel):
    id: UUID
    telegram_id: int
    wg_id: str
    has_premium_status: bool
    premium_status_is_valid_until: Optional[datetime]
    config_file: Optional[str]
    qr_code: Optional[str]
    enabled_status: bool
    created_at: Optional[datetime]
    need_to_disable: bool
    wg_server: Optional[str]
    last_used_gigabytes: float
    used_gigabytes: float
    max_gigabytes: float
    jwt_version: int 


    
    # Метод для преобразования в словарь
    def to_dict(self):
        return self.model_dump()
