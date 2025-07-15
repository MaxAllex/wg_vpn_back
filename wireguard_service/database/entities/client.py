from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel

class Client(BaseModel):
    id: str
    telegram_id: Optional[int] = None
    wg_id: Optional[str] = None
    has_premium_status: Optional[bool] = False
    premium_status_is_valid_until: Optional[datetime] = None
    config_file: Optional[str] = None
    enabled_status: Optional[bool] = False
    created_at: Optional[datetime] = None
    need_to_disable: Optional[bool] = False
    wg_server: Optional[str] = None
    jwt_version: Optional[int] = 0
    latest_handshake: Optional[datetime] = None
    app_token: Optional[str] = None
    email: Optional[str] = None
    yookassa_payment_method_id: Optional[str] = None
    yookassa_autopayment_active: Optional[bool] = False
    yookassa_last_payment_type: Optional[str] = None
    yookassa_subscription_type: Optional[str] = None


    # Метод для преобразования в словарь
    def to_dict(self):
        return self.model_dump()
