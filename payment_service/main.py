


import json
import logging
import os
import traceback
import asyncio
from database.postgres_client import get_client_by_telegram_id, update_single_field
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime
from utils import Utils
import threading
logger = logging.getLogger(__name__)
load_dotenv()
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")
admin_chat_id = os.getenv("ADMIN_CHAT_ID")

TARIFFS = {
    "add-traffic": {
        "title": "Дополнительный пакет",
        "description": "+10 ГБ для вашего VPN. Идеально для временного увеличения лимита.",
        "price": 11_000,
        "payload": "add-traffic"
    },
    "unlimited-month": {
        "title": "Безлимит на месяц",
        "description": "Полная свобода интернета на 30 дней!",
        "price": 16_900,
        "payload": "unlimited-month"
    },
    "unlimited-year": {
        "title": "Безлимит на год",
        "description": "Экономьте 600 рублей в год! Всего 120 рублей в месяц.",
        "price": 139_000,
        "payload": "unlimited-year"
    }
}


class PaymentProcessor:
    async def precheckout_callback(self, user_data):
        """Подтверждение предварительного платежа"""
        try:
            query = update.pre_checkout_query
            # Проверка, что payload существует в TARIFFS
            if query.invoice_payload not in TARIFFS:
                logger.error(f"Неизвестный payload: {query.invoice_payload}. Доступные: {list(TARIFFS.keys())}")
                await query.answer(ok=False, error_message="Платеж завершился с ошибкой. Обратитесь в поддержку /support.")
            else:
                await query.answer(ok=True)
                logger.info(f"Предварительная обработка платежа прошла успешно для {query.invoice_payload}")
        except Exception as e:
            traceback.print_exc()
            await update.message.reply_text(
                "Ошибка при обработке успешного платежа. \nОбратитесь в поддержку /support")
            logger.error(f"Ошибка при обработке precheckout callback: {e}")
            
    async def successful_payment_callback(self, user_data):
        """Обработка успешного платежа"""
        try:
            logger.info("Обработка успешного платежа")
            payment = update.message.successful_payment
            amount = payment.total_amount / 100  # Переводим из копеек в рубли
            currency = payment.currency
            invoice_payload = payment.invoice_payload
            first_name = update.effective_user.first_name
            user_id = update.effective_user.id
            login = update.effective_user.username
            premium_status_is_valid_until = None

            # Устанавливаем дату окончания премиум статуса в зависимости от тарифа
            if invoice_payload == "unlimited-month":
                premium_status_is_valid_until = datetime.now() + relativedelta(months=1)
            elif invoice_payload == "unlimited-year":
                premium_status_is_valid_until = datetime.now() + relativedelta(years=1)

            # Подтверждение пользователю
            await update.message.reply_text(
                f"Спасибо за оплату! Вы оплатили {amount:.2f} {currency}."
            )

            telegram_id = update.effective_user.id
            client = await get_client_by_telegram_id(telegram_id)
            if client:
                # Применяем тариф
                if invoice_payload == "add-traffic":
                    if client.enabled_status:
                        await Utils.disable_client(client)
                    await Utils.enable_client(client)
                else:
                    if not client.enabled_status:
                        await Utils.enable_client(client)
                    await update_single_field(telegram_id, "has_premium_status", True)
                    await update_single_field(telegram_id, "premium_status_is_valid_until", premium_status_is_valid_until)
            else:
                logger.error(f"Клиент {telegram_id} не найден в базе данных")
                await update.message.reply_text(
                    "Ваш платеж успешно обработан, но вас нет в базе наших клиентов. Пожалуйста, обратитесь в поддержку."
                )

            # Отправляем уведомление администратору о платеже
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=Utils.format_pay_message(user_id, login, first_name, amount, currency, invoice_payload),
                parse_mode="HTML"
            )
        except Exception as e:
            traceback.print_exc()
            await update.message.reply_text(
                "Ошибка при обработке успешного платежа. \nОбратитесь в поддержку"
            )
            logger.error(f"Ошибка при обработке успешного платежа: {e}")
      async def handle_tariff_selection(self, user_data):
        """Обрабатываем выбор тарифа"""
        try:
            query = update.callback_query
            await query.answer()

            tariff_key = query.data
            tariff = TARIFFS.get(tariff_key)

            if not tariff:
                logger.warning(f"Попытка выбрать несуществующий тариф: {tariff_key}")
                await query.message.reply_text(
                    "Ошибка: такого тарифа не существует. \nОбратитесь в поддержку.")
                return

            prices = [LabeledPrice(tariff["title"], tariff["price"])]
            provider_data = {
                "receipt": {
                    "items": [
                        {
                            "description": tariff["title"],
                            "quantity": "1.00",
                            "amount": {
                                "value": f"{tariff['price'] / 100:.2f}",
                                "currency": "RUB"
                            },
                            "vat_code": 1
                        }
                    ]
                }
            }

            # Отправка счета пользователю
            await context.bot.send_invoice(
                chat_id=query.message.chat_id,
                title=tariff["title"],
                description=tariff["description"],
                payload=tariff["payload"],
                provider_token=PROVIDER_TOKEN,
                currency="RUB",
                prices=prices,
                start_parameter=f"start-{tariff_key}",
                need_email=True,
                send_email_to_provider=True,
                provider_data=json.dumps(provider_data)
            )
        except Exception as e:
            traceback.print_exc()
            await update.callback_query.message.reply_text(
                "Ошибка при обработке тарифа. \nОбратитесь в поддержку"
            )
            logger.error(f"Ошибка при обработке тарифа {tariff_key}: {e}")

    async def _start_kafka_consumer(self):
        loop = asyncio.get_event_loop()
        def consume_responses():
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id='config-gateway-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            consumer.subscribe(['config-requests','connect-requests', "status-requests", "qr-requests"])
            for msg in consumer:
                try:
                    data = msg.value
                    correlation_id = data['correlation_id']
                    user_data = data['user_data']
                    task = asyncio.run_coroutine_threadsafe(
                        self._process_message(msg.topic, user_data, correlation_id),
                        loop
                    )
                    task.result() 

                except Exception as e:
                    self.logger.error(f"Error processing Kafka message: {e}")
        
        threading.Thread(target=consume_responses, daemon=True).start()