from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from typing import List, Optional
import pandas as pd
import nest_asyncio
import uvicorn
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import asyncio
from asyncio import Queue
import pytz

# Определение моделей данных
class OnlineRequest(BaseModel):
    phone: int
    token: str
    sid: str
    url: str
    timestamp: int
    ip_insert: Optional[str] = Field(default=None, description="IP-адрес отправителя")
    datetime_insert: Optional[str] = Field(default=None, description="Дата и время вставки в формате ISO 8601")
    date_insert: Optional[str] = Field(default=None, description="Текущая дата записи данных в формате YYYY-MM-DD")

class OnlineExtRequest(BaseModel):
    phone: int
    token: str
    sid: str
    url: str
    timestamp: int
    RegionDef: str
    RegionIp: str
    Device: str
    Browser: str
    OS: str
    IP: str
    ip_insert: Optional[str] = Field(default=None, description="IP-адрес отправителя")
    datetime_insert: Optional[str] = Field(default=None, description="Дата и время вставки в формате ISO 8601")
    date_insert: Optional[str] = Field(default=None, description="Текущая дата записи данных в формате YYYY-MM-DD")

class DailyPhoneData(BaseModel):
    phone: int
    sid: str

class DailyRequest(BaseModel):
    count: int
    token: str
    name: str
    timestamp: int
    phones: List[DailyPhoneData]
    ip_insert: Optional[str] = Field(default=None, description="IP-адрес отправителя")
    datetime_insert: Optional[str] = Field(default=None, description="Дата и время вставки в формате ISO 8601")
    date_insert: Optional[str] = Field(default=None, description="Текущая дата записи данных в формате YYYY-MM-DD")

# Настройка nest_asyncio
nest_asyncio.apply()
app = FastAPI()

# Инициализация BigQuery клиента
credentials = service_account.Credentials.from_service_account_file('project_cred.json')
project_id = 'my_project'
clientBQ = bigquery.Client(credentials=credentials, project=project_id)

# Очереди для буферизации данных
online_buffer = Queue()
online_ext_buffer = Queue()
daily_buffer = Queue()

# Функция для записи данных в BigQuery
async def load_to_gbq(df, dataset_id, table_id, schema):
    if df.shape[0] > 0:
        try:
            df['datetime_insert'] = pd.to_datetime(df['datetime_insert'], errors='coerce')
            df['date_insert'] = pd.to_datetime(df['date_insert'], errors='coerce').dt.date

            job = clientBQ.load_table_from_dataframe(
                df,
                f'{dataset_id}.{table_id}',
                job_config=bigquery.LoadJobConfig(schema=schema)
            )
            print(job.result())
            print(job.state)
        except Exception as ex:
            print(f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}. Ошибка записи в GBQ - {ex}.")

async def process_buffer(buffer: Queue, dataset_id: str, table_id: str, schema: list):
    while True:
        df_list = []
        while not buffer.empty():
            df_list.append(buffer.get_nowait())
        
        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            await load_to_gbq(df, dataset_id, table_id, schema)
        await asyncio.sleep(60)  # Периодическая запись каждые 60 секунд

@app.post("/api/online")
async def receive_online_data(request: Request, data: OnlineRequest):

     # Получение IP-адреса клиента из заголовка X-Forwarded-For или request.client.host
    forwarded_for = request.headers.get('X-Forwarded-For')
    if forwarded_for:
        ip_address = forwarded_for.split(",")[0].strip()  # Берем первый IP из списка
    else:
        ip_address = request.client.host

    # Устанавливаем временную зону на GMT+3
    timezone = pytz.timezone('Europe/Moscow') 
    
    now = datetime.now(timezone)
    data.datetime_insert = now.isoformat()
    data.date_insert = now.date().isoformat()
    data.ip_insert = ip_address

    # Преобразование данных запроса в DataFrame
    df = pd.DataFrame([data.model_dump()])

    # Добавление данных в буфер
    await online_buffer.put(df)

    return {"message": "Online data received successfully", "data": data}

@app.post("/api/online_ext")
async def receive_online_ext_data(request: Request, data: OnlineExtRequest):
    
     # Получение IP-адреса клиента из заголовка X-Forwarded-For или request.client.host
    forwarded_for = request.headers.get('X-Forwarded-For')
    if forwarded_for:
        ip_address = forwarded_for.split(",")[0].strip()  # Берем первый IP из списка
    else:
        ip_address = request.client.host
        
    # Устанавливаем временную зону на GMT+3
    timezone = pytz.timezone('Europe/Moscow') 
    
    now = datetime.now(timezone)
    data.datetime_insert = now.isoformat()
    data.date_insert = now.date().isoformat()
    data.ip_insert = ip_address

    # Преобразование данных запроса в DataFrame
    df = pd.DataFrame([data.model_dump()])

    # Добавление данных в буфер
    await online_ext_buffer.put(df)

    return {"message": "Online ext data received successfully", "data": data}

@app.post("/api/daily")
async def receive_daily_data(request: Request, data: DailyRequest):
    
     # Получение IP-адреса клиента из заголовка X-Forwarded-For или request.client.host
    forwarded_for = request.headers.get('X-Forwarded-For')
    if forwarded_for:
        ip_address = forwarded_for.split(",")[0].strip()  # Берем первый IP из списка
    else:
        ip_address = request.client.host
        
    # Устанавливаем временную зону на GMT+3
    timezone = pytz.timezone('Europe/Moscow') 
    
    now = datetime.now(timezone)
    data.datetime_insert = now.isoformat()
    data.date_insert = now.date().isoformat()
    data.ip_insert = ip_address

    # Преобразование данных запроса в DataFrame
    phones_data = [phone.model_dump() for phone in data.phones]
    df = pd.DataFrame(phones_data)
    df['token'] = data.token
    df['name'] = data.name
    df['timestamp'] = data.timestamp
    df['count'] = data.count
    df['ip_insert'] = data.ip_insert
    df['datetime_insert'] = data.datetime_insert
    df['date_insert'] = data.date_insert

    # Добавление данных в буфер
    await daily_buffer.put(df)

    return {"message": "Daily data received successfully", "data": data}



# Схемы таблиц BigQuery
online_schema = [
    {"name": "phone", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "token", "type": "STRING", "mode": "NULLABLE"},
    {"name": "sid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "ip_insert", "type": "STRING", "mode": "NULLABLE"},
    {"name": "datetime_insert", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "date_insert", "type": "DATE", "mode": "NULLABLE"}
]

online_ext_schema = [
    {"name": "phone", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "token", "type": "STRING", "mode": "NULLABLE"},
    {"name": "sid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "RegionDef", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RegionIp", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Device", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Browser", "type": "STRING", "mode": "NULLABLE"},
    {"name": "OS", "type": "STRING", "mode": "NULLABLE"},
    {"name": "IP", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ip_insert", "type": "STRING", "mode": "NULLABLE"},
    {"name": "datetime_insert", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "date_insert", "type": "DATE", "mode": "NULLABLE"}
]

daily_schema = [
    {"name": "phone", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "sid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "token", "type": "STRING", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "ip_insert", "type": "STRING", "mode": "NULLABLE"},
    {"name": "datetime_insert", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "date_insert", "type": "DATE", "mode": "NULLABLE"}
]

# Функции для запуска фоновых задач
async def background_tasks():
    asyncio.create_task(process_buffer(online_buffer, 'input', 'api_online_data', online_schema))
    asyncio.create_task(process_buffer(online_ext_buffer, 'input', 'api_online_ext_data', online_ext_schema))
    asyncio.create_task(process_buffer(daily_buffer, 'input', 'api_daily_data', daily_schema))



@app.on_event("startup")
async def startup_event():
    await background_tasks()


# Запуск сервера для внешнего IP-адреса
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
