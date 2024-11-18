import json
import os
import websockets
import aiohttp
import threading
import time
import asyncio
from typing import Dict, List
from src.common.producer import send_to_kafka, init_kafka_producer
from src.logger import logger
import requests
from .crud import get_company_details
import random

TOPIC_STOCK_DATA = "real_time_asking_prices"


# 승인 키 가져오는 함수
async def get_approval():
    url = 'https://openapivts.koreainvestment.com:29443/oauth2/Approval'
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": os.getenv("APP_KEY"),
        "secretkey": os.getenv("APP_SECRET")
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as res:
            data = await res.json()
            return data.get("approval_key")

# WebSocket 구독 메시지 생성 함수
async def subscribe(websocket, app_key, stock_code):
    message = json.dumps({
        "header": {
            "approval_key": app_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {
            "input": {
                "tr_id": "H0STASP0",
                "tr_key": stock_code
            }
        }
    })
    await websocket.send(message)
    logger.info(f"Subscribed to H0STCNT0 for stock: {stock_code}")

# WebSocket 메시지 처리 함수 (비동기 작업으로 메시지를 큐에 추가)
async def handle_message(data_queue: asyncio.Queue, message: str, stock_symbol: str):
    kafka_data = process_data_for_kafka(message, stock_symbol)
    if kafka_data:
        await data_queue.put(kafka_data)

# WebSocket 핸들러 함수
async def websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    approval_key = await get_approval()
    if not approval_key:
        logger.error("Approval key not obtained, terminating connection.")
        return

    url = "ws://ops.koreainvestment.com:31000"
    async with websockets.connect(url, ping_interval=60) as websocket:
        # 각 종목에 대해 구독
        for stock in stock_symbols:
            await subscribe(websocket, approval_key, stock["symbol"])

        # 메시지 수신 및 처리
        async for message in websocket:
            d1 = message.split("|")
            if len(d1) >= 3:
                stock_symbol = d1[3].split("^")[0]
                await handle_message(data_queue, message, stock_symbol)

async def kafka_producer_task(data_queue: asyncio.Queue, producer, topic="real_time_stock_prices"):
    while True:
        data = await data_queue.get()
        if data is None:  # 종료 신호
            break

        try:
            # 데이터 형식에 따라 직렬화 처리
            if isinstance(data, bytes):  # 이미 바이트 형식인 경우
                serialized_data = data
            elif isinstance(data, str):  # JSON 문자열인 경우
                serialized_data = data.encode('utf-8')
            elif isinstance(data, dict):  # 딕셔너리인 경우 JSON 직렬화
                serialized_data = json.dumps(data).encode('utf-8')
            else:
                raise TypeError(f"Unexpected data format: {type(data)}")

            # Kafka로 데이터 전송
            await producer.send_and_wait(topic, value=serialized_data)
            logger.debug(f"Data to be sent to Kafka: {data}, Type: {type(data)}")
            if isinstance(data, dict):
                logger.info(f"Sent data to Kafka for symbol: {data.get('symbol', 'unknown')}")
            else:
                logger.info("Sent data to Kafka.")

        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
        finally:
            data_queue.task_done()


# WebSocket 에러 및 종료 핸들러
def on_error(ws, error):
    logger.error(f'WebSocket error occurred: {error}')
    if isinstance(error, OSError) and error.errno == 32:
        logger.error("Broken pipe error detected. Connection might be closed unexpectedly.")

def on_close(ws, status_code, close_msg):
    logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')

def process_data_for_kafka(data, stock_symbol):
    stock_info = get_company_details(stock_symbol)
    id = stock_info.get("id")
    name = stock_info.get("name")

    if not stock_info or "id" not in stock_info or "name" not in stock_info:
        logger.error(f"No valid company information for symbol: {stock_symbol}")
        return None

    try:
        # 데이터 파싱
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")
            if len(result) > 42:  # 필수 호가 데이터가 포함된 최소 인덱스 확인

                # 매도 호가 및 잔량 (3~10위)
                sell_prices = {
                    f"sell_price_{i + 3}": result[12 - i] for i in range(8)
                }
                sell_volumes = {
                    f"sell_volume_{i + 3}": result[32 - i] for i in range(8)
                }

                # 매수 호가 및 잔량 (1~8위)
                buy_prices = {
                    f"buy_price_{i + 1}": result[13 + i] for i in range(8)
                }
                buy_volumes = {
                    f"buy_volume_{i + 1}": result[33 + i] for i in range(8)
                }

                # 필요한 정보 딕셔너리 생성
                stock_data = {
                    "id": id,
                    "symbol": stock_symbol,
                    "name": name,
                    **sell_prices,
                    **sell_volumes,
                    **buy_prices,
                    **buy_volumes
                }
                return stock_data
            else:
                logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for Kafka: {e}")
    return None

# WebSocket 연결을 비동기적으로 실행
async def run_asking_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()
    
    # Kafka Producer 비동기 초기화
    producer = await init_kafka_producer()
    if producer is None:
        logger.error("Kafka producer initialization failed. Exiting.")
        return data_queue

    # Kafka 전송 작업 비동기 시작
    asyncio.create_task(kafka_producer_task(data_queue, producer))
    
    # WebSocket 연결 핸들러 비동기 시작
    await websocket_handler(stock_symbols, data_queue)

    return data_queue
