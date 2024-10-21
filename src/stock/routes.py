# from fastapi import APIRouter
# from fastapi.responses import StreamingResponse
# import asyncio
# from kafka import KafkaConsumer
# from multiprocessing import Process
# from .producer import start_websocket
# import json
#
# router = APIRouter(
#     prefix="/api/v1/stockDetails",
#     tags=["stockDetails"],
# )
#
# # Kafka Consumer 설정
# def kafka_consumer(stock_symbol: str):
#     consumer = KafkaConsumer(
#         'real_time_stock_prices',
#         bootstrap_servers=['kafka:9092'],
#         auto_offset_reset='earliest',
#         group_id=f'{stock_symbol}_consumer_group',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#     )
#     return consumer
#
# # 종목 코드에 따른 SSE 실시간 스트리밍 함수
# @router.get("/{stock_symbol}/stream", response_class=StreamingResponse)
# async def stream_stock_data(stock_symbol: str):
#     print(f"Received request for stock symbol: {stock_symbol}")  # 로그 추가
#
#     async def event_generator():
#         # 웹소켓을 다른 프로세스로 실행
#         def run_producer():
#             print(f"Starting producer for stock symbol: {stock_symbol}")  # 로그 추가
#             start_websocket(stock_symbol)
#
#         producer_process = Process(target=run_producer)
#         producer_process.start()
#
#         # Kafka Consumer로부터 데이터를 읽어와 SSE로 전송
#         consumer = kafka_consumer(stock_symbol)
#
#         try:
#             for message in consumer:
#                 stock_data = message.value
#                 yield f"data: {json.dumps(stock_data)}\n\n"  # 실제 Kafka에서 받은 데이터 SSE로 전송
#                 print(f"Sending data for stock symbol {stock_symbol} to SSE: {stock_data}")  # 로그 추가
#                 await asyncio.sleep(1)  # 여유 시간 주기
#         except Exception as e:
#             print(f"Error in event generator: {e}")
#         finally:
#             consumer.close()
#
#     return StreamingResponse(event_generator(), media_type="text/event-stream")


from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import asyncio
from multiprocessing import Process
from .producer import send_mock_data, kafka_consumer
import json

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stockDetails"],
)

# 종목 코드에 따른 SSE 실시간 스트리밍 함수 (Mock 데이터 전용)
@router.get("/{stock_symbol}/stream", response_class=StreamingResponse)
async def stream_stock_data(stock_symbol: str):
    print(f"Received request for stock symbol: {stock_symbol}")  # 로그 추가

    async def event_generator():
        # Mock 데이터를 다른 프로세스로 실행
        mock_process = Process(target=send_mock_data, args=(stock_symbol,))
        mock_process.start()

        # Kafka Consumer로부터 데이터를 읽어와 SSE로 전송
        consumer = kafka_consumer(stock_symbol)

        try:
            for message in consumer:
                stock_data = message.value
                yield f"data: {json.dumps(stock_data)}\n\n"  # 실제 Kafka에서 받은 데이터 SSE로 전송
                print(f"Sending mock data for stock symbol {stock_symbol} to SSE: {stock_data}")  # 로그 추가
                await asyncio.sleep(1)  # 여유 시간 주기
        except Exception as e:
            print(f"Error in event generator: {e}")
        finally:
            consumer.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")
