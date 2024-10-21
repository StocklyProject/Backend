# # import json
# # import websocket
# # from kafka import KafkaProducer
# # from kis_configs import get_approval
# # from src.configs import APP_SECRET, APP_KEY
# #
# # # Kafka Producer 설정
# # producer = KafkaProducer(
# #     bootstrap_servers=['localhost:9094'],  # Kafka 브로커 주소
# #     value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
# # )
# #
# # # 실제 사용 중인 appkey와 secretkey
# # g_appkey = APP_KEY
# # g_appsecret = APP_SECRET
# #
# # # approval_key를 가져옴
# # g_approval_key = get_approval(g_appkey, g_appsecret)
# #
# # h = {
# #     "appkey": g_appkey,
# #     "appsecret": g_appsecret
# # }
# # b = {
# #     "header": {
# #         "approval_key": g_approval_key,
# #         "custtype": "P",
# #         "tr_type": "1",
# #         "content-type": "utf-8"
# #     },
# #     "body": {
# #         "input": {
# #             "tr_id": "H0STCNT0",  # API명
# #             "tr_key": "373220"    # 종목번호
# #         }
# #     }
# # }
# #
# # def on_message(ws, data):
# #     if data[0] in ['0', '1']:  # 시세 데이터일 경우
# #         d1 = data.split("|")
# #         if len(d1) >= 4:
# #             recvData = d1[3]
# #             result = recvData.split("^")
# #             stock_data = {
# #                 "date": result[1],
# #                 "open": result[7], # 시가
# #                 "close": result[2], # 종가(현재가)
# #                 "day_high": result[8], # 1일 최고가
# #                 "day_low": result[9], # 1일 최저가
# #                 "price_change": result[4], # 가격 상승/하락폭
# #                 "price_change_late": result[5], # 등락률
# #                 "volume": result[12], # 실시간 체결 거래량
# #                 "transaction_volume": result[13],# 당일 누적 거래량
# #                 "volume_price": result[14], # 당일 누적 거래 대금
# #             }
# #             # Kafka로 데이터 전송
# #             producer.send('real_time_stock_prices', stock_data)
# #             producer.flush()
# #
# #             # 콘솔 출력 (디버깅용)
# #             print(f"Sent to Kafka: {stock_data}")
# #         else:
# #             print('Data Size Error=', len(d1))
# #     else:
# #         recv_dic = json.loads(data)
# #         tr_id = recv_dic['header']['tr_id']
# #
# #         if tr_id == 'PINGPONG':
# #             ws.send(data, websocket.ABNF.OPCODE_PING)
# #         else:
# #             print('tr_id=', tr_id, '\nmsg=', data)
# #
# #
# # # WebSocket 연결 에러 처리
# # def on_error(ws, error):
# #     print('error=', error)
# #
# #
# # # WebSocket 연결 종료 처리
# # def on_close(ws, status_code, close_msg):
# #     print('on_close close_status_code=', status_code, " close_msg=", close_msg)
# #
# #
# # # WebSocket 연결 시 초기화 작업
# # def on_open(ws):
# #     print('on_open send data=', json.dumps(b))
# #     ws.send(json.dumps(b), websocket.ABNF.OPCODE_TEXT)
# #
# #
# # # WebSocket 연결 설정
# # ws = websocket.WebSocketApp("ws://ops.koreainvestment.com:31000",
# #                             on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
# #
# # # WebSocket 실행
# # ws.run_forever()
#
# import json
# from sys import api_version
#
# from kafka import KafkaProducer
# from mysql.connector import apilevel
#
# from .kis_configs import get_approval
# from src.configs import APP_SECRET, APP_KEY
# import websocket
#
# # Kafka Producer 설정
# producer = KafkaProducer(
#     bootstrap_servers=['kafka:9092'],  # Kafka 브로커 주소
#     api_version=(0, 11, 5),
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
# )
#
# # 실제 사용 중인 appkey와 secretkey
# g_appkey = APP_KEY
# g_appsecret = APP_SECRET
#
# # approval_key를 가져옴
# g_approval_key = get_approval(g_appkey, g_appsecret)
# print(f"Approval key obtained: {g_approval_key}")  # 로그 추가
#
# # 종목 코드를 동적으로 받아 처리할 수 있도록 수정
# def create_message(stock_symbol):
#     message = {
#         "header": {
#             "approval_key": g_approval_key,
#             "custtype": "P",
#             "tr_type": "1",
#             "content-type": "utf-8"
#         },
#         "body": {
#             "input": {
#                 "tr_id": "H0STCNT0",  # API명
#                 "tr_key": stock_symbol  # 종목번호
#             }
#         }
#     }
#     print(f"Message created for stock symbol {stock_symbol}: {message}")  # 로그 추가
#     return message
#
# # WebSocket 메시지 수신 시 처리하는 함수
# def on_message(ws, data):
#     print(f"Received data from WebSocket: {data}")  # 로그 추가
#
#     if data[0] in ['0', '1']:  # 시세 데이터일 경우
#         d1 = data.split("|")
#         if len(d1) >= 4:
#             recvData = d1[3]
#             result = recvData.split("^")
#             stock_data = {
#                 "date": result[1],
#                 "open": result[7],  # 시가
#                 "close": result[2],  # 종가(현재가)
#                 "day_high": result[8],  # 1일 최고가
#                 "day_low": result[9],  # 1일 최저가
#                 "price_change": result[4],  # 가격 상승/하락폭
#                 "price_change_late": result[5],  # 등락률
#                 "volume": result[12],  # 실시간 체결 거래량
#                 "transaction_volume": result[13],  # 당일 누적 거래량
#                 "volume_price": result[14],  # 당일 누적 거래 대금
#             }
#             # Kafka로 데이터 전송
#             try:
#                 print(f"Sending data to Kafka: {stock_data}")  # 로그 추가
#                 producer.send('real_time_stock_prices', stock_data)
#                 producer.flush()
#                 print("Message sent to Kafka successfully.")
#             except Exception as e:
#                 print(f"Error sending data to Kafka: {e}")
#         else:
#             print(f'Data Size Error= {len(d1)}')  # 로그 추가
#     else:
#         recv_dic = json.loads(data)
#         tr_id = recv_dic['header']['tr_id']
#         if tr_id == 'PINGPONG':
#             ws.send(data, websocket.ABNF.OPCODE_PING)
#         else:
#             print(f'tr_id= {tr_id}, msg= {data}')  # 로그 추가
#
# # WebSocket 연결 에러 처리
# def on_error(ws, error):
#     print(f'Error: {error}')  # 로그 추가
#
# # WebSocket 연결 종료 처리
# def on_close(ws, status_code, close_msg):
#     print(f'Connection closed. Status code: {status_code}, Message: {close_msg}')  # 로그 추가
#
# # WebSocket 연결 시 초기화 작업
# def on_open(ws, stock_symbol):
#     message = create_message(stock_symbol)
#     print(f'on_open send data= {json.dumps(message)}')  # 로그 추가
#     ws.send(json.dumps(message), websocket.ABNF.OPCODE_TEXT)
#
# # WebSocket 실행 함수
# def start_websocket(stock_symbol):
#     print(f"Starting WebSocket for stock symbol: {stock_symbol}")  # 로그 추가
#     ws = websocket.WebSocketApp(
#         "ws://ops.koreainvestment.com:31000",
#         on_open=lambda ws: on_open(ws, stock_symbol),
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close
#     )
#     ws.run_forever()
#

import json
import time
import random
from kafka import KafkaProducer, KafkaConsumer

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Kafka 브로커 주소
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
)

# Mock 데이터 생성 함수
def generate_mock_stock_data(stock_symbol):
    return {
        "stock_symbol": stock_symbol,
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),  # 현재 시간
        "open": round(random.uniform(100, 200), 2),  # 랜덤 시가
        "close": round(random.uniform(100, 200), 2),  # 랜덤 종가
        "day_high": round(random.uniform(200, 300), 2),  # 랜덤 최고가
        "day_low": round(random.uniform(100, 150), 2),  # 랜덤 최저가
        "price_change": round(random.uniform(-10, 10), 2),  # 랜덤 가격 변화
        "price_change_rate": round(random.uniform(-5, 5), 2),  # 랜덤 등락률
        "volume": random.randint(1000, 10000),  # 랜덤 거래량
        "transaction_volume": random.randint(10000, 100000),  # 랜덤 누적 거래량
        "volume_price": random.randint(100000, 1000000)  # 랜덤 거래 대금
    }

# Mock 데이터를 Kafka에 전송
def send_mock_data(stock_symbol):
    while True:
        mock_data = generate_mock_stock_data(stock_symbol)
        print(f"Sending mock data to Kafka: {mock_data}")
        producer.send('real_time_stock_prices', mock_data)
        producer.flush()
        time.sleep(2)  # 2초 간격으로 데이터 전송

# Kafka Consumer 설정
def kafka_consumer(stock_symbol: str):
    consumer = KafkaConsumer(
        'real_time_stock_prices',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id=f'{stock_symbol}_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer