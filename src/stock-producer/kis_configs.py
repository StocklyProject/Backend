import requests
import json
import time

def get_approval(key, secret):
    url = 'https://openapivts.koreainvestment.com:29443'  # 모의투자계좌 URL
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": key,
        "secretkey": secret
    }
    PATH = "oauth2/Approval"
    URL = f"{url}/{PATH}"
    time.sleep(0.05)

    # 요청을 보내고 응답을 확인
    res = requests.post(URL, headers=headers, data=json.dumps(body))

    # 응답을 출력해서 확인해보기
    print(f"Response Status Code: {res.status_code}")
    print(f"Response Body: {res.text}")

    # 응답에서 approval_key 추출
    approval_key = res.json().get("approval_key")

    # approval_key 출력
    print(f"Approval Key: {approval_key}")

    return approval_key
