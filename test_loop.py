import requests
for i in range(5):
    try:
        resp = requests.post("http://127.0.0.1:8000/api/v1/chat/negotiate", json={"user_input": "连续3年亏损的股票", "current_conditions": []})
        if resp.status_code != 200:
            print(f"Error {resp.status_code}: {resp.text}")
        else:
            print(f"Success {i}")
    except Exception as e:
        print(e)
