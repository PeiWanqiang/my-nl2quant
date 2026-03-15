import requests
resp = requests.post("http://127.0.0.1:8000/api/v1/chat/negotiate", json={"user_input": "连续3年亏损的股票", "current_conditions": []})
print(resp.status_code)
print(resp.text)
