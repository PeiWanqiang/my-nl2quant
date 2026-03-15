import requests

code = """
def apply_strategy(df):
    import pandas as pd
    import numpy as np
    global final_codes
    final_codes = ["000001", "000002"]
"""
resp = requests.post("http://127.0.0.1:8000/api/v1/quant/execute", json={"code": code})
print(resp.status_code)
print(resp.text)
