"""
通用模型客户端接口
支持多种大模型：DeepSeek V3.2, MiniMax 2.5, Gemini 3.1 Pro
"""
import os
import json
import abc
import requests
from typing import Dict, Any, List, Optional
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


class BaseModelClient(abc.ABC):
    """模型客户端基类"""
    
    @abc.abstractmethod
    def generate_content(
        self, 
        prompt: str, 
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 8192
    ) -> str:
        """生成文本内容"""
        pass
    
    @abc.abstractmethod
    def generate_json(
        self, 
        prompt: str, 
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        json_schema: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """生成 JSON 结构化输出"""
        pass


class GeminiClient(BaseModelClient):
    """Google Gemini 模型客户端"""
    
    def __init__(self, api_key: str = None, model: str = "gemini-2.5-flash"):
        from google import genai
        self.client = genai.Client(api_key=api_key or os.environ.get("GEMINI_API_KEY"))
        self.model = model
    
    def generate_content(self, prompt: str, system_prompt: str = None, temperature: float = 0.1, max_tokens: int = 8192) -> str:
        from google.genai import types
        contents = []
        if system_prompt:
            contents.append(types.Content(role="user", parts=[types.Part(text=system_prompt + "\n\n" + prompt)]))
        else:
            contents.append(prompt)
        
        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
        )
        return response.text
    
    def generate_json(self, prompt: str, system_prompt: str = None, temperature: float = 0.1, json_schema: Dict = None) -> Dict[str, Any]:
        from google.genai import types
        contents = []
        if system_prompt:
            contents.append(types.Content(role="user", parts=[types.Part(text=system_prompt + "\n\n" + prompt)]))
        else:
            contents.append(prompt)
        
        config = types.GenerateContentConfig(
            temperature=temperature,
            response_mime_type="application/json",
            response_schema=json_schema,
        )
        
        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=config,
        )
        
        text = response.text
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        
        return json.loads(text)


class DeepSeekClient(BaseModelClient):
    """DeepSeek 模型客户端 (支持 V3.2)"""
    
    def __init__(self, api_key: str = None, model: str = "deepseek-chat"):
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        self.model = model
        self.base_url = "https://api.deepseek.com"
    
    def _call_api(self, messages: List[Dict], temperature: float = 0.1, max_tokens: int = 8192, json_mode: bool = False) -> Dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
        
        response = requests.post(
            f"{self.base_url}/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def generate_content(self, prompt: str, system_prompt: str = None, temperature: float = 0.1, max_tokens: int = 8192) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        result = self._call_api(messages, temperature, max_tokens)
        return result["choices"][0]["message"]["content"]
    
    def generate_json(self, prompt: str, system_prompt: str = None, temperature: float = 0.1, json_schema: Dict = None) -> Dict[str, Any]:
        system_msg = system_prompt or ""
        if json_schema:
            system_msg += f"\n\n请严格按照以下 JSON Schema 返回: {json.dumps(json_schema, ensure_ascii=False)}"
        
        messages = [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt}
        ]
        
        result = self._call_api(messages, temperature, json_mode=True)
        content = result["choices"][0]["message"]["content"]
        
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        return json.loads(content)


class MiniMaxClient(BaseModelClient):
    """MiniMax 模型客户端 (支持 2.5)"""
    
    def __init__(self, api_key: str = None, model: str = "MiniMax-Text-01"):
        self.api_key = api_key or os.environ.get("MINIMAX_API_KEY")
        self.model = model
        self.base_url = "https://api.minimax.chat/v1"
    
    def _call_api(self, messages: List[Dict], temperature: float = 0.1, max_tokens: int = 8192, json_mode: bool = False) -> Dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        # MiniMax doesn't support response_format like OpenAI, so we skip it
        # and rely on prompting the model to return JSON
        
        response = requests.post(
            f"{self.base_url}/text/chatcompletion_v2",
            headers=headers,
            json=payload,
            timeout=120
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"MiniMax API response: {result}")
        return result
    
    def generate_content(self, prompt: str, system_prompt: str = None, temperature: float = 0.1, max_tokens: int = 8192) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        result = self._call_api(messages, temperature, max_tokens)
        return result["choices"][0]["message"]["content"]
    
    def generate_json(self, prompt: str, system_prompt: str = None, temperature: float = 0.1, json_schema: Dict = None) -> Dict[str, Any]:
        system_msg = system_prompt or ""
        if json_schema:
            system_msg += f"\n\n请严格按照以下 JSON Schema 返回: {json.dumps(json_schema, ensure_ascii=False)}"
        
        messages = [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt}
        ]
        
        result = self._call_api(messages, temperature, json_mode=True)
        content = result["choices"][0]["message"]["content"]
        
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        return json.loads(content)


def get_model_client(model_type: str = None) -> BaseModelClient:
    """
    根据配置获取模型客户端
    
    model_type: 
        - "gemini" 或 "gemini-2.5-flash" 或 "gemini-3.1-pro"
        - "deepseek" 或 "deepseek-v3.2"
        - "minimax" 或 "minimax-2.5"
        - 或从环境变量 MODEL_PROVIDER 自动选择
    """
    if not model_type:
        model_type = os.environ.get("MODEL_PROVIDER", "gemini").lower()
    
    if "gemini" in model_type:
        if "3.1" in model_type or "pro" in model_type:
            return GeminiClient(model="gemini-3.1-pro-preview")
        return GeminiClient(model="gemini-3.1-pro-preview")
    
    elif "deepseek" in model_type:
        return DeepSeekClient(model="deepseek-reasoner")
    
    elif "minimax" in model_type:
        return MiniMaxClient(model="MiniMax-M2.5")
    
    else:
        logger.warning(f"Unknown model type: {model_type}, defaulting to Gemini")
        return GeminiClient()
