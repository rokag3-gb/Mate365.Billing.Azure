import os
import sys

import requests

from src.tool.dotenv import get_variables_from_dotenv


class AzurePartnerCenterEnv:
    __instance = None

    @classmethod
    def __getInstance(cls):
        return cls.__instance

    @classmethod
    def instance(cls, *args, **kargs):
        cls.__instance = cls(*args, **kargs)
        cls.instance = cls.__getInstance
        return cls.__instance

    def __init__(self) -> None:
        # .env 파일 존재시, 환경변수 업데이트 후 불러오기.
        env_path = os.path.join(os.path.dirname(sys.modules['__main__'].__file__), '.env')
        if os.path.exists(env_path):
            os.environ.update(get_variables_from_dotenv(env_path))
        self.appid = os.environ['APPID']
        self.secret = os.environ['SECRET']
        self.tenant = os.environ['TENANT']
        self.baseurl = os.environ['PARTNER_CENTER_API_BASEURL'] or 'https://api.partnercenter.microsoft.com'
        self.__access_token = None

    @property
    def access_token(self) -> str:
        if self.__access_token is None:
            self.refresh_token()
        return self.__access_token

    def refresh_token(self) -> None:
        self.__get_token()

    def __get_token(self, resource: str = 'https://graph.windows.net') -> None:
        """
        Azure Partner Center API를 호출하기 위한 auth token 발급
        :param resource:
        :return:
        """
        url = f'https://login.microsoftonline.com/{self.tenant}/oauth2/token'
        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'charset': 'utf-8'
        }
        data = {
            'resource': resource,
            'client_id': self.appid,
            'client_secret': self.secret,
            'grant_type': 'client_credentials'
        }
        r = requests.post(url=url, data=data, headers=headers)
        r.raise_for_status()
        # TODO: 에러처리
        self.__access_token = r.json()['access_token']
