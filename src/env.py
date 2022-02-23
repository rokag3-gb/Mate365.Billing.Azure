import argparse
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
        parser = argparse.ArgumentParser()
        parser.add_argument('-f', '--file', action='store_true', help='.env파일을 통한 환경변수 등록')
        parser.add_argument('--database-password', type=str, help='')
        parser.add_argument('--mfa-auth-issue', action='store_true', help='MFA 인증으로 토큰 발급하여 저장')
        parser.add_argument('--mfa-auth', type=str, help='MFA 인증으로 발급한 토큰으로 인증') #MFA 인증으로 발급한 토큰으로 인증
        parser.add_argument('--app-secret', type=str, help='')
        parser.add_argument('--daily-usage', action='store_true', help='일별 사용량 수집')
        parser.add_argument('--daily-usage-update', action='store_true', help='일별 사용량 수집에 대한 과거 데이터 업데이트')
        parser.add_argument('--update-period', type=int, help='업데이트 기간(어제날짜부터의 기간)')
        parser.add_argument('--monthly-invoice', action='store_true', help='월별 인보이스 수집')
        parser.add_argument('--price-update', action='store_true', help='Azure 제품군 가격 업데이트')
        parser.add_argument('--azplan-usage-update', type=str, help="Azure Plan 구독 사용량 데이터 갱신 (current, previous, OR Azure Partner Invoice ID)")
        
        args = parser.parse_args()
        # 실행 로직 선택(bool)
        self.daily_usage = args.daily_usage
        self.daily_usage_update = args.daily_usage_update
        self.update_period = args.update_period
        self.monthly_invoice = args.monthly_invoice
        self.price_update = args.price_update
        self.mfa_auth_issue = args.mfa_auth_issue
        self.mfa_auth = args.mfa_auth
        self.azplan_usage_update = args.azplan_usage_update
        # 파일로 환경변수 세팅
        if args.file:
            env_path = os.path.join(os.path.dirname(sys.modules['__main__'].__file__), '.env')
            if os.path.exists(env_path):
                os.environ.update(get_variables_from_dotenv(env_path))
        self.stage = os.environ['STAGE']
        # Database 환경변수
        if args.database_password:
            self.database_password = args.database_password
        else:
            self.database_password = os.environ['DATABASE_PASSWORD']
        self.database_type = os.environ['DATABASE_TYPE']
        self.database_host = os.environ['DATABASE_HOST']
        self.database_port = os.environ['DATABASE_PORT']
        self.database_user = os.environ['DATABASE_USER']
        self.database_name = os.environ['DATABASE_NAME']
        self.commit = True if self.stage == 'prod' else False
        # Partercenter APP AUTH
        self.appid = os.environ['APPID']
        if args.app_secret:
            self.secret = args.app_secret
        else:
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
        if self.mfa_auth:
            self.__get_token(
                resource="https://api.partnercenter.microsoft.com",
                grant_type="refresh_token",
                refresh_token=self.mfa_auth,
                scope="openid"
            )
        else:
            self.__get_token()

    def __get_token(self, 
        resource: str = 'https://graph.windows.net',
        grant_type: str = "client_credentials",
        refresh_token: str = None,
        scope: str = None) -> None:
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
            'grant_type': grant_type
        }

        if refresh_token is not None:
            data["refresh_token"] = refresh_token
        if scope is not None:
            data["scope"] = scope

        r = requests.post(url=url, data=data, headers=headers)
        r.raise_for_status()
        # TODO: 에러처리
        self.__access_token = r.json()['access_token']


AzurePartnerCenterEnv.instance()