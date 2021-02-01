from src.env import AzurePartnerCenterEnv
from src.direct_crawler_main import azure_csp_price_crawler, monthly_invoice_crawler, daily_usage_cralwer
from src.ms_pc_controller import *
from src.ms_pc_request import AzureResourceSearch

if __name__ == '__main__':
    # 환경변수 설정
    # .env 혹은 환경변수 설정
    daily_usage_cralwer()