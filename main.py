from datetime import datetime

from src.env import AzurePartnerCenterEnv
from src.direct_crawler_main import daily_usage_cralwer, invoice_crawler, azure_csp_price_crawler


if __name__ == '__main__':
    # 환경변수 설정
    env = AzurePartnerCenterEnv.instance()

    # 일별 사용량 수집
    if env.daily_usage:
        daily_usage_cralwer()
        # TODO: 완료 메시지

    # 월별 인보이스 수집
    if env.monthly_invoice:
        invoice_crawler(t_date=datetime.now())
        # TODO: 완료 메시지

    # 월별 Azure 서비스 가격 수집
    if env.price_update:
        azure_csp_price_crawler()
        # TODO: 완료 메시지
