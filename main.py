from datetime import datetime

from src.env import AzurePartnerCenterEnv
from src.direct_crawler_main import daily_usage_crawler, invoice_crawler, azure_csp_price_crawler, \
    daily_usage_update_crawler, auth_user_app_mfa, azplan_usage_crawler
from src.logger.teams_msg import send_teams_msg

if __name__ == '__main__':
    # 환경변수 설정
    env = AzurePartnerCenterEnv.instance()

    # 일별 사용량 수집
    if env.daily_usage:
        daily_usage_crawler()
        send_teams_msg('일별 사용량 수집 완료')

    if env.daily_usage_update:
        daily_usage_update_crawler(period=env.update_period)
        send_teams_msg(f'일별 사용량 업데이트 완료 - 기간: {env.update_period}')
    
    if env.azplan_usage_update == "current" or env.azplan_usage_update == "previous":
        azplan_usage_crawler(period=env.azplan_usage_update)
        send_teams_msg('azplan 사용량 수집 완료')

    # 월별 인보이스 수집
    if env.monthly_invoice:
        invoice_crawler(t_date=datetime.now())
        send_teams_msg('월별 인보이스 수집 완료')

    # 월별 Azure 서비스 가격 수집
    if env.price_update:
        azure_csp_price_crawler()
        send_teams_msg('Azure 서비스 가격 수집 완료')
    
    if env.mfa_auth_issue:
        auth_user_app_mfa()
        pass
