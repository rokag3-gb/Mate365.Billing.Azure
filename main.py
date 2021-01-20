from src.env import AzurePartnerCenterEnv
from src.direct_crawler_main import crawler_main, azure_csp_price_crawler
from src.ms_pc_controller import *
from src.ms_pc_request import AzureResourceSearch

if __name__ == '__main__':
    # 환경변수 설정
    # .env 혹은 환경변수 설정
    # AzurePartnerCenterEnv.instance()
    # 크롤링할 Tenant 수집
    # TODO: CM database에서 tenant 리스트 수집

    #
    # tenant_list = ['9478e9f4-fb30-4450-8f05-544ed27c16c4']
    # print(get_all_subscription_info(tenant_list))
    # print(get_all_tenant_info())
    # subscriptions = get_all_subscription_info(get_all_tenant_id_list())
    # for t in subscriptions:
    #     print(filter_azure_subscription(subscriptions[t]))
    crawler_main(get_all_tenant_id_list())
    # azure_csp_price_crawler()