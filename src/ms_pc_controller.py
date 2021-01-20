"""
Microsoft Partner Center API호출을 통한 정보 수집 Controller

1. 고객리스트, 구독리스트
2. 사용량 * 가격 계산
3. 인보이스, 사용량
"""
from datetime import datetime, timedelta

from src.logger.logger import LOGGER
from src.ms_pc_request import AzureResourceSearch

pc_request = AzureResourceSearch()


# 모든 고객(테넌트) 정보를 받아옴
def get_all_tenant_info() -> dict:
    return pc_request.customer_list()


def get_all_tenant_id_list() -> list:
    result = []
    all_tenant_info = get_all_tenant_info()
    for tenant in all_tenant_info['items']:
        result.append(tenant['id'])
    LOGGER.debug(f'result : {result}')
    return result


# Tenant list를 입력으로 모든 subscription 정보들을 받아옴.
def get_all_subscription_info(tenants: list) -> dict:
    """

    :param tenants: ['tenantid1', 'tenantid2' ...]
    :return: {'tenantid1': [{'id': 'tenantid1', ...}...]...}
    """
    LOGGER.debug(f'param : tenants = {tenants}')
    result = {}
    for tenant in tenants:
        result[tenant] = pc_request.customer_subscription_info(tenant)['items']
    LOGGER.debug(f'result : {result}')
    return result


# get_all_subscription_info 함수 결과 입력으로 Azure subscription 정보들만 뽑음.
def filter_azure_subscription(subscription_info_list: dict) -> dict:
    """

    :param subscription_info_list: {'tenantid1': [{'id': 'tenantid1', ...}...]...
    :return: {'tenantid1': [{'id': 'tenantid1', ...}...]...
    """
    LOGGER.debug(f'param : subscription_info_list = {subscription_info_list}')
    result = {}
    for tenant in subscription_info_list:
        for subscription_info in subscription_info_list[tenant]:
            if subscription_info['offerId'] == 'MS-AZR-0146P':
                if tenant in result:
                    result[tenant].append(subscription_info)
                else:
                    result[tenant] = [subscription_info]
    LOGGER.debug(f'result : {result}')
    return result


# tenant, subscription id를 입력으로 특정일에 대한 사용량을 받아옴. TODO: size control 필요
def get_azure_daily_usage(tenant: str, subscription: str, t_date: datetime, params=None) -> list:
    if params is None:
        params = {
            'granularity': 'daily',
            'show_details': 'true',
            'size': 1000
        }
    convert_date_fmt = '%Y-%m-%d'
    target_date = {'start_time': t_date.strftime(convert_date_fmt),
                   'end_time': (t_date + timedelta(days=1)).strftime(convert_date_fmt)}
    params.update(target_date)
    return pc_request.azure_subscription_daily_usage(tenant=tenant,
                                                     subscription=subscription,
                                                     param=params)['items']


# 년-월 입력으로 해당 인보이스 받아옴 (복수일경우 에러)
def get_invoice(t_date: datetime):
    pass


# 인보이스ID 입력으로 인보이스 요약 받아옴
def get_invoice_summary(invoiceid: str):
    pass


# 인보이스ID 입력으로 인보이스 자세히(모든 사용내역) 받아옴
def get_invoice_detail(invoiceid: str):
    # totalCount가 2000 이상일경우 seekOperation=Next 을 param으로 추가호출
    pass


# MS Product 가격 업데이트    filter필요
def get_ms_product_price():
    pass


# Azure 가격 업데이트    리전 필요
def get_azure_resource_price(region='kr', currency='KRW'):
    rates = pc_request.ratecards(param={'currency': currency, 'region': region})
    LOGGER.debug(f'Meter len : {len(rates["meters"])}')
    return rates
