"""
Microsoft Partner Center API호출을 통한 정보 수집 Controller

1. 고객리스트, 구독리스트
2. 사용량 * 가격 계산
3. 인보이스, 사용량
"""
from datetime import datetime, timedelta

from src.logger.logger import LOGGER
from src.ms_pc_request import AzureResourceSearch
from src.env import AzurePartnerCenterEnv

pc_request = AzureResourceSearch()
exclude_tenant_list = ['bb43721a-680c-4c9f-b234-72c8fe6c8e3c', 'fd17bb21-df5e-4169-83c4-f9d0ac42bfcb',
                       'cfcd9b87-7c5a-4042-9129-abee6253febe', '2b6cc091-4358-40b5-82bf-7edb22548e6d',
                       '8968d423-d612-41c7-88d5-ad47ef1bc18e', '3d30527c-c921-42f9-b7c5-2402e4ccd8c5',
                       '5586c79e-3391-4c24-a38e-cb9382226fed', 'eb22e9f6-90db-475e-8ebf-debcd9702343',
                       '6ae6c023-e4d1-4d82-ad4e-88399ea154b4', 'c7f02969-fbda-4e1c-8406-f26a787118e6',
                       'f1e38691-dce2-4030-b8c2-a4e3ed7ea68c', '00a9d622-d274-44b5-a698-ec207bb63bbb',
                       'fa43eb3b-9127-4e78-a202-69b5121b7198', 'eeb68620-9521-4cb6-9b8d-12c96314d68c']


# 모든 고객(테넌트) 정보를 받아옴
def get_all_tenant_info() -> dict:
    tenant_list = pc_request.customer_list()
    items = tenant_list['items'].copy()
    for item in items:
        if item['id'] in exclude_tenant_list:
            tenant_list['items'].remove(item)
    return tenant_list


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
            if subscription_info['offerId'] == 'MS-AZR-0146P' or subscription_info['offerId'] == 'MS-AZR-0145P':
                if tenant in result:
                    result[tenant].append(subscription_info)
                else:
                    result[tenant] = [subscription_info]
    LOGGER.debug(f'result : {result}')
    return result


def get_all_azure_software_info(tenants: list) -> dict:
    """

        :param tenants: ['tenantid1', 'tenantid2' ...]
        :return: {'tenantid1': [{'id': 'tenantid1', ...}...]...}
        """
    LOGGER.debug(f'param : tenants = {tenants}')
    result = {}
    param = {'entitlementType': 'software',
             'showExpiry': True}
    for tenant in tenants:
        result[tenant] = pc_request.customer_entitlements(tenant, param=param)[
            'items']
    LOGGER.debug(f'result : {result}')
    return result


def get_all_azure_reserved_info(tenants: list) -> dict:
    """

        :param tenants: ['tenantid1', 'tenantid2' ...]
        :return: {'tenantid1': [{'id': 'tenantid1', ...}...]...}
        """
    LOGGER.debug(f'param : tenants = {tenants}')
    result = {}
    param = {'entitlementType': 'reservedInstance',
             'showExpiry': True}
    for tenant in tenants:
        result[tenant] = pc_request.customer_entitlements(tenant, param=param)[
            'items']
    LOGGER.debug(f'result : {result}')
    return result


# tenant, subscription id를 입력으로 특정일에 대한 사용량을 받아옴. TODO: size control 필요
def get_azure_daily_usage(tenant: str, subscription: str, t_date: datetime, params=None) -> list:
    """

    :param tenant: str
    :param subscription: str
    :param t_date: datetime
    :param params: {
            'granularity': 'daily',
            'show_details': 'true',
            'size': 1000
        }
    :return:
    """
    if params is None:
        params = {
            'granularity': 'daily',
            'show_details': 'true',
            'size': 1000
        }
    convert_date_fmt = '%Y-%m-%dT00:00:00Z'
    target_date = {'start_time': t_date.strftime(convert_date_fmt),
                   'end_time': (t_date + timedelta(days=1)).strftime(convert_date_fmt)}
    params.update(target_date)
    return pc_request.azure_subscription_daily_usage(tenant=tenant,
                                                     subscription=subscription,
                                                     param=params)['items']


def azure_plan_unbilled_usage_raw(period: str = "current", continuationToken: str = None, max_size=2000) -> list:
    req_params = {
        "provider": "onetime",
        "invoicelineitemtype": "usagelineitems",
        "currencycode": "KRW",
        "period": period,
        "size": max_size
    }
    invoice_id = "unbilled"
    if period not in ["current", "previous"]:
        invoice_id = period
        req_params.pop("period", None)
    headers = {}
    if continuationToken is not None:
        req_params["seekOperation"] = "Next"
        headers["MS-ContinuationToken"] = continuationToken

    result = pc_request.azure_plan_unbilled_usage_raw(req_params, headers, invoice_id=invoice_id)
    req_params["tenantId"] = AzurePartnerCenterEnv.instance().tenant
    if continuationToken is not None:
        req_params["MS-ContinuationToken"] = continuationToken
    return (result, req_params)

# 년-월 입력으로 해당 인보이스 받아옴
def search_invoice(invoice_id: str = None, t_date: datetime = None):
    all_invoice_list = pc_request.invoice_list()['items']
    if (t_date is None) and (invoice_id is None):
        return all_invoice_list

    if invoice_id is not None:
        t_invoice_list = []
        for invoice in all_invoice_list:
            if invoice['id'] == invoice_id:
                t_invoice_list.append(invoice)
        all_invoice_list = t_invoice_list

    if t_date is not None:
        t_invoice_list = []
        for invoice in all_invoice_list:
            _invoice_date = datetime.strptime(
                invoice['invoiceDate'][0:10], '%Y-%m-%d')  # 2021-01-02T00:00:00Z
            if _invoice_date.year == t_date.year and _invoice_date.month == t_date.month:
                t_invoice_list.append(invoice)
        all_invoice_list = t_invoice_list

    return all_invoice_list


# 인보이스ID 입력으로 인보이스 자세히(모든 사용내역) 받아옴
def get_invoice_detail(invoice_id: str, provider: str = 'azure'):
    # provider는 ['office', 'azure', 'onetime']만 받음.
    provider_list = ['office', 'azure', 'onetime']
    if provider not in provider_list:
        LOGGER.error(
            f'잘못된 Provider 입력 : {provider} | 허용 Provider : {provider_list}')
        LOGGER.exception(f'잘못된 Provider 입력 : {provider}')
        raise ValueError

    param = {'provider': provider,
             'invoicelineitemtype': 'billinglineitems',
             'currencycode': 'kwd',
             'size': 2000}
    items = pc_request.invoice_billing_line_items(
        invoice=invoice_id, param=param)
    # TODO: totalCount가 2000 이상일경우 seekOperation=Next 을 param으로 추가호출
    # TODO: 2000이상인 케이스를 TEST할 수 없어서, 추후 업데이트 ( 2000이상일경우 raise)
    if int(items['totalCount']) > 2000:
        LOGGER.exception(f'Invoice Usage Items가 2000개를 넘음. Response : {items}')
        raise
    if int(items['totalCount']) != len(items['items']):
        LOGGER.exception(f'Invoice Usage Items가 2000개를 넘음. Response : {items}')
        raise
    return items


# MS Product 가격 업데이트    filter필요
def get_ms_product_price():
    pass


# Azure 가격 업데이트    리전 필요
def get_azure_resource_price(region='KR', currency='KRW', is_shared=False):
    if is_shared:
        rates = pc_request.ratecards_shared(
            param={'currency': currency, 'region': region})
    else:
        rates = pc_request.ratecards(
            param={'currency': currency, 'region': region})
    LOGGER.debug(f'Meter len : {len(rates["meters"])}')
    return rates
