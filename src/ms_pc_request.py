"""
Azure Partner Center API 요청
"""
import codecs
import json
from datetime import datetime
from json.decoder import JSONDecodeError

import requests

from src.env import AzurePartnerCenterEnv
from src.logger.logger import LOGGER

'''
1. 고객 테넌트에 대한 모든 구독정보 가져오기
2. 구독별 Daily 사용량
3. 인보이스
4. 제품가격 + 환율
'''


class AzureResourceSearch:
    PARTNER_API_BASEURL = 'https://api.partnercenter.microsoft.com'

    def __init__(self):
        self.env = AzurePartnerCenterEnv.instance()

    def usage_summary(self):
        req = self.__partner_center_api_request(endpoint='/v1/usagesummary')
        LOGGER.debug(req)
        return req

    def customer_list(self, param={}):
        """
        고객 리스트
        :param param: {'size': int,
                       'filter': '{"Field":"IndirectCloudSolutionProvider","Value":"true","Operator":"starts_With"}'}
        :return: {'totalCount':2,'items':[{'id':'cd3bdda9-bc0e-4fdc-b858-10b850d7043b','companyProfile':{'tenantId':'cd3bdda9-bc0e-4fdc-b858-10b850d7043b','domain':'previewcloudmtcustomer.onmicrosoft.com','companyName':'CloudMatePreview','links':{'self':{'uri':'/customers/cd3bdda9-bc0e-4fdc-b858-10b850d7043b/profiles/company','method':'GET','headers':[]}},'attributes':{'objectType':'CustomerCompanyProfile'}},'relationshipToPartner':'reseller','links':{'self':{'uri':'/customers/cd3bdda9-bc0e-4fdc-b858-10b850d7043b','method':'GET','headers':[]}},'attributes':{'objectType':'Customer'}},{'id':'9478e9f4-fb30-4450-8f05-544ed27c16c4','companyProfile':{'tenantId':'9478e9f4-fb30-4450-8f05-544ed27c16c4','domain':'previewcloudmtcustomer2.onmicrosoft.com','companyName':'CloudMatePreview_2','links':{'self':{'uri':'/customers/9478e9f4-fb30-4450-8f05-544ed27c16c4/profiles/company','method':'GET','headers':[]}},'attributes':{'objectType':'CustomerCompanyProfile'}},'relationshipToPartner':'reseller','links':{'self':{'uri':'/customers/9478e9f4-fb30-4450-8f05-544ed27c16c4','method':'GET','headers':[]}},'attributes':{'objectType':'Customer'}}],'links':{'self':{'uri':'/customers','method':'GET','headers':[]}},'attributes':{'objectType':'Collection'}}
        """
        req = self.__partner_center_api_request(endpoint='/v1/customers', params=param)
        LOGGER.debug(req)
        return req

    def customer_subscription_info(self, tenant: str) -> dict:
        """
        고객 테넌트에 대한 모든 구독정보 가져오기
        :param tenant: str
        :return: dict
        {'totalCount': 2, 'items': [{'id': '3194B176-A623-40A1-9A89-F96825DF3CE8', 'offerId': 'MS-AZR-0145P', 'entitlementId': '3194B176-A623-40A1-9A89-F96825DF3CE8', 'offerName': 'Microsoft Azure', 'friendlyName': 'TEST_CSP_BASIC', 'quantity': 1, 'unitType': 'Usage-based', 'hasPurchasableAddons': False, 'creationDate': '2020-12-21T08:35:05.29Z', 'effectiveStartDate': '2020-12-21T00:00:00Z', 'commitmentEndDate': '9999-12-01T00:00:00Z', 'status': 'active', 'autoRenewEnabled': False, 'isTrial': False, 'billingType': 'usage', 'billingCycle': 'monthly', 'actions': ['Cancel', 'Edit'], 'termDuration': 'P1Y', 'isMicrosoftProduct': True, 'attentionNeeded': False, 'actionTaken': False, 'contractType': 'subscription', 'links': {'offer': {'uri': '/offers/MS-AZR-0145P?country=KR', 'method': 'GET', 'headers': []}, 'product': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/products/9DEA7946-EC2C-441E-9FFD-E3B275F7E838', 'method': 'GET', 'headers': []}, 'sku': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/products/9DEA7946-EC2C-441E-9FFD-E3B275F7E838/skus/MS-AZR-0145P', 'method': 'GET', 'headers': []}, 'availability': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/products/9DEA7946-EC2C-441E-9FFD-E3B275F7E838/skus/MS-AZR-0145P/availabilities/MS-AZR-0145P', 'method': 'GET', 'headers': []}, 'self': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/subscriptions/3194B176-A623-40A1-9A89-F96825DF3CE8', 'method': 'GET', 'headers': []}}, 'orderId': 'C59F98C7-95E3-493D-85B4-43C52BD38CD8', 'attributes': {'etag': 'eyJpZCI6IjMxOTRiMTc2LWE2MjMtNDBhMS05YTg5LWY5NjgyNWRmM2NlOCIsInZlcnNpb24iOjN9', 'objectType': 'Subscription'}}, {'id': '9EBB3D71-C7F7-4152-8337-BDBD743DC17B', 'offerId': 'MS-AZR-0145P', 'entitlementId': '9EBB3D71-C7F7-4152-8337-BDBD743DC17B', 'offerName': 'Microsoft Azure', 'friendlyName': 'TEST_CSP_DELETE_파트너RBAC', 'quantity': 1, 'unitType': 'Usage-based', 'hasPurchasableAddons': False, 'creationDate': '2020-12-21T07:03:21.593Z', 'effectiveStartDate': '2020-12-21T00:00:00Z', 'commitmentEndDate': '9999-12-01T00:00:00Z', 'status': 'active', 'autoRenewEnabled': False, 'isTrial': False, 'billingType': 'usage', 'billingCycle': 'monthly', 'actions': ['Cancel', 'Edit'], 'termDuration': 'P1Y', 'isMicrosoftProduct': True, 'attentionNeeded': False, 'actionTaken': False, 'contractType': 'subscription', 'links': {'offer': {'uri': '/offers/MS-AZR-0145P?country=KR', 'method': 'GET', 'headers': []}, 'product': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/products/9DEA7946-EC2C-441E-9FFD-E3B275F7E838', 'method': 'GET', 'headers': []}, 'sku': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/products/9DEA7946-EC2C-441E-9FFD-E3B275F7E838/skus/MS-AZR-0145P', 'method': 'GET', 'headers': []}, 'availability': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/products/9DEA7946-EC2C-441E-9FFD-E3B275F7E838/skus/MS-AZR-0145P/availabilities/MS-AZR-0145P', 'method': 'GET', 'headers': []}, 'self': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/subscriptions/9EBB3D71-C7F7-4152-8337-BDBD743DC17B', 'method': 'GET', 'headers': []}}, 'orderId': '5019781B-8BBA-48B4-8CA9-6D7E7C1F0DE3', 'attributes': {'etag': 'eyJpZCI6IjllYmIzZDcxLWM3ZjctNDE1Mi04MzM3LWJkYmQ3NDNkYzE3YiIsInZlcnNpb24iOjR9', 'objectType': 'Subscription'}}], 'links': {'self': {'uri': '/customers/4d879a8c-f9c2-4f15-9452-6bdf268e7060/subscriptions', 'method': 'GET', 'headers': []}}, 'attributes': {'objectType': 'Collection'}}
        """
        req = self.__partner_center_api_request(endpoint=f'/v1/customers/{tenant}/subscriptions')
        LOGGER.debug(req)
        return req

    def customer_entitlements_info(self, tenant, param={}) -> dict:
        """

        :param tenant:
        :param param:
        :return:
        """
        req = self.__partner_center_api_request(endpoint=f'/v1/customers/{tenant}/entitlements',
                                                params=param)
        LOGGER.debug(req)
        return req

    def azure_subscription_daily_usage(self, tenant: str, subscription: str, param: dict) -> dict:
        """
        구독별 Daily 사용량
        요청실패시 http error가 나옴. TODO: 에러 처리
        :param tenant:
        :param subscription:
        :param param: {'start_time': '2020-12-01T00:00:00-08:00', 'end_time': '2020-12-28T00:00:00-08:00'}
        :return:
        """
        LOGGER.debug(f'tenant : {tenant}, subscription : {subscription}, param : {param}')
        req = self.__partner_center_api_request(
            endpoint=f'/v1/customers/{tenant}/subscriptions/{subscription}/utilizations/azure',
            params=param)
        LOGGER.debug(req)
        return req

    def invoice_list(self):
        """
        인보이스 리스트
        :return: {'totalCount': 1, 'items': [{'id': 'D0500033K3', 'invoiceDate': '2021-01-02T00:00:00Z', 'billingPeriodStartDate': '2020-12-01T00:00:00Z', 'billingPeriodEndDate': '2020-12-31T00:00:00Z', 'totalCharges': 23.0, 'paidAmount': 0.0, 'currencyCode': 'KRW', 'currencySymbol': '₩', 'pdfDownloadLink': '/invoices/D0500033K3/documents/statement', 'invoiceDetails': [{'invoiceLineItemType': 'billing_line_items', 'billingProvider': 'azure', 'links': {'self': {'uri': '/invoices/Recurring-D0500033K3/lineitems/Azure/BillingLineItems', 'method': 'GET', 'headers': []}}, 'attributes': {'objectType': 'InvoiceDetail'}}, {'invoiceLineItemType': 'usage_line_items', 'billingProvider': 'azure', 'links': {'self': {'uri': '/invoices/Recurring-D0500033K3/lineitems/Azure/UsageLineItems', 'method': 'GET', 'headers': []}}, 'attributes': {'objectType': 'InvoiceDetail'}}], 'documentType': 'invoice', 'state': '', 'invoiceType': 'Recurring', 'links': {'self': {'uri': '/invoices/Recurring-D0500033K3', 'method': 'GET', 'headers': []}}, 'attributes': {'objectType': 'Invoice'}}], 'links': {'self': {'uri': '/invoices', 'method': 'GET', 'headers': []}}, 'attributes': {'objectType': 'Collection'}}
        """
        req = self.__partner_center_api_request(endpoint='/v1/invoices')
        LOGGER.debug(req)
        return req

    def invoice_from_id(self, invoice_id):
        req = self.__partner_center_api_request(endpoint=f'/v1/invoices/{invoice_id}')
        LOGGER.debug(req)
        return req

    def invoice_billing_line_items(self, invoice, param={}):
        """

        :param invoice:
        :param param:
        :return: {"totalCount":18,"items":[{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"계층화된 블록 BLOB","resourceGuid":"6CEE1889-F299-47A7-9EC4-5CCFC733427B","resourceName":"핫 GRS 쓰기 작업","region":"한국 중부","consumedQuantity":0.0001,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"계층화된 블록 BLOB","resourceGuid":"326D7102-7768-4CE9-B958-18376E3D40CC","resourceName":"기타 모든 작업","region":"한국 중부","consumedQuantity":0.0022,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"테이블","resourceGuid":"12DA282F-7E96-49E2-983A-9A65DA2A4866","resourceName":"읽기 작업","region":"","consumedQuantity":0.001,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-23T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"프리미엄 SSD MANAGED DISKS","resourceGuid":"81891594-6CD0-4F2D-9FA6-63150AE9E4AF","resourceName":"P4 디스크","region":"한국 중부","consumedQuantity":0.001344,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"1/월","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-23T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"VIRTUAL NETWORK","serviceType":"IP 주소","resourceGuid":"F114CB19-EA64-40B5-BCD7-AEE474B62853","resourceName":"동적 공용 IP","region":"","consumedQuantity":0.36388889,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"1시간","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"계층화된 블록 BLOB","resourceGuid":"6CEE1889-F299-47A7-9EC4-5CCFC733427B","resourceName":"핫 GRS 쓰기 작업","region":"한국 중부","consumedQuantity":0.0001,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"계층화된 블록 BLOB","resourceGuid":"326D7102-7768-4CE9-B958-18376E3D40CC","resourceName":"기타 모든 작업","region":"한국 중부","consumedQuantity":0.0021,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"계층화된 블록 BLOB","resourceGuid":"1572FC9E-30D9-406F-97E1-57F100A2AA32","resourceName":"GRS 컨테이너 나열 및 생성 작업","region":"한국 중부","consumedQuantity":0.0006,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"BANDWIDTH","resourceGuid":"62D7DD8D-C715-4D02-894E-9AEF6E69A862","resourceName":"지역에서 복제 V2 데이터 전송","region":"한국 중부","consumedQuantity":0.000002,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"1GB","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"테이블","resourceGuid":"AD22FAC8-9DA5-4577-8683-56AE94D39E42","resourceName":"GRS 쓰기 작업","region":"","consumedQuantity":0.0011,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"테이블","resourceGuid":"BB20DFDA-6D18-4BB1-B7FC-86A05D9CFB8D","resourceName":"GRS 배치 쓰기 작업","region":"","consumedQuantity":0.0001,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"테이블","resourceGuid":"12DA282F-7E96-49E2-983A-9A65DA2A4866","resourceName":"읽기 작업","region":"","consumedQuantity":0.0011,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"테이블","resourceGuid":"BB20DFDA-6D18-4BB1-B7FC-86A05D9CFB8D","resourceName":"GRS 배치 쓰기 작업","region":"","consumedQuantity":0.0002,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"테이블","resourceGuid":"AD22FAC8-9DA5-4577-8683-56AE94D39E42","resourceName":"GRS 쓰기 작업","region":"","consumedQuantity":0.001,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-23T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"VIRTUAL MACHINES","serviceType":"BS 시리즈","resourceGuid":"D4504800-9AA8-472C-9C45-353DE236775E","resourceName":"B1S","region":"한국 중부","consumedQuantity":1.083355,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"1시간","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"BANDWIDTH","serviceType":"지역 간","resourceGuid":"941A7A91-F0B4-4C70-BE0F-D87EA6FF09EE","resourceName":"데이터 송신","region":"아시아","consumedQuantity":0.000007,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"1GB","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8640704073","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"9EBB3D71-C7F7-4152-8337-BDBD743DC17B","subscriptionName":"TEST_CSP_DELETE_파트너RBAC","subscriptionDescription":"TEST_CSP_DELETE_파트너RBAC","billingCycleType":"MONTHLY","orderId":"568860952529548667","serviceName":"STORAGE","serviceType":"계층화된 블록 BLOB","resourceGuid":"1572FC9E-30D9-406F-97E1-57F100A2AA32","resourceName":"GRS 컨테이너 나열 및 생성 작업","region":"한국 중부","consumedQuantity":0.0007,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"10K","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}},{"customerBillableAccount":"8645826697","usageDate":"2020-12-21T00:00:00Z","invoiceLineItemType":"usage_line_items","partnerId":"6D5AC8EE-3862-4452-93E7-A836C2D9742B","partnerName":"(주)클라우드메이트","partnerBillableAccountId":"8558017093","customerId":"4D879A8C-F9C2-4F15-9452-6BDF268E7060","domainName":"cloudmttest.onmicrosoft.com","customerCompanyName":"CLOOUDMATE TEST","mpnId":5308239,"tier2MpnId":-1,"invoiceNumber":"D0500033K3","subscriptionId":"3194B176-A623-40A1-9A89-F96825DF3CE8","subscriptionName":"TEST_CSP_BASIC","subscriptionDescription":"TEST_CSP_BASIC","billingCycleType":"MONTHLY","orderId":"568860952594317789","serviceName":"STORAGE","serviceType":"BANDWIDTH","resourceGuid":"62D7DD8D-C715-4D02-894E-9AEF6E69A862","resourceName":"지역에서 복제 V2 데이터 전송","region":"한국 중부","consumedQuantity":0.000002,"chargeStartDate":"2020-12-21T00:00:00Z","chargeEndDate":"2020-12-31T00:00:00Z","unit":"1GB","billingProvider":"azure","attributes":{"objectType":"DailyUsageLineItem"}}],"links":{"self":{"uri":"/invoices/D0500033K3/lineitems?provider=Azure&invoicelineitemtype=UsageLineItems&size=2000&offset=0","method":"GET","headers":[]}},"attributes":{"objectType":"Collection"}}
        """
        req = self.__partner_center_api_request(endpoint=f'/v1/invoices/{invoice}/lineitems',
                                                params=param)
        LOGGER.debug(req)
        return req

    def customer_entitlements(self, tenant, param={}):
        """

        :param tenant:
        :param param: {'entitlementType': 'reservedInstance' or 'software',
                       'showExpiry': True}
        :return:
        """
        req = self.__partner_center_api_request(endpoint=f'/v1/customers/{tenant}/entitlements',
                                                params=param)
        LOGGER.debug(req)
        return req

    def ratecards(self, param={}):
        """
        Azure 요금표 - 파트너가격
        (대용량 JSON)
        https://docs.microsoft.com/ko-kr/partner-center/develop/azure-rate-card-resources#azuremeter
        :param param: {'currency': currency, 'region': region}
        :return:
        """
        req = self.__partner_center_api_request(endpoint='/v1/ratecards/azure',
                                                params=param,
                                                timeout=60)
        return req

    def ratecards_shared(self, param={}):
        """
        Azure 요금표 - 공유 (소비자가격)
        (대용량 JSON)
        https://docs.microsoft.com/ko-kr/partner-center/develop/azure-rate-card-resources#azuremeter
        :param param: {'currency': currency, 'region': region}
        :return:
        """
        req = self.__partner_center_api_request(endpoint='/v1/ratecards/azure-shared',
                                                params=param,
                                                timeout=60)
        return req

    def microsoft_price_list(self, param={}):
        """
        Test중....
        :param param:
        :return:
        """
        req = self.__partner_center_api_request(endpoint='/v1.0/sales/pricesheets(Market=\'kr\',PricesheetView=\'azure_consumption\')', params=param)

    def license_usage_data(self, param={}) -> dict:
        """
        https://docs.microsoft.com/en-us/partner-center/develop/get-licenses-usage-information
        :param tenant:
        :param param: {'processedDateTime': '2018-10-14T00:00:00', 'filter':'customerTenantId eq tenant-id'}
        :return:
            {
            "Value": [
                {
                "processedDateTime": "2018-10-14T00:00:00",
                "workloadCode": "SPO",
                "workloadName": "SharePoint",
                "serviceCode": "o365",
                "serviceName": "Microsoft Office 365",
                "channel": "reseller",
                "customerTenantId": "0112A436-B14E-4888-967B-CA4BB2CF1234",
                "customerName": "TEST COMPANY",
                "productId": "6FD2C87F-B296-42F0-B197-1E91E994B900",
                "productName": "OFFICE 365 ENTERPRISE E3",
                "licenseActive": 0,
                "licensesQualified": 1
                }
            }
        """
        req = self.__partner_center_api_request(
            endpoint=f'/v1/analytics/commercial/usage/license/', params=param)
        LOGGER.debug(req)
        return req

    def test_request(self, endpoint):
        req = self.__partner_center_api_request(endpoint=endpoint)
        return req

    def __partner_center_api_request(self, endpoint: str, params={}, headers={}, timeout=30, retry=5) -> dict:
        LOGGER.debug(f'Partner Center API Request - endpoint :{endpoint}')
        url = self.PARTNER_API_BASEURL + endpoint
        headers.update({
            'Authorization': 'Bearer ' + self.env.access_token,
            'Content-Type': 'application/json',
            'charset': 'utf-8'
        })
        for i in range(retry):
            try:
                r = requests.get(url, headers=headers, params=params, timeout=timeout)
                LOGGER.debug(f'Partner Center API Request - response {r.status_code}')
                r.raise_for_status()
                # TODO: 에러처리
                # TODO: 재시도 로직 추가
                try:
                    r_json = r.json()
                    return r_json
                except JSONDecodeError:
                    r_json = json.loads(codecs.decode(r.text.encode(), encoding='utf-8-sig'))
                    return r_json
                except Exception:
                    LOGGER.debug(r.text)
                    if i == (retry - 1):
                        raise
            except:
                LOGGER.warning("Try again")
                LOGGER.exception('exception message')
                continue
            break

    

