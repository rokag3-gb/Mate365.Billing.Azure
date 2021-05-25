"""
1. Daily crawler : MS 제품군 사용현황, Azure 사용량 to DATABASE
2. Monthly crawler : 인보이스, 가격(Azure, Microsoft) to DATABASE

"""
from datetime import datetime
import cgi, webbrowser
from urllib.parse import parse_qs
import requests
from src.cm_controller import save_ratecard, save_azure_customer_subscription, save_azure_customer, \
    get_azure_utilization_user, remove_azure_utilization_user, save_azure_utilization_user, \
    save_azure_customer_software, save_azure_customer_ri, save_invoice, save_invoice_detail_azure, \
    save_invoice_detail_office, save_invoice_detail_onetime
from src.database.db_connection import DBConnect
from src.env import AzurePartnerCenterEnv
from src.logger.teams_msg import send_teams_msg
from src.ms_pc_controller import *
from http.server import BaseHTTPRequestHandler, HTTPServer


# def crawler_main(tenant_list: list, date: datetime = None):
#     daily_usage_cralwer(tenant_list)


def daily_usage_crawler(tenant_list: list = None, t_date: datetime = None, term=1):
    """

    :param tenant_list:
    :param t_date:
    :param term:
    :return:
    """
    LOGGER.info('Start Daily Crawler')
    # Customer 정보 업데이트. #######################################################
    tenent_info = get_all_tenant_info()
    save_azure_customer(tenent_info)
    # #############################################################################
    if tenant_list is None:
        tenant_list = []
        for tenant in tenent_info['items']:
            tenant_list.append(tenant['id'])
    LOGGER.debug(f'수집 Tenant 리스트 : {tenant_list}')
    LOGGER.info(f'수집 Tenant len : {len(tenant_list)}')
    # 모든 구독 사용현황 #############################################################
    subscriptions_info = get_all_subscription_info(tenant_list)
    # CM DATABASE 저장
    if len(subscriptions_info.keys()) > 0:
        save_azure_customer_subscription(subscriptions_info)
    LOGGER.info(f'Save azure_customer_subscription 완료')
    # #############################################################################

    # 모든 RI, Software (Entitlements) 수집 #########################################
    software_info_list = get_all_azure_software_info(tenant_list)
    ri_info_list = get_all_azure_reserved_info(tenant_list)
    # SOFTWARE, RI to DATABASE
    save_azure_customer_software(software_info_list)
    save_azure_customer_ri(ri_info_list)
    LOGGER.info(f'Save azure_customer_software, ri 완료')
    # ##############################################################################

    # 모든 Azure 구독 수집 & Azure 사용량 수집 #########################################
    azure_subscriptions = filter_azure_subscription(subscriptions_info)
    # 모든 구독에 대한 사용량 [(tenant, subscription, [usage list]) ...]
    azure_daily_usages = []
    # Default : 어제 사용량 요청
    if t_date is None:
        today = datetime.now()
        t_date = datetime(year=today.year,
                          month=today.month,
                          day=today.day) - timedelta(days=1)
    for t in azure_subscriptions:
        for s in azure_subscriptions[t]:
            azure_daily_usages.append((t, s['id'], get_azure_daily_usage(tenant=t,
                                                                         subscription=s['id'],
                                                                         t_date=t_date)))
    # CM DATABASE 저장
    # get daily usage from CM database & if exist : DELETE & insert
    for usage in azure_daily_usages:
        cm_daily_usage = get_azure_utilization_user(tenant=usage[0],
                                                    subscription=usage[1],
                                                    start_time=t_date,
                                                    offset=term)
        if cm_daily_usage:
            remove_azure_utilization_user(tenant=usage[0],
                                          subscription=usage[1],
                                          start_time=t_date,
                                          offset=term)
        save_azure_utilization_user(tenant=usage[0],
                                    subscription=usage[1],
                                    start_time=t_date,
                                    end_time=t_date + timedelta(days=term),
                                    daily_usage=usage[2])
    LOGGER.info(f'Save azure_utilization 완료')
    # ##################################################################################
    send_teams_msg('Daily Crawler Done.')


def daily_usage_update_crawler(tenant_list: list = None, t_date: datetime = None, period=1, term=1):
    """
    과거 데이터 가져올 수 있는 것만 크롤링
    :param tenant_list:
    :param t_date:
    :param term:
    :return:
    """
    # Customer 정보 업데이트. #######################################################
    tenent_info = get_all_tenant_info()
    # #############################################################################
    if tenant_list is None:
        tenant_list = []
        for tenant in tenent_info['items']:
            tenant_list.append(tenant['id'])
    LOGGER.debug(f'수집 Tenant 리스트 : {tenant_list}')
    # 모든 구독 사용현황 #############################################################
    subscriptions_info = get_all_subscription_info(tenant_list)
    # #############################################################################

    # 모든 Azure 구독 수집 & Azure 사용량 수집 #########################################
    azure_subscriptions = filter_azure_subscription(subscriptions_info)
    # Default : 어제 사용량 부터 요청
    if t_date is None:
        today = datetime.now()
        t_date = datetime(year=today.year,
                          month=today.month,
                          day=today.day) - timedelta(days=1)

    for i in range(period):
        LOGGER.info(f'Update - {t_date}')
        # 모든 구독에 대한 사용량 [(tenant, subscription, [usage list]) ...]
        azure_daily_usages = []
        for t in azure_subscriptions:
            for s in azure_subscriptions[t]:
                azure_daily_usages.append((t, s['id'], get_azure_daily_usage(tenant=t,
                                                                             subscription=s['id'],
                                                                             t_date=t_date)))
        # CM DATABASE 저장
        # get daily usage from CM database & if exist : DELETE & insert
        for usage in azure_daily_usages:
            cm_daily_usage = get_azure_utilization_user(tenant=usage[0],
                                                        subscription=usage[1],
                                                        start_time=t_date,
                                                        offset=term)
            if cm_daily_usage:
                remove_azure_utilization_user(tenant=usage[0],
                                              subscription=usage[1],
                                              start_time=t_date,
                                              offset=term)
            save_azure_utilization_user(tenant=usage[0],
                                        subscription=usage[1],
                                        start_time=t_date,
                                        end_time=t_date + timedelta(days=term),
                                        daily_usage=usage[2])

        t_date = t_date - timedelta(days=1)
        # ##################################################################################



def invoice_crawler(invoice_id: str = None, t_date: datetime = None):
    """

    :param invoice_id: 특정 invoice id에 대해서만 서치할때 사용
    :param t_date: 월단위로 search 
    :return: 
    """
    # INVOICE SEARCH ########################################################
    invoice_list = search_invoice(invoice_id=invoice_id, t_date=t_date)

    # invoice는 D계열(사용량, 라이선스기반) G계열(one-time) 두개가 나옴.
    # 없을경우 에러.
    if len(invoice_list) == 0:
        LOGGER.error(
            f'해당 인보이스가 없음. input param : invoice id : {invoice_id} t_date : {t_date}')
        LOGGER.exception(f'인보이스가 없음. 파트너센터 청구 유무 확인')
        raise

    # invoice record DB 저장
    save_invoice(invoice_list)
    # #######################################################################

    # INVOICE DETAIL ########################################################
    # azure, office, OneTime
    invoice_type = ['Recurring', 'OneTime']
    invoice_detail_type = ['azure', 'office', 'one_time']
    invoice_line_item_type = 'billing_line_items'
    for invoice in invoice_list:
        if invoice['invoiceType'] in invoice_type:
            for detail in invoice['invoiceDetails']:
                if detail['billingProvider'] in invoice_detail_type:
                    # azure
                    if detail['billingProvider'] == 'azure' and detail['invoiceLineItemType'] == invoice_line_item_type:
                        # SAVE DATABASE
                        save_invoice_detail_azure(invoice['id'], get_invoice_detail(invoice_id=invoice['id'],
                                                                                    provider='azure'))
                    # office
                    if detail['billingProvider'] == 'office' and detail['invoiceLineItemType'] == invoice_line_item_type:
                        # SAVE DATABASE
                        save_invoice_detail_office(invoice['id'], get_invoice_detail(invoice_id=invoice['id'],
                                                                                     provider='office'))
                    # # onetime
                    if detail['billingProvider'] == 'one_time' and detail['invoiceLineItemType'] == invoice_line_item_type:
                        # SAVE DATABASE
                        save_invoice_detail_onetime(invoice['id'], get_invoice_detail(invoice_id=invoice['id'],
                                                                                      provider='onetime'))
                else:
                    LOGGER.warning(
                        f'Invoice Detail Type이 {invoice_detail_type}에서 벗어남. Invoice 정보: {detail}')

        else:
            LOGGER.warning(
                f'Invoice Type이 {invoice_type}에서 벗어남. Invoice 정보: {invoice}')

    DBConnect.get_instance().commit()
    DBConnect.get_instance().close()
    # #######################################################################


def azure_csp_price_crawler():
    """
    월별 수집으로, azure 리소스 가격 수집 & CM DATABASE에 저장.
    :return:
    """
    rates = get_azure_resource_price()
    # Mock data
    # rates = {"locale":"en","currency":"KRW","isTaxIncluded":False,"meters":[{"id":"cd2d7ca5-2d4c-5f93-94d0-8cee0662c71c","name":"E20 v4","rates":{'0':59.269055,'51200.0000000000':56.879173,'512000.0000000000':54.489292},"tags":[],"category":"Virtual Machines","subcategory":"Ev4 Series","region":"AP Southeast","unit":"1 Hour","includedQuantity":0.0,"effectiveDate":"2020-05-01T00:00:00Z"},{"id":"702d55d5-e395-53b1-8fc8-665a45b35539","name":"E8-2s v4 Low Priority","rates":{"0":96.551202},"tags":[],"category":"Virtual Machines","subcategory":"Esv4 Series","region":"US East 2","unit":"1 Hour","includedQuantity":0.0,"effectiveDate":"2020-05-01T00:00:00Z"},{"id":"dee311f4-b516-4e64-9158-df610306a632","name":"S1","rates":{"0":97.60275},"tags":[],"category":"Azure App Service","subcategory":"Standard Plan","region":"NO East","unit":"1 Hour","includedQuantity":0.0,"effectiveDate":"2019-10-28T00:00:00Z"},{"id":"7baf71c7-13a4-469d-ba60-1dc5b675b90b","name":"Cool Read Additional IO","rates":{"0":4.17695},"tags":[],"category":"Storage","subcategory":"Azure Data Lake Storage Gen2 Hierarchical Namespace","region":"AE Central","unit":"10K","includedQuantity":0.0,"effectiveDate":"2020-02-01T00:00:00Z"},{"id":"705ac022-f360-4a96-9f32-a2a982d7b70e","name":"D14 v2/DS14 v2","rates":{"0":1858.37166},"tags":[],"category":"Virtual Machines","subcategory":"Dv2/DSv2 Series Windows","region":"US East 2","unit":"1 Hour","includedQuantity":0.0,"effectiveDate":"2018-01-01T00:00:00Z"},{"id":"6acfb317-2b22-48ca-b518-6161fe94e8f9","name":"Protocol Operations","rates":{"0":1.576759},"tags":[],"category":"Storage","subcategory":"Files","region":"NO East","unit":"10K","includedQuantity":0.0,"effectiveDate":"2019-10-28T00:00:00Z"},{"id":"b0808d59-e6ac-47fe-80d0-dc6e24c29dfe","name":"Read Operations","rates":{"0":1.433928},"tags":[],"category":"Storage","subcategory":"Standard Page Blob v2","region":"US West Central","unit":"10K","includedQuantity":0.0,"effectiveDate":"2018-01-01T00:00:00Z"},],"offerTerms":[{'name':'Overage discount','discount':0.15,'excludedMeterIds':['a0dec664-8fe2-4206-826f-ea1567c574eb','e3f4faa4-7ba7-45fe-be6b-899fcd462c7e','b9edcc5b-a429-4778-bc5a-82e7fa07fe55','dd75c4ed-be82-4c52-9e65-f63f0b544941','e11331a8-fd32-4e71-b60e-4de2a818c67a','378b8125-d8a5-4e09-99bc-c1462534ffb0','0f753205-b97f-4c77-8622-b76ba800c88b','4d713b98-dd51-47ad-a1c3-d0b7d4051d84','5d7db11a-54e9-404e-aaa8-509fac7c0638','26b3b0c1-5a98-4cbc-97e7-e49cd8cf3041','c82dbd27-c978-43a7-ad41-525a90d8962b','bb21066f-fe46-46d3-8006-b326b1663e52','6e5c1729-e829-4262-abb0-af3b4e306030','035e1988-343e-4671-8425-0b181c3cf0f0','fb528cdf-6fbb-4f53-bfac-b653423c0ef0','4b2fecfc-b110-4312-8f9d-807db1cb79ae','180c1a0a-b0a5-4de3-a032-f92925a4bf90','4ed70d2d-e2bb-4dcd-b6fa-42da71861a1c','e97e4b49-0961-41e6-a6e2-1836f486257a','a25c8206-da8f-48c6-bb1e-7c54f1b5f61b','754c7f93-e3fb-455b-8ba8-1069d56b4566','60b3ae9d-e77a-46b2-9cdf-92fa87407969','2a0c92c8-23a7-4dc9-a39c-c4a73a85b5da','5c33b840-166c-4ef2-a2d9-49fa9f9d456f','5f045ba8-1314-4b6c-adcb-58e180c5a7b5','089f79d8-0349-432c-96a6-8add90b8a40e','e2d2d63e-8741-499a-8989-f5f7ec5c3b3f','aca36c16-fe97-47a3-802a-574d450f2ce5','875898d3-3639-423c-82c1-38846281b7e8','370bd1dc-3d81-447e-9fb0-7f20809b21de','6dfb482b-23ea-487f-810c-e66360f025de','799827a0-dc0e-40b8-bb4e-ec6628d71371','4171c100-43d8-4582-bef4-93769845ead3','9769b669-1e76-45f8-99fa-a779c5598226','53cc0061-0fe2-4249-bf62-e1008c811f5c','900607f5-1f1d-4186-bdf6-5c947ccee42f','8ab720e4-7494-48fe-9369-8214361a6ae7','488751c2-868e-4975-867c-5083a608ad2e','57105f32-0637-432e-ab45-76aa1956acfb','b5c04ba8-af0e-4b4c-8ef3-c405c9e9c338','98babd23-6265-4b84-a3f0-4d18934b894f','b6862df1-0d78-4af2-9054-61d9f4388733','e531e1c0-09c9-4d83-b7d0-a2c6741faa22','25889e91-c740-42ac-bc52-6b8f73b98575','b9ee1dbb-5227-4abc-a884-6993e8d744d4','6f44ae85-a70e-44be-83ec-153a0bc23979','35e3f2e1-d7f2-4410-ba00-80ffd6aa4066','e22e4189-b955-473e-809b-466b82aa2296','dac14c31-c493-435c-a93d-6a63e5d5acf2','94d5413a-8ae1-48db-8ecd-32eba9742e95','d315af10-098d-4f9f-b840-f3ff5abea4fa','cc40eaba-713c-4614-b8e4-7bc79ea4b154','c9deceb6-bf79-4be0-b865-e50497409c91','70509cbf-282e-4b2f-a4b9-5e0c5fe5577f','071d7e1b-6708-4e48-ae89-d011b968df63','9ee47aa8-3dd6-40a2-8c6c-24205a413c7f','b243a898-b80d-4243-9b8e-6e3f5557a637','3a3a26df-aaa0-4f02-bd93-e3291acfc0e8','3b97c9f5-f5d5-4fd3-a421-b78fca32a656','1ebda3db-45b6-40b3-923a-02389937d3af','830e2e37-bbc7-4261-a484-1ccbaae4342d','c5228804-1de6-4bd4-a61c-501d9003acc8','bed570b0-e4b0-40fe-bdc3-0509d734ca3e','e8862232-6131-4dbe-bde4-e2ae383afc6f','5e2eede2-0b0d-40e1-97dc-28f497bb2f18','d47e0af7-1b90-4dc1-adf7-e5931119e963','847887de-68ce-4adc-8a33-7a3f4133312f','eef9c82a-e1c2-426c-badf-cd0a44c40947','a6ef2941-2393-4bb1-9eb4-cbf66fe9e6e9','016858f4-90a2-4afc-abc2-4f3105629a96','bdd569da-1f71-4dc2-8aa1-eb9f6bf0ba03','462cd632-ec6b-4663-b79f-39715f4e8b38','4e633ef0-4c61-4132-a50f-651a7dc03284','9297a3f2-b3fc-4e84-af19-a156eb84ed57','18ae79cd-dfce-48c9-897b-ebd3053c6058','d499fc22-1b34-4853-aea3-d1dc00c6c02f','a21ce912-a5b4-4f3b-9212-6f2bb512a94d','4db9e645-2f3f-4c74-b38b-d8977859d406','461da01b-1647-4a22-a759-ca80f6955b30','1d3bb602-0cde-4618-9fb0-f9d94805c2a6','a22bb342-ba9a-4529-a178-39a92ce770b6','c92a7ec3-d054-4c9d-98c1-ef708e97a201','56c69cb9-ba42-4c1a-bd67-3acb8ade44a6','465cce1c-dda5-4e4c-a896-b04f50e1ecc5','d31fc1c5-9ffe-4e96-8f3d-a52f1ec2a7d1','f5eed8fc-a5dc-411a-9250-f98c71699de7','fc9a8657-b1e7-4bc2-b79b-d006e7ade863','0ec88494-2022-4939-b809-0d914d954692','497fe0b6-fa3c-4e3d-a66b-836097244142','b39feb58-57bf-40f2-8193-f4fe9ac3dda3','792894ed-5b32-452e-ac70-0463d027d90b','3992dbde-d651-4365-b1b0-ceaa5d18c539','c001d6a9-c55e-4cd8-95df-1df744137ebd','d0ee13b3-4d2e-4ce7-8fca-4c29174038a5','722bda73-a8c8-4d04-b96b-541f0bb6c0c4','fe82dacb-c546-4797-86e8-6fc3fb5f1f19','b8ea6d4d-d009-47ea-ba62-fd7be2c511ff','dcba3e7c-3c67-4dea-9e84-29035b3a0ee4','48de6092-451c-4de6-8451-b5faac386c74','978969fe-ee5b-425c-a9fc-97feaf41e100','3709a480-8192-46e4-bf16-1f5a3288a139','4d9c0264-daa4-45af-88ee-ed917c8ca7f4','ac27e4d7-44b5-4fee-bc1a-78ac5b4abaf7','f5bbc71d-211c-4f94-b659-cbe20967b85a','085dc9ee-005d-4075-ac11-822ccde9e8f6','2018c3a8-ff13-41f8-b64d-9558c5206547','93954aa4-b55f-4b7b-844d-a119d6bf3c4e','99affa87-7ab5-45fc-83f0-08b3d4ebd781','e923baa3-418e-493c-80b5-0abc97b61046','12f36230-2d0e-4b97-a09c-44686e38b9b1','8fde36a4-bc5b-4738-a7ab-e0773dbc6f21','447eaaa9-0fe2-44d0-96d8-9784d1a2a379','4edcd5a5-8510-49a8-a9fc-c9721f501913','4d902611-eed7-4060-a33e-3c7fdbac6406','e9711132-d9d9-450c-8203-25cfc4bce8de','e275a668-ce79-44e2-a659-f43443265e98','0c3ebb4c-db7d-4125-b45a-0534764d4bda','7fdc93db-3b2d-4d27-8d96-e69f829f05a0','66f5ed1b-089b-446f-ac52-2c165223b0e3','2d2f2728-a485-4b69-be54-11a6666e4dc8','b93ab53e-b403-46ba-8d18-6b6a25e6b05e','d6d016fd-1373-4e23-8743-05f18f538377','cb0f3f1f-907e-4353-b46d-a28b61025edc','44832cb2-92a3-44f9-8e55-0a6c52157a6d','219a0229-a7fe-4943-aee3-5de746e15986','924bee71-5eb8-424f-83ed-a58823c33908','1237160a-2cc2-4df1-9d25-0fc7cc61209b','e2aeea67-8d45-46c9-8a05-d09674025934','6a4a9b68-9344-4146-8583-84056af6c92f','62a1fd03-6116-4d29-bc4f-db145a644e58','8c94ad45-b93b-4772-aab1-ff92fcec6610','f07634d8-433f-423f-9a7d-564535191859','a161d3d3-0592-4956-9b64-6829678b6506','50869abf-e114-4b42-bbe3-65ee0ddbd03d','bf39c49e-fb61-4aee-83fd-6c305b0d6ef9','7f5a36ed-d5b5-4732-b6bb-837dbf0fb9d8','be0a59d1-eed7-47ec-becd-453267753793','83752114-8379-467d-a01a-e31dd8c064d0','7b349b65-d906-42e5-833f-b2af38513468','450f680c-b109-486a-8fec-2b9e7ab0fbc9','a5afd00d-d3ef-4bcd-8b42-f158b2799782','165d3901-128e-4dae-96bf-8721e0c610fc','907a85de-024f-4dd6-969c-347d47a1bdff','93329a72-24d7-4faa-93d9-203f367ed334','ab488eb1-555a-409d-9b4c-78887a461c92'],'effectiveDate':'2015-06-10T00:00:00Z'}],"links":{'self':{'uri':'/ratecards/azure','method':'GET','headers':[]}},"attributes":{'objectType':'AzureRateCard'}}
    # DATABASE 저장.
    save_ratecard(rates)
    rates.clear()
    rates = get_azure_resource_price(is_shared=True)
    save_ratecard(rates, is_shared=True)
    rates.clear()


class UserAppAuthTokenHandler(BaseHTTPRequestHandler):
        
    def do_POST(self):
        partner_env = AzurePartnerCenterEnv.instance()
        ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
        if ctype == 'multipart/form-data':
            body = cgi.parse_multipart(self.rfile, pdict, encoding="utf-8")
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.get('content-length'))
            body = parse_qs(str(self.rfile.read(length), "utf-8"), encoding='utf-8', keep_blank_values=1)
        else:
            body = {}
        
        print("Requesting refresh token")
        result = requests.post("https://login.microsoftonline.com/{}/oauth2/token".format(partner_env.tenant),
                      data={
                          "resource": "https://api.partnercenter.microsoft.com",
                          "client_id": partner_env.appid,
                          "client_secret": partner_env.secret,
                          "grant_type": "authorization_code",
                          "code": body["code"]
                      })
        print("Received refresh token")
        print("====================")
        print("Store following refresh token on secret store")
        print("====================")
        token = result.json()
        print("Refresh token: "+ token["refresh_token"])
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("<html><head><title>Received refresh token</title></head>", "utf-8"))
        self.wfile.write(bytes("<body>", "utf-8"))
        self.wfile.write(bytes("<p>Sucessfully got refresh token. Continue task on CLI App.</p>", "utf-8"))
        self.wfile.write(bytes("</body></html>", "utf-8"))
        exit()


def auth_user_app_mfa():
    partner_env = AzurePartnerCenterEnv.instance()
    webServer = HTTPServer(("localhost", 8080), UserAppAuthTokenHandler)

    token_issue_url = "https://login.microsoftonline.com/common/oauth2/authorize?"
    token_issue_url += "client_id=" + partner_env.appid
    token_issue_url +=  "&response_mode=form_post"
    token_issue_url +=  "&response_type=code%20" + "id_token"
    token_issue_url +=  "&scope=openid%20profile"
    token_issue_url +=  "&nonce=1"
    print("Visit {} on web browser to continue issue token.".format(token_issue_url))
    webbrowser.open(token_issue_url)

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
