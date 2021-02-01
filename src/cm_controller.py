"""
CM database controller
Data와 Database간 컨트롤
"""
# CM database를 통한 Tenant list 받아오기.
import json
from datetime import datetime, timedelta

from src.database.db_connection import DBConnect
from src.logger.logger import LOGGER


MS_TIME_FORMATTING = '%Y-%m-%dT%H:%M:%S%z'


def get_target_tenant_list():
    pass


def save_azure_customer(tenant_info, commit=True):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_OR_UPDATE_AZURE_CUSTOMER  # Update or Insert
    insert_data = []
    for tenant in tenant_info['items']:
        # [Id],[companyName],[tenantId],[domain],[objectType],[relationshipToPartner],[RegDate],[RequestUri],[ResponseData]
        _data = (tenant['id'], tenant['companyProfile']['companyName'], tenant['companyProfile']['tenantId'], tenant['companyProfile']['domain'],
                 tenant['attributes']['objectType'], tenant['relationshipToPartner'], datetime.now(), None, None)
        insert_data.append(_data)
    db.insert_data(sql, insert_data)
    if commit:
        db.commit()
    db.close()


def save_azure_customer_subscription(subscription_info, commit=True):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_CUSTOMER_SUBSCRIPTION
    insert_data = []
    for tenant in subscription_info:
        for subscription in subscription_info[tenant]:
            # [tenantId],[subscriptionId],[offerId],[entitlementId],[offerName],[friendlyName],[quantity],[unitType],[hasPurchasableAddons],
            # [creationDate],[effectiveStartDate],[commitmentEndDate],[status],[autoRenewEnabled],[isTrial],[billingType],[billingCycle],[actions],
            # [termDuration],[isMicrosoftProduct],[attentionNeeded],[actionTaken],[contractType],[links_offer_uri],[links_product_uri],[links_sku_uri],
            # [links_availability_uri],[links_self_uri],[orderId],[attributes_etag],[attributes_objectType],[RegDate],[RequestUri],[ResponseData]
            _data = (tenant, subscription['id'], subscription['offerId'], subscription['entitlementId'], subscription['offerName'], subscription['friendlyName'],
                     subscription['quantity'], subscription['unitType'], subscription['hasPurchasableAddons'], subscription['creationDate'],
                     subscription['effectiveStartDate'], subscription['commitmentEndDate'], subscription['status'], subscription['autoRenewEnabled'],
                     subscription['isTrial'], subscription['billingType'], subscription['billingCycle'], json.dumps(subscription['actions']), subscription['termDuration'],
                     subscription['isMicrosoftProduct'], subscription['attentionNeeded'], subscription['actionTaken'], subscription['contractType'],
                     subscription['links']['offer']['uri'], subscription['links']['product']['uri'], subscription['links']['sku']['uri'],
                     subscription['links']['availability']['uri'], subscription['links']['self']['uri'], subscription['orderId'], subscription['attributes']['etag'],
                     subscription['attributes']['objectType'], datetime.now(), None, None)
            insert_data.append(_data)
    db.insert_data(sql, insert_data)
    if commit:
        db.commit()
    db.close()


def save_azure_customer_software(software_info, commit=True):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_CUSTOMER_SOFTWARE
    insert_data = []
    for tenant in software_info:
        for software in software_info[tenant]:
            # [customerId] ,[includeEntitlements] ,[referenceOrderId] ,[lineItemId] ,[alternateId]
            # ,[productId] ,[quantity] ,[entitledArtifacts] ,[skuId] ,[entitlementType] ,[expiryDate] ,[RegDate] ,[RequestUri] ,[ResponseData]
            _data = (tenant, json.dumps(software['includeEntitlements']), software['referenceOrder']['id'], software['referenceOrder']['lineItemId'], software['referenceOrder']['alternateId'],
                     software['productId'], software['quantity'], json.dumps(software['entitledArtifacts']), software['skuId'],
                     software['entitlementType'], datetime.strptime(software['expiryDate'], MS_TIME_FORMATTING), datetime.now(), None, None)
            insert_data.append(_data)
    db.insert_data(sql, insert_data, auto_commit=commit)
    # if commit:
    #     db.commit()
    db.close()


def save_azure_customer_ri(ri_info, commit=True):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_CUSTOMER_RI
    insert_data = []
    for tenant in ri_info:
        for ri in ri_info[tenant]:
            # entitledArtifacts가 1개라고 가정하고 넣기. entitledArtifacts에는 전체, 나머지에는 [0]
            # 2개 이상일 경우, Warning 으로 체크.
            if len(ri['quantityDetails']) != 1:
                LOGGER.warning(f'Azure Customer RI 데이터의 quantityDetails 배열 길이 이상. 해당 데이터: {ri}')
            if len(ri['entitledArtifacts']) != 1:
                LOGGER.warning(f'Azure Customer RI 데이터의 entitledArtifacts 배열 길이 이상. 해당 데이터: {ri}')
            # [customerId] ,[includeEntitlements] ,[referenceOrderId] ,[lineItemId] ,[alternateId] ,[productId]
            # ,[quantity] ,[quantityDetails_status] ,[entitledArtifacts] ,[entitledArtifacts_uri] ,[entitledArtifacts_headers]
            # ,[entitledArtifacts_ResourceId] ,[entitledArtifacts_artifactType] ,[entitledArtifacts_dynamicAttributes] ,[skuId]
            # ,[entitlementType] ,[fulfillmentState] ,[expiryDate] ,[RegDate] ,[RequestUri] ,[ResponseData]
            _data = (tenant, json.dumps(ri['includedEntitlements']), ri['referenceOrder']['id'], ri['referenceOrder']['lineItemId'], ri['referenceOrder']['alternateId'],
                     ri['productId'], ri['quantity'], ri['quantityDetails'][0]['status'] if len(ri['quantityDetails']) else None,
                     json.dumps(ri['entitledArtifacts']),
                     ri['entitledArtifacts'][0]['link']['uri'] if len(ri['entitledArtifacts']) else None,
                     json.dumps(ri['entitledArtifacts'][0]['link']['headers']) if len(ri['entitledArtifacts']) else None,
                     ri['entitledArtifacts'][0]['resourceId'] if len(ri['entitledArtifacts']) else None,
                     ri['entitledArtifacts'][0]['artifactType'] if len(ri['entitledArtifacts']) else None,
                     json.dumps(ri['entitledArtifacts'][0]['dynamicAttributes']) if len(ri['entitledArtifacts']) else None,
                     ri['skuId'], ri['entitlementType'], ri['fulfillmentState'],
                     datetime.strptime(ri['expiryDate'], MS_TIME_FORMATTING), datetime.now(), None, None)
            insert_data.append(_data)
    db.insert_data(sql, insert_data, auto_commit=commit)
    # if commit:
    #     db.commit()
    db.close()


def save_azure_utilization_user(tenant: str, subscription: str, start_time: datetime,
                                end_time: datetime, daily_usage: list, commit=True):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_UTILIZATION
    # [start_time],[end_time],[tenantId],[subscriptionId],[usageStartTime],[usageEndTime],[resourceId],[resourceName],
    # [resourceCategory],[resourceSubCategory],[resourceRegion],[quantity],[unit],[ResourceUri],[Location],[PartNumber],
    # [OrderNumber],[tags],[additionalInfo],[RegDate],[RequestUri],[ResponseData]
    insert_data = []
    for usage in daily_usage:
        _data = (start_time, end_time, tenant, subscription, datetime.strptime(usage['usageStartTime'], MS_TIME_FORMATTING), datetime.strptime(usage['usageEndTime'], MS_TIME_FORMATTING), usage['resource']['id'],
                 usage['resource']['name'], usage['resource']['category'], usage['resource']['subcategory'], usage['resource']['region'],
                 usage['quantity'], usage['unit'], usage['instanceData']['resourceUri'], usage['instanceData']['location'],
                 usage['instanceData']['partNumber'], usage['instanceData']['orderNumber'], json.dumps(usage['instanceData']['tags']),
                 json.dumps(usage['instanceData']['additionalInfo']), datetime.now(), None, None)
        insert_data.append(_data)
    db.insert_data(sql, insert_data)
    if commit:
        db.commit()


def get_azure_utilization_user(tenant: str, subscription: str,
                               start_time: datetime, offset=1):
    start_time = start_time
    end_time = start_time + timedelta(days=offset)
    db = DBConnect.get_instance()
    sql = db.get_sql().SELECT_AZURE_UTILIZATION_WHERE_USER
    # [start_time]=%s [end_time]=%s [tenantId]=%s [subscriptionId]=%s
    select_data = (start_time, end_time, tenant, subscription)
    return db.select_data(sql, select_data)


def remove_azure_utilization_user(tenant: str, subscription: str,
                                  start_time: datetime, offset=1, commit=True):
    start_time = start_time
    end_time = start_time + timedelta(days=offset)
    db = DBConnect.get_instance()
    sql = db.get_sql().DELETE_AZURE_UTILIZATION_WHERE_USER
    # [start_time]=%s [end_time]=%s [tenantId]=%s [subscriptionId]=%s
    delete_data = (start_time, end_time, tenant, subscription)
    if commit:
        db.commit()
    return db.delete_data(sql, delete_data)


def save_ratecard(rates, region='kr', currency='KRW', commit=True):
    """

    :param rates:
    :param region:
    :param currency:
    :param commit:
    :return:
    """
    db = DBConnect.get_instance()
    # 일괄 삭제 후, Insert
    # 일괄삭제
    del_azure_meter_sql = """DELETE FROM [dbo].[Azure_Meter]"""
    affected = db.delete_data(del_azure_meter_sql)
    LOGGER.debug(f'del_azure_meter_sql : {affected}')
    del_azure_offerterm_sql = """DELETE FROM [dbo].[Azure_OfferTerm]"""
    affected = db.delete_data(del_azure_offerterm_sql)
    LOGGER.debug(f'del_azure_offerterm_sql : {affected}')
    # 일괄 insert
    insert_azure_meter_sql = db.get_sql().INSERT_AZURE_METER
    insert_azure_offerterm_sql = db.get_sql().INSERT_OFFERTERM
    insert_data_azure_meter = []
    # METER INSERT
    for meter in rates['meters']:
        # [currency],[region],[locale],[isTaxIncluded],[meterId],[meter_name],[meter_rates],[meter_tags],[meter_category],
        # [meter_Subcategory],[meter_region],[meter_unit],[meter_includedQuantity],[meter_effectiveDate],[RegDate],[RequestUri],[ResponseData]
        _data = (rates['currency'], region, rates['locale'], rates['isTaxIncluded'], meter['id'], meter['name'], json.dumps(meter['rates']), json.dumps(meter['tags']), meter['category'],
                 meter['subcategory'], meter['region'], meter['unit'], meter['includedQuantity'], meter['effectiveDate'], datetime.now(), rates['links']['self']['uri'], "")
        insert_data_azure_meter.append(_data)
    db.insert_data(insert_azure_meter_sql, insert_data_azure_meter)

    # OFFERTERM INSERT
    insert_data_azure_offerterm = []
    for offer in rates['offerTerms']:
        if 'excludedMeterIds' in offer and len(offer['excludedMeterIds']):
            for meterid in offer['excludedMeterIds']:
                # [currency],[region],[name],[discount],[excludedMeterId],[effectiveDate],[RegDate]
                _data = (rates['currency'], region, offer['name'], offer['discount'], meterid,
                         offer['effectiveDate'], datetime.now())
                insert_data_azure_offerterm.append(_data)
        else:
            # [currency],[region],[name],[discount],[excludedMeterId],[effectiveDate],[RegDate]
            _data = (rates['currency'], region, offer['name'], offer['discount'], str(offer['excludedMeterIds']),
                     offer['effectiveDate'], datetime.now())
            insert_data_azure_offerterm.append(_data)
    db.insert_data(insert_azure_offerterm_sql, insert_data_azure_offerterm)
    if commit:
        db.commit()
    db.close()


def save_invoice(data: dict):
    db = DBConnect.get_instance()
    # insert, 인보이스 존재시 오류
    sql = db.get_sql().INSERT_AZURE_INVOICE
    insert_data = [(data['id'], datetime.strptime(data['invoiceDate'], MS_TIME_FORMATTING), datetime.strptime(data['billingPeriodStartDate'], MS_TIME_FORMATTING),
                    datetime.strptime(data['billingPeriodEndDate'], MS_TIME_FORMATTING), data['totalCharges'], data['paidAmount'],
                    data['currencyCode'], data['currencySymbol'], data['pdfDownloadLink'], data['invoiceType'], data['documentType'],
                    data['state'], json.dumps(data['links']), datetime.now())]
    pass


def save_invoice_detail(details: list):
    db = DBConnect.get_instance()
    # TODO: 바뀐 스키마에 대한 sql 수정


def save_ms_product_price(data):
    # IF EXISTS : insert or update
    pass


def save_ri_product_price(data):
    # IF EXISTS : insert or update
    pass
