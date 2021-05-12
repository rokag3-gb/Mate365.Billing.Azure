"""
CM database controller
Data와 Database간 컨트롤
"""
# CM database를 통한 Tenant list 받아오기.
import json
from datetime import datetime, timedelta
from dateutil.parser import parse, isoparse

from src.database.db_connection import DBConnect
from src.env import AzurePartnerCenterEnv
from src.logger.logger import LOGGER


MS_TIME_FORMATTING = '%Y-%m-%dT%H:%M:%S%z'
MS_TIME_FORMATTING_WITHOUT_ZONE = '%Y-%m-%dT%H:%M:%S'
is_commit = AzurePartnerCenterEnv.instance().commit

def get_target_tenant_list():
    pass


def save_azure_customer(tenant_info, commit=is_commit):
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


def save_azure_customer_subscription(subscription_info, commit=is_commit):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_CUSTOMER_SUBSCRIPTION
    insert_data = []
    for tenant in subscription_info:
        for subscription in subscription_info[tenant]:
            # [UsageDate], [tenantId],[subscriptionId],[offerId],[entitlementId],[offerName],[friendlyName],[quantity],[unitType],[hasPurchasableAddons],
            # [creationDate],[effectiveStartDate],[commitmentEndDate],[status],[autoRenewEnabled],[isTrial],[billingType],[billingCycle],[actions],
            # [termDuration],[isMicrosoftProduct],[attentionNeeded],[actionTaken],[contractType],[links_offer_uri],[links_product_uri],[links_sku_uri],
            # [links_availability_uri],[links_self_uri],[orderId],[attributes_etag],[attributes_objectType],[RegDate],[RequestUri],[ResponseData]
            _data = (datetime.now(), tenant, subscription['id'], subscription['offerId'], subscription['entitlementId'] if 'entitlementId' in subscription else subscription['id'], subscription['offerName'], subscription['friendlyName'],
                     subscription['quantity'], subscription['unitType'], subscription['hasPurchasableAddons'], 
                     isoparse(subscription['creationDate']).replace(microsecond=0, tzinfo=None),
                     isoparse(subscription['effectiveStartDate']).replace(microsecond=0, tzinfo=None), 
                     isoparse(subscription['commitmentEndDate']).replace(microsecond=0, tzinfo=None), 
                     subscription['status'], subscription['autoRenewEnabled'],
                     subscription['isTrial'], subscription['billingType'], subscription['billingCycle'], json.dumps(subscription['actions']) if 'actions' in subscription else None, subscription['termDuration'],
                     subscription['isMicrosoftProduct'], subscription['attentionNeeded'], subscription['actionTaken'], subscription['contractType'],
                     subscription['links']['offer']['uri'] if 'offer' in subscription['links'] else None, subscription['links']['product']['uri'], subscription['links']['sku']['uri'],
                     subscription['links']['availability']['uri'], subscription['links']['self']['uri'], subscription['orderId'], subscription['attributes']['etag'] if 'etag' in subscription['attributes'] else None,
                     subscription['attributes']['objectType'], datetime.now(), None, None)
            insert_data.append(_data)
    db.insert_data(sql, insert_data)
    if commit:
        db.commit()
    db.close()

def save_ms_license_usage_as_customer_subscription(usage, commit=is_commit):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_CUSTOMER_SUBSCRIPTION
    tenant = usage[0]
    insert_data = []
    for subscription in usage[1]:
        # TODO: 라이선스 사용량 Insert 쿼리 작업
        # [UsageDate], [tenantId],[subscriptionId],[offerId],[entitlementId],[offerName],[friendlyName],[quantity],[unitType],[hasPurchasableAddons],
        # [creationDate],[effectiveStartDate],[commitmentEndDate],[status],[autoRenewEnabled],[isTrial],[billingType],[billingCycle],[actions],
        # [termDuration],[isMicrosoftProduct],[attentionNeeded],[actionTaken],[contractType],[links_offer_uri],[links_product_uri],[links_sku_uri],
        # [links_availability_uri],[links_self_uri],[orderId],[attributes_etag],[attributes_objectType],[RegDate],[RequestUri],[ResponseData]
        _data = (subscription['processedDateTime'], tenant, "", subscription['productId'], "", subscription['productName'], subscription['productName'],
                subscription['licensesQualified'], "Licenses", 0, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None, datetime.now(), None, None)
        insert_data.append(_data)
        db.insert_data(sql, insert_data)
    if commit:
        db.commit()
    db.close()
def save_azure_customer_software(software_info, commit=is_commit):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_CUSTOMER_SOFTWARE
    insert_data = []
    for tenant in software_info:
        for software in software_info[tenant]:
            # [customerId] ,[includeEntitlements] ,[referenceOrderId] ,[lineItemId] ,[alternateId]
            # ,[productId] ,[quantity] ,[entitledArtifacts] ,[skuId] ,[entitlementType] ,[expiryDate] ,[RegDate] ,[RequestUri] ,[ResponseData]
            _data = (tenant, json.dumps(software['includedEntitlements']), software['referenceOrder']['id'], software['referenceOrder']['lineItemId'], software['referenceOrder']['alternateId'],
                     software['productId'], software['quantity'], json.dumps(software['entitledArtifacts']), software['skuId'],
                     software['entitlementType'], parse(software['expiryDate']), datetime.now(), None, None)
            insert_data.append(_data)
    db.insert_data(sql, insert_data, auto_commit=commit)
    if commit:
        db.commit()
    db.close()


def save_azure_customer_ri(ri_info, commit=is_commit):
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
                     parse(ri['expiryDate']), datetime.now(), None, None)
            insert_data.append(_data)
    db.insert_data(sql, insert_data)
    if commit:
        db.commit()
    db.close()


def save_azure_utilization_user(tenant: str, subscription: str, start_time: datetime,
                                end_time: datetime, daily_usage: list, commit=is_commit):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_AZURE_UTILIZATION
    # [start_time],[end_time],[tenantId],[subscriptionId],[usageStartTime],[usageEndTime],[resourceId],[resourceName],
    # [resourceCategory],[resourceSubCategory],[resourceRegion],[quantity],[unit],[ResourceUri],[Location],[PartNumber],
    # [OrderNumber],[tags],[additionalInfo],[RegDate],[RequestUri],[ResponseData]
    insert_data = []
    for usage in daily_usage:
        _data = (start_time, end_time, tenant, subscription, parse(usage['usageStartTime']), parse(usage['usageEndTime']), usage['resource']['id'],
                 usage['resource']['name'], usage['resource']['category'], usage['resource']['subcategory'], usage['resource']['region'],
                 usage['quantity'], usage['unit'], usage['instanceData']['resourceUri'], usage['instanceData']['location'],
                 usage['instanceData']['partNumber'], usage['instanceData']['orderNumber'], json.dumps(usage['instanceData']['tags']),
                 json.dumps(usage['instanceData']['additionalInfo']), datetime.now(), None, None)
        insert_data.append(_data)
    db.insert_data(sql, insert_data)
    if commit:
        db.commit()
    db.close()


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
                                  start_time: datetime, offset=1, commit=is_commit):
    start_time = start_time
    end_time = start_time + timedelta(days=offset)
    db = DBConnect.get_instance()
    sql = db.get_sql().DELETE_AZURE_UTILIZATION_WHERE_USER
    # [start_time]=%s [end_time]=%s [tenantId]=%s [subscriptionId]=%s
    delete_data = (start_time, end_time, tenant, subscription)
    if commit:
        db.commit()
    return db.delete_data(sql, delete_data)


def save_ratecard(rates, region='kr', currency='KRW', is_shared=False, commit=is_commit):
    """
    DATABASE의 [Azure_Meter] : 파트너가격
    DATABASE의 [Azure_Meter_Listprice] : 소비자가격(is_shared)
    :param rates:
    :param region:
    :param currency:
    :param commit:
    :return:
    """
    db = DBConnect.get_instance()
    # 일괄 삭제 후, Insert
    # 일괄삭제
    if is_shared:
        del_azure_meter_sql = db.get_sql().DELETE_AZURE_RATECARD_SHARED
        # del_azure_offerterm_sql = db.get_sql().DELETE_AZURE_OFFERTERM_SHARED
        insert_azure_meter_sql = db.get_sql().INSERT_AZURE_METER_SHARED
        # insert_azure_offerterm_sql = db.get_sql().INSERT_OFFERTERM_SHARED
    else:
        del_azure_meter_sql = db.get_sql().DELETE_AZURE_RATECARD
        del_azure_offerterm_sql = db.get_sql().DELETE_AZURE_OFFERTERM
        insert_azure_meter_sql = db.get_sql().INSERT_AZURE_METER
        insert_azure_offerterm_sql = db.get_sql().INSERT_OFFERTERM
        affected = db.delete_data(del_azure_offerterm_sql)
        LOGGER.debug(f'del_azure_offerterm_sql : {affected}')
    affected = db.delete_data(del_azure_meter_sql)
    LOGGER.debug(f'del_azure_meter_sql : {affected}')
    # 일괄 insert

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
    if not is_shared:
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


def save_invoice(data: list, commit=is_commit):
    db = DBConnect.get_instance()
    # insert, 인보이스 존재시 오류
    sql = db.get_sql().INSERT_AZURE_INVOICE
    insert_data = []
    # [invoiceDate], [invoiceId], [billingPeriodStartDate], [billingPeriodEndDate], [totalCharges], [paidAmount],
    # [currencyCode], [currencySymbol], [pdfDownloadLink], [invoiceType], [documentType], [state], [links_self_uri],
    # [RegDate], [RequestUri], [ResponseData]
    for invoice in data:
        if len(invoice['invoiceDate']) > 20:
            invoice['invoiceDate'] = invoice['invoiceDate'][:19] + 'Z'
        if len(invoice['billingPeriodStartDate']) > 20:
            invoice['billingPeriodStartDate'] = invoice['billingPeriodStartDate'][:19] + 'Z'
        if len(invoice['billingPeriodEndDate']) > 20:
            invoice['billingPeriodEndDate'] = invoice['billingPeriodEndDate'][:19] + 'Z'
        insert_data.append((parse(invoice['invoiceDate']), invoice['id'],
                            parse(invoice['billingPeriodStartDate']),
                            parse(invoice['billingPeriodEndDate']),
                            invoice['totalCharges'], invoice['paidAmount'],
                            invoice['currencyCode'], invoice['currencySymbol'], invoice['pdfDownloadLink'],
                            invoice['invoiceType'], invoice['documentType'],
                            invoice['state'], json.dumps(invoice['links']), datetime.now(), None, None))
    db.insert_data(sql=sql, data=insert_data)
    if commit:
        db.commit()
    db.close()


def save_invoice_detail_azure(invoice_id: str, detail: dict):
    db = DBConnect.get_instance()

    detail_item = detail['items']
    invoice_id = invoice_id

    # 존재하는지 확인. 있으면 Delete
    is_exist_sql = "SELECT 1 AS [count] FROM [dbo].[Azure_Invoice_Detail_Azure] WHERE [invoiceId] = %s AND [billingProvider] = %s"
    if db.select_data(is_exist_sql, (invoice_id, 'azure')):
        invoice_detail_delete_sql = "DELETE FROM [dbo].[Azure_Invoice_Detail_Azure] WHERE [invoiceId] = %s AND [billingProvider] = %s"
        db.delete_data(invoice_detail_delete_sql, (invoice_id, 'azure'))

    insert_sql = db.get_sql().INSERT_AZURE_INVOICE_DETAIL_AZURE
    insert_data = []
    # [invoiceId], [sku], [includedQuantity], [overageQuantity], [listPrice], [currency], [pretaxCharges],
    # [taxAmount], [postTaxTotal], [preTaxEffectiveRate], [postTaxEffectiveRate], [chargeType],
    # [invoiceLineItemType], [partnerId], [partnerName], [partnerBillableAccountId], [customerId],
    # [domainName], [customerCompanyName], [mpnId], [tier2MpnId], [invoiceNumber], [subscriptionId],
    # [subscriptionName], [subscriptionDescription], [billingCycleType], [orderId], [serviceName],
    # [serviceType], [resourceGuid], [resourceName], [region], [consumedQuantity], [chargeStartDate],
    # [chargeEndDate], [unit], [billingProvider], [RegDate], [RequestUri], [ResponseData]
    for items in detail_item:
        insert_data.append((items['invoiceNumber'], items['sku'], items['includedQuantity'], items['overageQuantity'],
                            items['listPrice'], items['currency'], items['pretaxCharges'], items['taxAmount'],
                            items['postTaxTotal'], items['pretaxEffectiveRate'], items['postTaxEffectiveRate'],
                            items['chargeType'], items['invoiceLineItemType'], items['partnerId'], items['partnerName'],
                            items['partnerBillableAccountId'], items['customerId'], items['domainName'],
                            items['customerCompanyName'], items['mpnId'], items['tier2MpnId'], items['invoiceNumber'],
                            items['subscriptionId'], items['subscriptionName'], items['subscriptionDescription'],
                            items['billingCycleType'], items['orderId'], items['serviceName'], items['serviceType'],
                            items['resourceGuid'], items['resourceName'], items['region'], items['consumedQuantity'],
                            items['chargeStartDate'], items['chargeEndDate'], items['unit'],
                            items['billingProvider'], datetime.now(), detail['links']['self']['uri'], None))
    db.insert_data(insert_sql, insert_data)


def save_invoice_detail_office(invoice_id: str, detail: dict):
    db = DBConnect.get_instance()

    detail_item = detail['items']
    invoice_id = invoice_id
    provider = 'office'
    # 존재하는지 확인. 있으면 Delete
    is_exist_sql = "SELECT 1 AS [count] FROM [dbo].[Azure_Invoice_Detail_Office] WHERE [invoiceId] = %s AND [billingProvider] = %s"
    if db.select_data(is_exist_sql, (invoice_id, provider)):
        invoice_detail_delete_sql = "DELETE FROM [dbo].[Azure_Invoice_Detail_Office] WHERE [invoiceId] = %s AND [billingProvider] = %s"
        db.delete_data(invoice_detail_delete_sql, (invoice_id, provider))

    insert_sql = db.get_sql().INSERT_AZURE_INVOICE_DETAIL_OFFICE
    insert_data = []
    # [invoiceId], [partnerId], [customerId], [customerName], [mpnId], [tier2MpnId], [orderId], [invoiceNumber],
    # [subscriptionId], [syndicationPartnerSubscriptionNumber], [offerId], [durableOfferId], [offerName], [domainName],
    # [billingCycleType], [subscriptionName], [subscriptionDescription], [chargeStartDate], [chargeEndDate],
    # [chargeType], [unitPrice], [quantity], [amount], [totalOtherDiscount], [subtotal], [tax], [totalForCustomer],
    # [currency], [invoiceLineItemType], [billingProvider], [RegDate], [RequestUri], [ResponseData]
    for items in detail_item:
        insert_data.append((items['invoiceNumber'], items['partnerId'], items['customerId'], items['customerName'],
                            items['mpnId'], items['tier2MpnId'], items['orderId'], items['invoiceNumber'],
                            items['subscriptionId'], items['syndicationPartnerSubscriptionNumber'], items['offerId'],
                            items['durableOfferId'], items['offerName'], items['domainName'], items['billingCycleType'],
                            items['subscriptionName'], items['subscriptionDescription'], items['chargeStartDate'],
                            items['chargeEndDate'], items['chargeType'], items['unitPrice'], items['quantity'],
                            items['amount'], items['totalOtherDiscount'], items['subtotal'],
                            items['tax'], items['totalForCustomer'], items['currency'], items['invoiceLineItemType'],
                            items['billingProvider'], datetime.now(), detail['links']['self']['uri'], None))
    db.insert_data(insert_sql, insert_data)


def save_invoice_detail_onetime(invoice_id: str, detail: dict):
    db = DBConnect.get_instance()

    detail_item = detail['items']
    invoice_id = invoice_id
    provider = 'one_time'
    # 존재하는지 확인. 있으면 Delete
    is_exist_sql = "SELECT 1 AS [count] FROM [dbo].[Azure_Invoice_Detail_Onetime] WHERE [invoiceId] = %s AND [billingProvider] = %s"
    if db.select_data(is_exist_sql, (invoice_id, provider)):
        invoice_detail_delete_sql = "DELETE FROM [dbo].[Azure_Invoice_Detail_Onetime] WHERE [invoiceId] = %s AND [billingProvider] = %s"
        db.delete_data(invoice_detail_delete_sql, (invoice_id, provider))

    insert_sql = db.get_sql().INSERT_AZURE_INVOICE_DETAIL_ONETIME
    insert_data = []
    # [invoiceId], [partnerId], [customerId], [customerName], [customerDomainName], [customerCountry], [invoiceNumber],
    # [mpnId], [resellerMpnId], [orderId], [orderDate], [productId], [skuId], [availabilityId], [productName],
    # [skuName], [chargeType], [unitPrice], [effectiveUnitPrice], [unitType], [quantity], [subtotal], [taxTotal],
    # [totalForCustomer], [currency], [publisherName], [publisherId], [subscriptionDescription], [subscriptionId],
    # [chargeStartDate], [chargeEndDate], [termAndBillingCycle], [alternateId], [priceAdjustmentDescription],
    # [discountDetails], [pricingCurrency], [pcToBCExchangeRate], [pcToBCExchangeRateDate], [billableQuantity],
    # [meterDescription], [billingFrequency], [reservationOrderId], [invoiceLineItemType], [billingProvider],
    # [RegDate], [RequestUri], [ResponseData])
    for items in detail_item:
        items['orderDate'] = items['orderDate'][:19] + 'Z'
        insert_data.append((items['invoiceNumber'], items['partnerId'], items['customerId'], items['customerName'],
                            items['customerDomainName'], items['customerCountry'], items['invoiceNumber'], items['mpnId'],
                            items['resellerMpnId'], items['orderId'], parse(items['orderDate']),
                            items['productId'], items['skuId'], items['availabilityId'], items['productName'],
                            items['skuName'], items['chargeType'], items['unitPrice'],
                            items['effectiveUnitPrice'], items['unitType'], items['quantity'], items['subtotal'],
                            items['taxTotal'], items['totalForCustomer'], items['currency'],
                            items['publisherName'], items['publisherId'], items['subscriptionDescription'], items['subscriptionId'],
                            parse(items['chargeStartDate']), parse(items['chargeEndDate']),
                            items['termAndBillingCycle'],
                            items['alternateId'], items['priceAdjustmentDescription'], items['discountDetails'],
                            items['pricingCurrency'], items['pcToBCExchangeRate'],
                            parse(items['pcToBCExchangeRateDate']),
                            items['billableQuantity'],
                            items['meterDescription'], items['billingFrequency'],
                            items['reservationOrderId'], items['invoiceLineItemType'], items['billingProvider'],
                            datetime.now(), detail['links']['self']['uri'], None))
    db.insert_data(insert_sql, insert_data)


def save_ms_product_price(data):
    # IF EXISTS : insert or update
    pass


def save_ri_product_price(data):
    # IF EXISTS : insert or update
    pass

def is_license_usage_missing(tenant: str, t_date: datetime) -> bool:
    db = DBConnect.get_instance()
    sql = db.get_sql().SELECT_AZURE_CUSTOMER_SUBSCRIPTION
    select_data = (t_date, tenant)
    return bool(db.select_data(sql, select_data))