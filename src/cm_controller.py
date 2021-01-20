"""
CM database controller
"""
# CM database를 통한 Tenant list 받아오기.
import json
from datetime import datetime

from src.database.db_connection import DBConnect
from src.logger.logger import LOGGER



def get_target_tenant_list():
    pass

def save_azure_customer(tenant_info, commit=True):
    db = DBConnect.get_instance()
    sql = db.get_sql().INSERT_OR_UPDATE_AZURE_CUSTOMER # Update or Insert
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


def save_ratecard(rates, region='kr', currency='KRW', commit=True):
    """

    :param rates:
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


def save_ms_product_price(data):
    # IF EXISTS : insert or update
    pass


def save_ri_product_price(data):
    # IF EXISTS : insert or update
    pass
