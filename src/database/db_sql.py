import os

if os.environ['DATABASE_TYPE'] == 'sqlite3':
    SELECT_PREPROCESS_OF_DAY_AND_SUBSCRIPTION_SQL = """SELECT * FROM `preprocess` WHERE last_update_date = %s and subscription = %s"""
    SELECT_PREPROCESS_OF_DAY_ALL_SQL = """SELECT * FROM `preprocess` WHERE last_update_date = %s"""
    DELETE_PREPROCESS_OF_DAY_SQL = """DELETE FROM `preprocess` WHERE last_update_date = %s and subscription = %s"""

    INSERT_SUMMARY_SQL = "INSERT INTO `summary` (`tenant`, `subscription`, `body`, `last_update_date`) VALUES (%s, %s, %s, %s);"
    INSERT_DETAIL_SQL = "INSERT INTO `detail` (`tenant`, `subscription`, `body`, `last_update_date`) VALUES (%s, %s, %s, %s);"
    INSERT_PREPROCESS_SQL = "INSERT INTO `preprocess` (`tenant`, `subscription`, `body`, `last_update_date`) VALUES (%s, %s, %s, %s);"
    UPDATE_PREPROCESS_SQL = """UPDATE `preprocess` set body = JSON_SET(body, '$.Services', CAST(%s as json)) WHERE `tenant` = %s and `subscription` = %s and `last_update_date` = %s"""
    INSERT_INVOICE_SQL = """INSERT INTO `invoice` (`SubscriptionId`, `OfferName`, `ChargeStartDate`, `ChargeEndDate`, `UnitPrice`,
         `UnitPriceRrp`, `Quantity`, `BillableRatio`, `SubTotal`, `SubTotalRrp`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    UPDATE_INVOICE_IN_PREPROCESS_SQL = """UPDATE preprocess set body = JSON_SET(body, '$.BillingTable', CAST(%s as json)) 
        WHERE preprocess_id = (SELECT preprocess_id FROM
         (SELECT preprocess_id FROM msw.preprocess WHERE subscription = %s and last_update_date >= %s and
          last_update_date <= %s order by last_update_date desc LIMIT 1) AS s)"""
    SELECT_PRODUCT_PRICE_ALL = """SELECT * FROM msw.product_price"""
elif os.environ['DATABASE_TYPE'] == 'mssql':
    INSERT_OR_UPDATE_AZURE_CUSTOMER = """
    DECLARE @tId VARCHAR(50);
    DECLARE @companyName NVARCHAR(300);
    DECLARE @tenantId VARCHAR(50);
    DECLARE @domain VARCHAR(200);
    DECLARE @objectType VARCHAR(50);
    DECLARE @relationshipToPartner VARCHAR(50);
    DECLARE @RegDate DATETIME;
    DECLARE @RequestUri VARCHAR(300);
    DECLARE @ResponseData VARCHAR(max);
    
    SET @tId = %s;
    SET @companyName = %s;
    SET @tenantId = %s;
    SET @domain = %s;
    SET @objectType = %s;
    SET @relationshipToPartner = %s;
    SET @RegDate = %s;
    SET @RequestUri = %s;
    SET @ResponseData = %s;
    IF EXISTS (SELECT 1
               FROM [dbo].[Azure_Customer]
               WHERE [tenantId] = @tenantId)
    BEGIN
         UPDATE [dbo].[Azure_Customer]
            SET [Id] = @tId
                ,[companyName] = @companyName
                ,[tenantId] = @tenantId
                ,[domain] = @domain
                ,[objectType] = @objectType
                ,[relationshipToPartner] = @relationshipToPartner
                ,[RegDate] = @RegDate
                ,[RequestUri] = @RequestUri
                ,[ResponseData] = @RequestUri
            WHERE [tenantId] = @tenantId
    END
    ELSE
    BEGIN
         INSERT INTO [dbo].[Azure_Customer]
           ([Id]
           ,[companyName]
           ,[tenantId]
           ,[domain]
           ,[objectType]
           ,[relationshipToPartner]
           ,[RegDate]
           ,[RequestUri]
           ,[ResponseData])
           VALUES
           (@tId
            ,@companyName
            ,@tenantId
            ,@domain
            ,@objectType
            ,@relationshipToPartner
            ,@RegDate
            ,@RequestUri
            ,@ResponseData)
    END
    """
    INSERT_AZURE_CUSTOMER = """
    INSERT INTO [dbo].[Azure_Customer]
           ([Id]
           ,[companyName]
           ,[tenantId]
           ,[domain]
           ,[objectType]
           ,[relationshipToPartner]
           ,[RegDate]
           ,[RequestUri]
           ,[ResponseData])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)
"""

    INSERT_AZURE_CUSTOMER_SUBSCRIPTION = """
    INSERT INTO [dbo].[Azure_Customer_Subscription]
           ([UsageDate]
           ,[tenantId]
           ,[subscriptionId]
           ,[offerId]
           ,[entitlementId]
           ,[offerName]
           ,[friendlyName]
           ,[quantity]
           ,[unitType]
           ,[hasPurchasableAddons]
           ,[creationDate]
           ,[effectiveStartDate]
           ,[commitmentEndDate]
           ,[status]
           ,[autoRenewEnabled]
           ,[isTrial]
           ,[billingType]
           ,[billingCycle]
           ,[actions]
           ,[termDuration]
           ,[isMicrosoftProduct]
           ,[attentionNeeded]
           ,[actionTaken]
           ,[contractType]
           ,[links_offer_uri]
           ,[links_product_uri]
           ,[links_sku_uri]
           ,[links_availability_uri]
           ,[links_self_uri]
           ,[orderId]
           ,[attributes_etag]
           ,[attributes_objectType]
           ,[RegDate]
           ,[RequestUri]
           ,[ResponseData])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)
    """
    INSERT_AZURE_CUSTOMER_SOFTWARE = """
    INSERT INTO [dbo].[Azure_Entitlements_Software]
               ([customerId]
               ,[includeEntitlements]
               ,[referenceOrderId]
               ,[lineItemId]
               ,[alternateId]
               ,[productId]
               ,[quantity]
               ,[entitledArtifacts]
               ,[skuId]
               ,[entitlementType]
               ,[expiryDate]
               ,[RegDate]
               ,[RequestUri]
               ,[ResponseData])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)
    """
    INSERT_AZURE_CUSTOMER_RI = """
    INSERT INTO [dbo].[Azure_Entitlements_RI]
               ([customerId]
               ,[includeEntitlements]
               ,[referenceOrderId]
               ,[lineItemId]
               ,[alternateId]
               ,[productId]
               ,[quantity]
               ,[quantityDetails_status]
               ,[entitledArtifacts]
               ,[entitledArtifacts_uri]
               ,[entitledArtifacts_headers]
               ,[entitledArtifacts_ResourceId]
               ,[entitledArtifacts_artifactType]
               ,[entitledArtifacts_dynamicAttributes]
               ,[skuId]
               ,[entitlementType]
               ,[fulfillmentState]
               ,[expiryDate]
               ,[RegDate]
               ,[RequestUri]
               ,[ResponseData])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)
    """
    INSERT_AZURE_INVOICE = """    
    DECLARE @invoiceDate datetime;
    DECLARE @invoiceId varchar(30);
    DECLARE @billingPeriodStartDate datetime;
    DECLARE @billingPeriodEndDate datetime;
    DECLARE @totalCharges money;
    DECLARE @paidAmount money;
    DECLARE @currencyCode varchar(20);
    DECLARE @currencySymbol nvarchar(10);
    DECLARE @pdfDownloadLink varchar(300);
    DECLARE @invoiceType varchar(50);
    DECLARE @documentType varchar(50);
    DECLARE @state varchar(50);
    DECLARE @links_self_uri varchar(300);
    DECLARE @RegDate datetime;
    DECLARE @RequestUri varchar(300);
    DECLARE @ResponseData varchar(max);
    
    SET @invoiceDate = %s;
    SET @invoiceId = %s;
    SET @billingPeriodStartDate = %s;
    SET @billingPeriodEndDate = %s;
    SET @totalCharges = %s;
    SET @paidAmount = %s;
    SET @currencyCode = %s;
    SET @currencySymbol = %s;
    SET @pdfDownloadLink = %s;
    SET @invoiceType = %s;
    SET @documentType = %s;
    SET @state = %s;
    SET @links_self_uri = %s;
    SET @RegDate = %s;
    SET @RequestUri = %s;
    SET @ResponseData = %s;
    
    IF EXISTS (SELECT 1
               FROM [dbo].[Azure_Invoice]
               WHERE [invoiceId] = @invoiceId)
    BEGIN
         UPDATE [dbo].[Azure_Invoice]
            SET [invoiceDate] = @invoiceDate
              ,[invoiceId] = @invoiceId
              ,[billingPeriodStartDate] = @billingPeriodStartDate
              ,[billingPeriodEndDate] = @billingPeriodEndDate
              ,[totalCharges] = @totalCharges
              ,[paidAmount] = @paidAmount
              ,[currencyCode] = @currencyCode
              ,[currencySymbol] = @currencySymbol
              ,[pdfDownloadLink] = @pdfDownloadLink
              ,[invoiceType] = @invoiceType
              ,[documentType] = @documentType
              ,[state] = @state
              ,[links_self_uri] = @links_self_uri
              ,[RegDate] = @RegDate
              ,[RequestUri] = @RequestUri
              ,[ResponseData] = @ResponseData
            WHERE [invoiceId] = @invoiceId
    END
    ELSE
    BEGIN
         INSERT INTO [dbo].[Azure_Invoice]
           ([invoiceDate]
           ,[invoiceId]
           ,[billingPeriodStartDate]
           ,[billingPeriodEndDate]
           ,[totalCharges]
           ,[paidAmount]
           ,[currencyCode]
           ,[currencySymbol]
           ,[pdfDownloadLink]
           ,[invoiceType]
           ,[documentType]
           ,[state]
           ,[links_self_uri]
           ,[RegDate]
           ,[RequestUri]
           ,[ResponseData])
           VALUES
           (@invoiceDate
           ,@invoiceId
           ,@billingPeriodStartDate
           ,@billingPeriodEndDate
           ,@totalCharges
           ,@paidAmount
           ,@currencyCode
           ,@currencySymbol
           ,@pdfDownloadLink
           ,@invoiceType
           ,@documentType
           ,@state
           ,@links_self_uri
           ,@RegDate
           ,@RequestUri
           ,@ResponseData)
    END
    """
    # INSERT_AZURE_INVOICE_DETAIL = """
    # INSERT INTO [dbo].[Azure_Invoice_Detail]
    #        ([invoiceId]
    #        ,[customerBillableAccount]
    #        ,[usageDate]
    #        ,[invoiceLineItemType]
    #        ,[partnerId]
    #        ,[partnerName]
    #        ,[partnerBillableAccountId]
    #        ,[customerId]
    #        ,[domainName]
    #        ,[customerCompanyName]
    #        ,[mpnId]
    #        ,[tier2MpnId]
    #        ,[invoiceNumber]
    #        ,[subscriptionId]
    #        ,[subscriptionName]
    #        ,[subscriptionDescription]
    #        ,[billingCycleType]
    #        ,[orderId]
    #        ,[serviceName]
    #        ,[serviceType]
    #        ,[resourceGuid]
    #        ,[resourceName]
    #        ,[region]
    #        ,[consumedQuantity]
    #        ,[chargeStartDate]
    #        ,[chargeEndDate]
    #        ,[unit]
    #        ,[billingProvider]
    #        ,[attributes_objectType]
    #        ,[RegDate]
    #        ,[RequestUri]
    #        ,[ResponseData])
    #  VALUES
    #        (<invoiceId, varchar(30),>
    #        ,<customerBillableAccount, varchar(20),>
    #        ,<usageDate, datetime,>
    #        ,<invoiceLineItemType, varchar(50),>
    #        ,<partnerId, varchar(50),>
    #        ,<partnerName, nvarchar(300),>
    #        ,<partnerBillableAccountId, varchar(20),>
    #        ,<customerId, varchar(30),>
    #        ,<domainName, varchar(200),>
    #        ,<customerCompanyName, nvarchar(300),>
    #        ,<mpnId, varchar(20),>
    #        ,<tier2MpnId, varchar(20),>
    #        ,<invoiceNumber, varchar(30),>
    #        ,<subscriptionId, varchar(50),>
    #        ,<subscriptionName, nvarchar(200),>
    #        ,<subscriptionDescription, nvarchar(1000),>
    #        ,<billingCycleType, varchar(50),>
    #        ,<orderId, varchar(50),>
    #        ,<serviceName, varchar(200),>
    #        ,<serviceType, nvarchar(200),>
    #        ,<resourceGuid, varchar(50),>
    #        ,<resourceName, nvarchar(200),>
    #        ,<region, nvarchar(100),>
    #        ,<consumedQuantity, float,>
    #        ,<chargeStartDate, datetime,>
    #        ,<chargeEndDate, datetime,>
    #        ,<unit, varchar(20),>
    #        ,<billingProvider, varchar(20),>
    #        ,<attributes_objectType, varchar(200),>
    #        ,<RegDate, datetime,>
    #        ,<RequestUri, varchar(300),>
    #        ,<ResponseData, varchar(max),>)
    # """
    INSERT_OR_UPDATE_AZURE_METER = """
    UPDATE [dbo].[Azure_Meter]
       SET [currency] = <currency, varchar(20),>
          ,[region] = <region, varchar(20),>
          ,[locale] = <locale, varchar(20),>
          ,[isTaxIncluded] = <isTaxIncluded, bit,>
          ,[meterId] = <meterId, varchar(50),>
          ,[meter_name] = <meter_name, varchar(100),>
          ,[meter_rates] = <meter_rates, varchar(500),>
          ,[meter_tags] = <meter_tags, varchar(100),>
          ,[meter_category] = <meter_category, varchar(50),>
          ,[meter_Subcategory] = <meter_Subcategory, varchar(50),>
          ,[meter_region] = <meter_region, varchar(20),>
          ,[meter_unit] = <meter_unit, varchar(20),>
          ,[meter_includedQuantity] = <meter_includedQuantity, float,>
          ,[meter_effectiveDate] = <meter_effectiveDate, datetime,>
          ,[RegDate] = <RegDate, datetime,>
          ,[RequestUri] = <RequestUri, varchar(300),>
          ,[ResponseData] = <ResponseData, varchar(max),>
     WHERE <검색 조건,,>
    
    """
    INSERT_AZURE_METER = """
    INSERT INTO [dbo].[Azure_Meter]
           ([currency]
           ,[region]
           ,[locale]
           ,[isTaxIncluded]
           ,[meterId]
           ,[meter_name]
           ,[meter_rates]
           ,[meter_tags]
           ,[meter_category]
           ,[meter_Subcategory]
           ,[meter_region]
           ,[meter_unit]
           ,[meter_includedQuantity]
           ,[meter_effectiveDate]
           ,[RegDate]
           ,[RequestUri]
           ,[ResponseData])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)

    """
    INSERT_AZURE_METER_SHARED = """
        INSERT INTO [dbo].[Azure_Meter_Listprice]
               ([currency]
               ,[region]
               ,[locale]
               ,[isTaxIncluded]
               ,[meterId]
               ,[meter_name]
               ,[meter_rates]
               ,[meter_tags]
               ,[meter_category]
               ,[meter_Subcategory]
               ,[meter_region]
               ,[meter_unit]
               ,[meter_includedQuantity]
               ,[meter_effectiveDate]
               ,[RegDate]
               ,[RequestUri]
               ,[ResponseData])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)

        """
    INSERT_OFFERTERM = """
    INSERT INTO [dbo].[Azure_OfferTerm]
           ([currency]
           ,[region]
           ,[name]
           ,[discount]
           ,[excludedMeterId]
           ,[effectiveDate]
           ,[RegDate])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)
    """
    INSERT_OFFERTERM_SHARED = """
        INSERT INTO [dbo].[Azure_OfferTerm_Listprice]
               ([currency]
               ,[region]
               ,[name]
               ,[discount]
               ,[excludedMeterId]
               ,[effectiveDate]
               ,[RegDate])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)
        """
    INSERT_PRICELIST = """
    INSERT INTO [dbo].[Azure_PriceList]
           ([ACDU]
           ,[ValidFromDate]
           ,[ValidToDate]
           ,[OfferDisplayName]
           ,[OfferId]
           ,[LicenseAgreementType]
           ,[PurchaseUnit]
           ,[SecondaryLicenseType]
           ,[EndCustomerType]
           ,[ListPrice]
           ,[ERPPrice]
           ,[Material]
           ,[RegDate])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)
    """
    INSERT_RI_PRICE = """
    INSERT INTO [dbo].[Azure_RI_PriceList]
           ([ProductTitle]
           ,[ServiceType]
           ,[ProductId]
           ,[EffectiveDate]
           ,[SkuId]
           ,[SkuTitle]
           ,[ArmRegionName]
           ,[ArmSkuName]
           ,[Duration]
           ,[ConsumptionMeterId]
           ,[AUD]
           ,[AUD_FJ]
           ,[BRL]
           ,[CAD]
           ,[CHF]
           ,[CNY]
           ,[DKK]
           ,[EUR]
           ,[GBP]
           ,[INR]
           ,[JPY]
           ,[KRW]
           ,[NOK]
           ,[NZD]
           ,[RUB]
           ,[SEK]
           ,[TWD]
           ,[USD]
           ,[RegDate])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)
           """
    INSERT_AZURE_UTILIZATION = """
    INSERT INTO [dbo].[Azure_Utilization]
           ([start_time]
           ,[end_time]
           ,[tenantId]
           ,[subscriptionId]
           ,[usageStartTime]
           ,[usageEndTime]
           ,[resourceId]
           ,[resourceName]
           ,[resourceCategory]
           ,[resourceSubCategory]
           ,[resourceRegion]
           ,[quantity]
           ,[unit]
           ,[ResourceUri]
           ,[Location]
           ,[PartNumber]
           ,[OrderNumber]
           ,[tags]
           ,[additionalInfo]
           ,[RegDate]
           ,[RequestUri]
           ,[ResponseData])
     VALUES
           (%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s
           ,%s)
    """
    SELECT_AZURE_UTILIZATION_WHERE_USER = """
    SELECT [Seq]
    ,[start_time]
    ,[end_time]
    ,[tenantId]
    ,[subscriptionId]
    ,[usageStartTime]
    ,[usageEndTime]
    ,[resourceId]
    ,[resourceName]
    ,[resourceCategory]
    ,[resourceSubCategory]
    ,[resourceRegion]
    ,[quantity]
    ,[unit]
    ,[ResourceUri]
    ,[Location]
    ,[PartNumber]
    ,[OrderNumber]
    ,[tags]
    ,[additionalInfo]
    ,[RegDate]
    ,[RequestUri]
    ,[ResponseData]
  FROM [dbo].[Azure_Utilization]
  WHERE [start_time]=%s and 
      [end_time]=%s and
      [tenantId]=%s and
      [subscriptionId]=%s

    """
    DELETE_AZURE_UTILIZATION_WHERE_USER = """
    DELETE FROM [dbo].[Azure_Utilization]
    WHERE [start_time]=%s and 
      [end_time]=%s and
      [tenantId]=%s and
      [subscriptionId]=%s
    """
    DELETE_AZURE_RATECARD = """DELETE FROM [dbo].[Azure_Meter]"""
    DELETE_AZURE_RATECARD_SHARED = """DELETE FROM [dbo].[Azure_Meter_Listprice]"""
    DELETE_AZURE_OFFERTERM = """DELETE FROM [dbo].[Azure_OfferTerm]"""
    DELETE_AZURE_OFFERTERM_SHARED = """DELETE FROM [dbo].[Azure_OfferTerm_Listprice]"""
    INSERT_AZURE_INVOICE_DETAIL_AZURE = """
    INSERT INTO [dbo].[Azure_Invoice_Detail_Azure]
               ([invoiceId]
               ,[sku]
               ,[includedQuantity]
               ,[overageQuantity]
               ,[listPrice]
               ,[currency]
               ,[pretaxCharges]
               ,[taxAmount]
               ,[postTaxTotal]
               ,[preTaxEffectiveRate]
               ,[postTaxEffectiveRate]
               ,[chargeType]
               ,[invoiceLineItemType]
               ,[partnerId]
               ,[partnerName]
               ,[partnerBillableAccountId]
               ,[customerId]
               ,[domainName]
               ,[customerCompanyName]
               ,[mpnId]
               ,[tier2MpnId]
               ,[invoiceNumber]
               ,[subscriptionId]
               ,[subscriptionName]
               ,[subscriptionDescription]
               ,[billingCycleType]
               ,[orderId]
               ,[serviceName]
               ,[serviceType]
               ,[resourceGuid]
               ,[resourceName]
               ,[region]
               ,[consumedQuantity]
               ,[chargeStartDate]
               ,[chargeEndDate]
               ,[unit]
               ,[billingProvider]
               ,[RegDate]
               ,[RequestUri]
               ,[ResponseData])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)

    """
    INSERT_AZURE_INVOICE_DETAIL_OFFICE = """

    INSERT INTO [dbo].[Azure_Invoice_Detail_Office]
               ([invoiceId]
               ,[partnerId]
               ,[customerId]
               ,[customerName]
               ,[mpnId]
               ,[tier2MpnId]
               ,[orderId]
               ,[invoiceNumber]
               ,[subscriptionId]
               ,[syndicationPartnerSubscriptionNumber]
               ,[offerId]
               ,[durableOfferId]
               ,[offerName]
               ,[domainName]
               ,[billingCycleType]
               ,[subscriptionName]
               ,[subscriptionDescription]
               ,[chargeStartDate]
               ,[chargeEndDate]
               ,[chargeType]
               ,[unitPrice]
               ,[quantity]
               ,[amount]
               ,[totalOtherDiscount]
               ,[subtotal]
               ,[tax]
               ,[totalForCustomer]
               ,[currency]
               ,[invoiceLineItemType]
               ,[billingProvider]
               ,[RegDate]
               ,[RequestUri]
               ,[ResponseData])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)
    """
    INSERT_AZURE_INVOICE_DETAIL_ONETIME = """
    INSERT INTO [dbo].[Azure_Invoice_Detail_Onetime]
               ([invoiceId]
               ,[partnerId]
               ,[customerId]
               ,[customerName]
               ,[customerDomainName]
               ,[customerCountry]
               ,[invoiceNumber]
               ,[mpnId]
               ,[resellerMpnId]
               ,[orderId]
               ,[orderDate]
               ,[productId]
               ,[skuId]
               ,[availabilityId]
               ,[productName]
               ,[skuName]
               ,[chargeType]
               ,[unitPrice]
               ,[effectiveUnitPrice]
               ,[unitType]
               ,[quantity]
               ,[subtotal]
               ,[taxTotal]
               ,[totalForCustomer]
               ,[currency]
               ,[publisherName]
               ,[publisherId]
               ,[subscriptionDescription]
               ,[subscriptionId]
               ,[chargeStartDate]
               ,[chargeEndDate]
               ,[termAndBillingCycle]
               ,[alternateId]
               ,[priceAdjustmentDescription]
               ,[discountDetails]
               ,[pricingCurrency]
               ,[pcToBCExchangeRate]
               ,[pcToBCExchangeRateDate]
               ,[billableQuantity]
               ,[meterDescription]
               ,[billingFrequency]
               ,[reservationOrderId]
               ,[invoiceLineItemType]
               ,[billingProvider]
               ,[RegDate]
               ,[RequestUri]
               ,[ResponseData])
         VALUES
               (%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s
               ,%s)

    """

INSERT_AZUREPLAN_UNBILLED_RAW = """

    INSERT INTO [dbo].[Azure_Invoice_Unbilled_raw]
               ([ResponseData])
         VALUES
               (%s)
    """