# 설계
## Flow
- 고객 Tenant 수집
  - Tenant 리스트를 따로 저장(Database 등 admin페이지에서 추가 가능한 형태)
  - 크롤러에서 여기에 저장된 Tenant를 target으로 수집.
- 구독 크롤러  
  - Azure, office 등 구분해서 사용량관련 크롤러에 사용.  
`Tenant ID input > Subscription 정보 output`
- 사용금액 크롤러  
`구독ID,Date input > Daily 사용량 수집 > 기준 제품가격, 환율 요청 > 사용금액계산 > 결과 write OR update in database`  
`구독ID,Date input > 이번달(청구기간)사용금액 수집 > 결과 update in database`
https://docs.microsoft.com/ko-kr/partner/develop/get-a-price-sheet

- 인보이스 크롤러  
`구독ID,Date input > 해당월 인보이스(청구) 수집 > 결과 write in database`


# 과금테스트 계정
## Sendbox
```
'tenantId': 'cd3bdda9-bc0e-4fdc-b858-10b850d7043b',
'domain': 'previewcloudmtcustomer.onmicrosoft.com',
'companyName': 'CloudMatePreview'

'tenantId': '9478e9f4-fb30-4450-8f05-544ed27c16c4',
'domain': 'previewcloudmtcustomer2.onmicrosoft.com',
'companyName': 'CloudMatePreview_2',
```

