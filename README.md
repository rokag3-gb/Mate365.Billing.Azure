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
하기위해서는 http://paltusov.pro/2019/07/22/dynamics-365-net-core/ 와같이 Application User가 필요해보임.

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

# 실행
## Setting
- **로컬 실행시** : `Project root` 폴더에 .env 파일 생성 및 세팅 ex)
```
APPID=
SECRET=
TENANT=

PARTNER_CENTER_API_BASEURL=https://api.partnercenter.microsoft.com

DATABASE_TYPE=
DATABASE_HOST=
DATABASE_PORT=
DATABASE_USER=
DATABASE_PASSWORD=
DATABASE_NAME=

TEAMS_WEBHOOK_LOG_URL=
TEAMS_WEBHOOK_INFO_URL=
```
- `STAGE` **환경변수**를 통해 dev, prod 구분
  - dev 환경에서 Database Commit이 일어나지않음.

- **prod 환경 실행 시**: 위 변수들을 환경변수로 처리.

## 실행방법
- 로컬 환경
  ```
  python main.py -f --daily-usage  // 일별사용량 수집
  python main.py -f --daily-usage-update --update-period 10 // 일별사용량 '10일이전 ~ 오늘' 업데이트
  python main.py -f --monthly-invoice  // 이번달 인보이스 업데이트
  python main.py -f --price-update  // azure price 업데이트
  ```
- Prod 환경
  - DB password, ad secret의 경우 Parameter로 입력. (pipeline yaml 참조)
  ```
  python main.py --daily-usage --database-password $(DATABASE_PASSWORD) --app-secret $(SECRET)
  ``` 