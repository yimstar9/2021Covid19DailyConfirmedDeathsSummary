# 2021Covid19DailyConfirmedDeathsSummary
2021년 코로나 확진자,사망자 통계

아래 site에서 제공된 데이터셋은 Johns’ Hopkins 대학 내 The Center For Systems 
Science and Engineering(CSSE) 에서 업데이트 하는 전세계 코로나(COVID-19) 발
생 현황 데이터셋이다.
제공된 데이터셋 중 2021년 일간 데이터에서 Python,R code로 다음을 수행하고 결과를
팀 별로 리포트 하시오.
https://github.com/CSSEGISandData/COVID19/tree/770dafdb73e9dc31140db77b13b1b92cfd8241f9/csse_covid_19_data/csse_covid_19_daily_re
ports

(1) 일별 국가별 코로나 발생자수와 사망자 수를 기준으로 전처리 하시오. 일부
국가는 지역별로 코로나 발생자수와 사망자 수가 분리되어 있으니 국가별로 집계
하고 국가, 총발생자수, 총사망자수, 일평균 발생자수, 일평균 사망자수 리스트를
제시하시오. (누적데이터인 경우 누적데이터로 해당 결과를 제시하고, 일별 데이터
를 산출하여 총합과 일평균값을 산출하여 결과 비교)

(2) 데이터가 0인 경우(코로나 환자 0)와 데이터가 없는 경우를 구분하여 전처리
하고 전처리 시 data가 없는 국가는 제외하고 제외된 국가 리스트를 제시하시오.

(3) 2021년 1년동안 코로나 총 발생자수, 총 사망자수, 일평균 발생자수, 일평균
사망자 수를 기준으로 가장 많은 20개 국가를 내림차순으로 정렬하고 총 발생자
수, 총 사망자수, 일평균 발생자수, 일평균 사망자 수를 리포트 하시오. (4가지 기
준 각각 sorting)

(4) 2021년 1년동안 대한민국에서 발생한 총 코로나 발생자수와 총 사망자 수와
일평균 발생자수와 일평균 사망자 수를 리포트 하시오.

