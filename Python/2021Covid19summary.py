##임성구

import pandas as pd
import os
import warnings
import numpy as np
warnings.simplefilter(action='ignore', category=FutureWarning)
# import numpy as np
os.getcwd()

pd.options.display.max_rows =30
pd.options.display.min_rows =20
pd.options.display.max_columns =10
pd.set_option('display.max.colwidth',15)

##검토용 데이터프레임
test=pd.DataFrame()
test1=pd.DataFrame()
testsub=pd.DataFrame()
test20=pd.DataFrame()
test21=pd.DataFrame()

nDf=pd.DataFrame() #newDataframe 오늘데이터프레임
tDf=pd.DataFrame() #tempDataFrame 필요한 컬럼들만 추출한 데이터프레임을 저장하는 곳
subDf=pd.DataFrame() #전날누적DataFrame(빼기 하기위한 프레임)
rs=pd.DataFrame()  #결과DataFrame
final=pd.DataFrame() #최종 DataFrame

###############전처리 과정#################################
filename=os.listdir('covid19daily') ##폴더지정해서 폴더내 파일 이름 리스트에 저장
df = pd.read_csv('12-31-2020.csv') ##누적데이터이므로 20년데이터 제외하기 위해 2020년 12월31일 데이터불러옴
#데이터 불러와서 필요없는 columns들 제거
df.drop(labels=['FIPS','Admin2','Province_State','Last_Update','Long_','Lat','Recovered',
                'Active','Combined_Key','Incident_Rate','Case_Fatality_Ratio'], axis=1)
#국가,확진자, 사망자를 country_region으로 그룹하고 합계낸 것을 tDf로 객체화
tDf=df[['Country_Region', 'Confirmed', 'Deaths']].groupby('Country_Region').sum()
#tDf 멀티컬럼명(날짜,확진자,사망자) 설정
tDf.columns=[['12-31-2020','12-31-2020'],['Confirmed','Deaths']]
#셀 합산을 위해 멀티컬럼 없는 subDf 객체 (tDf와 내용은 똑같다)
subDf=df[['Country_Region', 'Confirmed', 'Deaths']].groupby('Country_Region').sum()

###########최종검토용 데이터(2021년 - 2020년 = 21년 데이터)#############
test21temp = pd.read_csv('covid19daily/12-31-2021.csv')
test21temp.drop(labels=['FIPS','Admin2','Province_State','Last_Update','Long_','Lat',
                        'Recovered','Active','Combined_Key','Incident_Rate','Case_Fatality_Ratio'], axis=1)
test21=test21temp[['Country_Region', 'Confirmed', 'Deaths']].groupby('Country_Region').sum()

test20.insert(loc=0,column='Confirmed',value=subDf['Confirmed'])
test20.insert(loc=1,column='Deaths',value=subDf['Deaths'])

test=test21.sub(test20,fill_value=0)

########폴더내에 파일을 하나씩 불러오는 for문
for i in range(len(filename)):
    df1= pd.read_csv('covid19daily/'+filename[i]) ##파일 하나씩 불러와서 df1로 객체화

    #필요없는 columns들 버리기
    df1.drop(labels=['FIPS','Admin2','Province_State','Last_Update','Long_','Lat','Recovered',
                     'Active','Combined_Key','Incident_Rate','Case_Fatality_Ratio'], axis=1)
    #국가,확진자, 사망자를 country_region으로 그룹하고 합계낸 것을 nDf(오늘누적데이터)로 객체화
    nDf=df1[['Country_Region', 'Confirmed', 'Deaths']].groupby('Country_Region').sum()
    #nDf에서 subDf를 빼준다(오늘데이터(nDf) = 오늘누적데이터 - 전날누적데이터) ///// 멀티컬럼이 있으면 빼기가 안되서 멀티컬럼이 없는 subDf를 따로 만들어준것
    #값에서 Nan을 빼면 Nan이 나오기 때문에 확진자 없다가 21년에 첫 환자 발생하여 새로 나타난 국가(Micronesia)가 제대로 집계가 안되었었다
    #1월21일 1명 발생했을때 제대로 집계를 못하고 그 뒤로 누적 확진자 수가 늘지 않아서 오늘누적-전날누적=0인 상황->총 확진자 0명 (실제데이터는 1명)
    #fill_value=0으로 결측값을 해결하였다.
    nDf=nDf.sub(subDf,fill_value=0)
    #nDf(오늘데이터) 멀티컬럼명을 (날짜, confirmed, deaths)로 설정
    nDf.columns = [[filename[i][:-4],filename[i][:-4]],['Confirmed','Deaths']]
    #tDf(임시데이터프레임)와 오늘데이터프레임 국가 기준으로 합칩합으로 합치고 result로 객체화
    result = pd.merge(tDf, nDf, how='outer', left_index=True, right_index=True)
    #오늘누적데이터 subDf로 객체화 (다음 파일 불러와서 뺄셈할때 전날누적데이터로 쓰임)
    subDf = df1[['Country_Region', 'Confirmed', 'Deaths']].groupby('Country_Region').sum()
    #tDf(임시데이터프레임)에 날짜별로 계속 데이터를 쌓는다.
    tDf=result

#result에 2020년 12월31일 컬럼 삭제하고 deep copy해서 rs로 객체화.
rs=(result.drop(labels=['12-31-2020'],axis=1,level=0)).copy(deep=True)
print(rs)

#rs 데이터프레임에서 멀티컬럼(날짜만)를 삭제(연산하는데 방해되고 에러뜨기때문에)
rs.columns=rs.columns.droplevel(0)

##############오입력 갯수 및 국가 리스트###########
#missC : missConfirmed 잘못입력된 확진자(누적데이터인데 전날보다 숫자가 적음)
#missD : missDeath 잘못입력된 사망자(누적데이터인데 전날보다 숫자가 적음)

#확진자컬럼 데이터중 음수인것들 row기준으로 갯수 세서 새로운 missC컬럼에 표시
final['missC']=((rs['Confirmed']<0)).sum(axis=1)
#음수데이터가 나온 index(국가) 추출
ts=(final['missC']) >0
missC_Country=final.index[ts]
missC_Country

#확진자컬럼내 결측값 row기준으로 갯수 세서 새로운 nullC컬럼에 표시
final['nullC']=((rs['Confirmed']).isnull()).sum(axis=1)
ts=(final['nullC']) >0
nullC_Country=final.index[ts]
nullC_Country

#사망자컬럼 데이터중 음수인것들 열기준으로 갯수 세서 새로운 missD컬럼에 표시
final['missD']=(rs['Deaths']<0).sum(axis=1)
#음수데이터가 나온 index(국가) 추출
ts=(final['missD']) >0
missD_Country=final.index[ts]
missD_Country

#사망자컬럼내 결측값 row기준으로 갯수 세서 새로운 nullD컬럼에 표시
final['nullD']=((rs['Deaths']).isnull()).sum(axis=1)
ts=(final['nullD']) >0
nullD_Country=final.index[ts]
nullD_Country

#음수데이터들 0으로 바꿔줌
#Nan데이터 0으로 바꿔주고 float64->int64 형변환
#(0이면 합계에는 영향없고 평균 구할땐 갯수만 365에서 빼주면된다)
rs[rs<0]=0
rs.fillna(0, inplace=True, downcast='infer')


'''
# (1) 일별 국가별 코로나 발생자수와 사망자 수를 기준으로 전처리 하시오. 일부
# 국가는 지역별로 코로나 발생자수와 사망자 수가 분리되어 있으니 국가별로
# 집계하고 국가, 총발생자수, 총사망자수, 일평균 발생자수, 일평균 사망자수 리
# 스트를 제시하시오.
# (난이도: 4, 배점: 30점)
'''

##confirmed컬럼을 row기준으로 합계내서 새로운 컬럼(총확진자)에 표시
final.insert(loc=0,column='총확진자',value=rs['Confirmed'].sum(axis=1))
#confirmed컬럼을 row기준으로 합계내고 [1년(365일)-음수데이터 갯수]로 나눈값을 새로운 컬럼(일평균 확진자)에 표시
final.insert(loc=1,column='일평균 확진자',value=rs['Confirmed'].sum(axis=1)/(365-(final['missC']+final['nullC'])))
#deaths컬럼을 row기준으로 합계내서 새로운 컬럼(총사망자)에 표시
final.insert(loc=2,column='총사망자',value=rs['Deaths'].sum(axis=1))
#deaths컬럼을 row기준으로 합계내고 [1년(365일)-음수데이터 갯수]로 나눈값을 새로운 컬럼(일평균 사망자)에 표시
final.insert(loc=3,column='일평균 사망자',value=rs['Deaths'].sum(axis=1)/(365-(final['missD']+final['nullD'])))


# final[['총확진자', '일평균 확진자', '총사망자', '일평균 사망자','missC', 'nullC', 'missD', 'nullD']]
# print(final)
# print(test1)
# # final.to_csv('result.csv')
# final.loc['Micronesia']

'''
# (2) 데이터가 0인 경우(코로나 환자 0)와 데이터가 없는 경우를 구분하여 전처
# 리하고 전처리 시 data가 없는 국가는 제외하고 제외된 국가 리스트를 제시하
# 시오
'''
###데이터들이 일별 데이터가 아니라 누적되는 데이터라서 오늘데이터=오늘누적데이터-전날누적데이터로 계산했는데
###몇몇데이터들이 잘못 입력이 되었는지 오늘데이터가 음수값으로 나오는 경우가 있었다.
###그래서 이상치(음수값) 갯수를 파악하고 해당 국가 리스트도 작성하였다.(1번에서)
###음수값은 0으로 처리하여 sum,mean에 영향없게 하였다
###null값은 0으로 대체

Ans = final.loc[final['총확진자']==0]
print("총합계 0인 국가 : 총",len(list(Ans.index)),"개 국가",list(Ans.index))
print('확진자 이상값 존재 국가(음수) : 총',len(missC_Country),'개 국가',list(missC_Country))
print('사망자 이상값 존재 국가(음수) : 총',len(missD_Country),'개 국가',list(missD_Country))
print('확진자 결측값 존재 국가(Nan) : 총',len(nullC_Country),'개 국가',list(nullC_Country))
print('사망자 결측값 존재 국가(Nan) : 총',len(nullD_Country),'개 국가',list(nullD_Country))


# (3) 2021년 1년동안 코로나 총 발생자수, 총 사망자수, 일평균 발생자수, 일평균
# 사망자 수를 기준으로 가장 많은 20개 국가를 내림차순으로 정렬하고 총 발생
# 자수, 총 사망자수, 일평균 발생자수, 일평균 사망자 수를 리포트 하시오. (4가
# 지 기준 각각 sorting)
# (난이도: 4, 배점: 30점)

totConfirmed20 = final.sort_values('총확진자',ascending=False).reset_index()
print(totConfirmed20.head(20))
meanConfirmed20 = final.sort_values('일평균 확진자',ascending=False).reset_index()
print(meanConfirmed20.head(20))
totDeaths20 = final.sort_values('총사망자',ascending=False).reset_index()
print(totDeaths20.head(20))
meanDeaths20 = final.sort_values('일평균 사망자',ascending=False).reset_index()
print(meanDeaths20.head(20))

# (4) 2021년 1년동안 대한민국에서 발생한 총 코로나 발생자수와 총 사망자 수
# 와 일평균 발생자수와 일평균 사망자 수를 리포트 하시오.
# (난이도: 3, 배점: 20점)
korea=final.loc['Korea, South']
print(final.loc['Korea, South'])


##최종검증###
#일별로 합친것과 단순히 12월31일값만빼서 구한것과 차이가 있는지 보고
#그 차이가 음수값(오입력)에 의한 것인지 확인
'''
####혹시 소수점 데이터 있는지 확인해 본 코드
# def is_int(n):
#     return n % 1 != 0
# ts=is_int(rs.iloc[:]).sum(axis=1)
# ts.sum()
'''

test1.insert(loc=0,column='Confirmed',value=rs['Confirmed'].sum(axis=1))
test1.insert(loc=1,column='Deaths',value=rs['Deaths'].sum(axis=1))

testsub= test1.sub(test,fill_value=0)

a=((testsub['Confirmed']!=0))
ts1=(a)==True
z=a.index[ts1]
len(a.index[ts1])
print("")
b=((testsub['Deaths']!=0))
ts2=(b)==True
z1=b.index[ts2]
len(b.index[ts2])

####차이는 오입력된 국가뿐 그외 국가는 전부 일치
print("확진자 총 합계 수치가 다른 국가 수:",len(a.index[ts1]),"확진자 음수값 존재 국가 수:",len(missC_Country))
print("사망자 총 합계 수치가 다른 국가 수:",len(b.index[ts2]),"확진자 음수값 존재 국가 수:",len(missD_Country))