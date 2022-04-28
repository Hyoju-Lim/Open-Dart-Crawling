# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# 필요한 모듈
import pandas as pd
import dart_fss as dart
import requests
from io import BytesIO
from zipfile import ZipFile
import xmltodict
from urllib.request import urlopen
import xml.etree.ElementTree as ET
import OpenDartReader
from bs4 import BeautifulSoup 

# -------

# ### 1. 기업개황
# - stock code가 있는 상장기업의 기업 개황 데이터 불러오기
# - 순서:  
# (1) corp_code 전체 불러오기  
# (2) stock_code가 NA가 아닌 회사의 corp_code만 리스트에 추가  
# (3) 이 리스트 이용해서 기업개황 수집하는 반복문 수행  
# (4) 수집된 기업개황을 DF 및 excel로 추출  

# +
# 회사고유번호(corp_code) 데이터 불러오기
api_key = "오픈다트에서 발급 받은 api key값"
url = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key="+api_key

# 회사고유번호(corp_num) 파일 압축 해제
with urlopen(url) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall('corp_num')

# xml 파일 가져오기
tree = ET.parse('corp_num/CORPCODE.xml')

# get root node
root = tree.getroot()  
# -

# 결과 확인 
print(root[0][0].tag)
print(root[0][2].tag)
print(root[956][1].text)

# +
# 상장된 기업의 corp_code만 담는 list
listed_code_list = []

for x in range(len(root)):
    # stock_code가 있는 회사만
    if root[x][2].text is not " ":
        listed_code_list.append(root[x][0].text)

# stock_code 있는 회사 개수
len(listed_code_list)


# +
### 기업개황 불러오는 함수

def load_data(corp_code):
    
    # 기업개황 요청 url
    url = "https://opendart.fss.or.kr/api/company.json?crtfc_key="+api_key+"&corp_code=" + corp_code
    # http 요청
    r = requests.get(url, verify=False)
    company_data = r.json()
        
    # 기업개황 데이터 return
    return company_data


# +
# 반복문 통해 상장된 회사의 기업개황 정보 수집

# 진행상황 표시 모듈
from tqdm import tqdm_notebook

listed_comp_info_list = []

for corp_code in tqdm_notebook(listed_code_list):
    listed_comp_dict = load_data(corp_code)
    listed_comp_info_list.append(listed_comp_dict)

# +
# 상장기업 기업개황 df 변환 및 excel 추출

## 상장기업 기업개황 df로 변환
listed_comp_info = pd.DataFrame.from_dict(listed_comp_info_list)
listed_comp_info.head()
# -

## 상장기업 기업개황 excel로 저장
listed_comp_info.to_excel("220419_상장기업_3431개_기업개황_eng_columns.xlsx")

# column name 변경 - 한국어
listed_comp_info.columns = ['에러 및 정보코드', '에러 및 정보 메시지', '고유번호', '정식명칭', '영문명칭', '종목명 또는 약식명칭',
                     '주식 종목코드', '대표자명', '법인구분', '법인등록번호', '사업자등록번호', '주소',
                    '홈페이지', 'IR홈페이지', '전화번호', '팩스번호', '업종코드', '설립일', '결산월']

# 한글컬럼명 엑셀 저장
listed_comp_info.to_excel("220419_상장기업_3431개_기업개황_kor_columns.xlsx")

# ---------

# ### 1-1. 기업개황 전처리
# - corp_code : 8자리
# - stock_code : 6자리 로 변경 필요

# +
# ==== [전처리] =========
# Dart api로 추출한 상장회사 기업개황 데이터에서
# (1) corp_code: 8자리, (2) stock_code: 6자리 만들기
listed_comp_info = pd.read_excel('./220419_상장기업_3431개_기업개황_eng_columns.xlsx', index_col=0)

# (1) 8자리 만들기
listed_comp_info['corp_code'] = listed_comp_info['corp_code'].apply(lambda x: "{:0>8d}".format(x))

# (2) 6자리 만들기
listed_comp_info['stock_code'] = listed_comp_info['stock_code'].apply(lambda x: "{:0>6d}".format(x))

# 전처리 잘 됐는지 확인
listed_comp_info[['corp_code', 'stock_code']]
# -

# --------

# ### 2. 공시 서류 원본파일 검색 및 사업보고서 추출

# ### (1) 공시자료 검색

# +
# 공시자료 검색 - 보고서 번호 추출
url_json = "https://opendart.fss.or.kr/api/list.json"
api_key
corp_code = "00426086"  # 휴켐스

params = {
    'crtfc_key': api_key,
    'corp_code': corp_code,
    'bgn_de': '20210101',
    'end_de': '20210331'
}

response = requests.get(url_json, params=params)
data = response.json()
data
# -

# DataFrame으로 변환
data_list_hucams = data.get('list')   # list로 만든 후
df_hucams = pd.DataFrame(data_list_hucams)   # df로 변환
df_hucams

# 문서의 제목(보고서명, report_nm)과 접수번호(rcept_no)만 뽑기
# => 공시서류 원본파일 추출에 사용
df_hucams[['report_nm', 'rcept_no']]

# +
# ---- 공시서류원본파일 검색 -------
url = "https://opendart.fss.or.kr/api/document.xml"
api_key
rcept_no = "20210318001017"  # 불러오고자하는 보고서 번호
# 휴켐스 2020.12 사업보고서

params = {
    'crtfc_key': api_key,
    'rcept_no': rcept_no
}

# zip file로 압축해제
import os
doc_zip_path = os.path.abspath('./document_hucams.zip')

if not os.path.isfile(doc_zip_path):
    response = requests.get(url, params=params)
    with open(doc_zip_path, 'wb') as fp:
        fp.write(response.content)

zf = ZipFile(doc_zip_path)
zf.extractall() # 압축 해제하면 경로에 문서번호.xml 파일 생성됨
# -

zf.filelist

# ### (2) xml 형식의 사업보고서 파싱

# +
### xml 파서 이용한 경우
orgpath = './20210318001017.xml'

with open(orgpath) as fp:
    soup = BeautifulSoup(fp, 'html')
    print(soup.prettify())
    
fp.close()
# -

# ### (3) 특정 태그 안의 정보 뽑기 - 연습용

soup.find_all('span')

soup.body

soup.find_all("table")

soup.find_all("p")

# ### (4) 사업보고서 텍스트만 추출
# - 사업보고서 텍스트 중 일부 항목만 추출하는 데 실패
# - instead, 보고서 본문 전체 추출

# html에서 텍스트만 뽑아내기
hucams_doc_text = soup.get_text()
texts = hucams_doc_text.splitlines()
texts

# +
# txt 형식으로 사업보고서 텍스트 저장

textfile = open("휴켐스_2020사업보고서_텍스트.txt", "w")

for element in texts:
    textfile.write(element + '\n')
textfile.close()
# -

# ----------

# ### 3. 재무제표
# - OpenDartReader 패키지 활용   
# - finstate_all() 의 회사명만 바꿔서 넣으면 원하는 기업의 재무제표 조회 가능

# OpenDartReader 패키지 활용을 위한 객체 설정
dart = OpenDartReader(api_key)

# (1) 2020년 SK하닉 전체 재무제표 = 상장기업 재무정보 - '단일회사 전체 재무제표 개발가이드'
SK_fs_df = dart.finstate_all('SK하이닉스', 2020)
SK_fs_df

SK_fs_df.to_excel('./220302_SK하이닉스 2020 재무제표 전체_opendartreader 이용.xlsx')

# -----------

# ### 4. 지분공시
# - OpenDartReader 패키지 활용

# ### (1) 대량보유 상황보고
# - stock_code 이용해서 조회
# - 조회를 희망하는 회사의 stock_code 이용하여 원하는 모든 회사 지분공시 현황 조회 가능

dart.major_shareholders('000660')  # stock_code로 조회하는 방법

# 엑셀로 추출
df_SK_share = dart.major_shareholders('000660')
df_SK_share.to_excel('./220302_SK하이닉스_대량보유 상황보고_eng.xlsx')

# +
# 대량보유 상황보고 df 한글 컬럼명으로 바꾸기
df_SK_share.columns = ['접수번호', '접수일자', '고유번호', '회사명', '보고구분',
                      '대표보고자', '보유주식등의 수', '보유주식등의 증감', '보유비율',
                      '보유비율 증감', '주요체결 주식등의 수', '주요체결 보유비율',
                      '보고사유']

df_SK_share.to_excel('./220302_SK하이닉스_대량보유 상황보고_kor.xlsx')
# -

# ### (2) 임원 및 주요주주 소유보고

df_SK_majorshare = dart.major_shareholders_exec('SK하이닉스')
df_SK_majorshare.to_excel('./220302_SK하이닉스_임원및주요주주 소유보고_eng.xlsx')

# +
# 임원 및 주요주주 소유보고 df 한글 컬럼명으로 바꾸기
df_SK_majorshare.columns = ['접수번호', '접수일자', '고유번호', '회사명', '보고자',
                       '발행 회사 관계 임원(등기여부)', '발행 회사 관계 임원 직위',
                      '발행 회사 관계 주요 주주', '특정 증권 등 소유 수', '특정 증권 등 소유 증감 수',
                      '특정 증권 등 소유 비율', '특정 증권 등 소유 증감 비율']

df_SK_majorshare.to_excel('./220302_SK하이닉스_임원및주요주주 소유보고_kor.xlsx')
