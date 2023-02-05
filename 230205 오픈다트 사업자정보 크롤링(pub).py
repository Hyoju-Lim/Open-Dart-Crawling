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

# +
# 회사고유번호(corp_code) 데이터 불러오기
api_key = "개인 발급 키"
url = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key="+api_key

# 회사고유번호(corp_num) 파일 압축 해제
with urlopen(url) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall('corp_num')

# xml 파일 가져오기
tree = ET.parse('corp_num/CORPCODE.xml')

# get root node
root = tree.getroot()  

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
listed_comp_info.to_excel("230205_상장기업_3540개_기업개황_eng_columns.xlsx")

# column name 변경 - 한국어
listed_comp_info.columns = ['에러 및 정보코드', '에러 및 정보 메시지', '고유번호', '정식명칭', '영문명칭', '종목명 또는 약식명칭',
                     '주식 종목코드', '대표자명', '법인구분', '법인등록번호', '사업자등록번호', '주소',
                    '홈페이지', 'IR홈페이지', '전화번호', '팩스번호', '업종코드', '설립일', '결산월']

# 한글컬럼명 엑셀 저장
listed_comp_info.to_excel("230205_상장기업_3540개_기업개황_kor_columns.xlsx")

# ---------

# ### 1-1. 기업개황 전처리
# - corp_code : 8자리
# - stock_code : 6자리 로 변경 필요

# +
# ==== [전처리] =========
# Dart api로 추출한 상장회사 기업개황 데이터에서
# (1) corp_code: 8자리, (2) stock_code: 6자리 만들기
listed_comp_info = pd.read_excel('./230205_상장기업_3540개_기업개황_eng_columns.xlsx', index_col=0)

# (1) 8자리 만들기
listed_comp_info['corp_code'] = listed_comp_info['corp_code'].apply(lambda x: "{:0>8d}".format(x))

# (2) 6자리 만들기
listed_comp_info['stock_code'] = listed_comp_info['stock_code'].apply(lambda x: "{:0>6d}".format(x))

# 전처리 잘 됐는지 확인
listed_comp_info[['corp_code', 'stock_code']]
# -

# --------

# ### 2. 업종코드와 한국표준산업분류코드 결합
# - 오픈다트를 통해 조회한 데이터가 어떤 산업군에 속하는지 정보 파악 가능
# - Data 출처:  
# 한국표준산업분류코드 (공공데이터포털)

ind_code = pd.read_csv("./고용노동부_표준산업분류코드_20220802.csv", encoding='cp949')

ind_code.columns

ind_code

# 필요한 컬럼만 추출
df2 = ind_code[['산업분류코드','산업분류명칭']]
# join 위해 key가 될 컬럼의 이름 통일되게 변경
df2.columns = ['induty_code', '산업분류명칭']
df2.head()

# 표준산업분류코드에는 숫자(소분류)와 알파벳(A~U: 대분류) 존재
df2['induty_code'].unique()

# 알파벳코드 빼고 숫자로된 induty_code만 남기기
df3 = df2.iloc[:2001,]
df3.tail()

# 추후 원활한 merge를 위해 산업코드의 데이터타입 변경
df3['induty_code'] = df3['induty_code'].astype(int)

listed_comp_info.columns

listed_comp_info[['corp_code', 'corp_name', 'induty_code']]

# 결합
comp_info_induty = pd.merge(listed_comp_info, df3)
comp_info_induty.head()

# column name 변경 - 한국어
comp_info_induty.columns = ['에러 및 정보코드', '에러 및 정보 메시지', '고유번호', '정식명칭', '영문명칭', '종목명 또는 약식명칭',
                     '주식 종목코드', '대표자명', '법인구분', '법인등록번호', '사업자등록번호', '주소',
                    '홈페이지', 'IR홈페이지', '전화번호', '팩스번호', '업종코드', '설립일', '결산월','산업분류명칭']

# 엑셀로 저장
comp_info_induty.to_excel("230205_상장기업_3540개_산업분류.xlsx")
