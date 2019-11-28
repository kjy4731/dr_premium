## Electric Load Preprocessing

# Read CSV File

import pandas as pd
import glob
import os.path
from datetime import datetime
import numpy as np
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


def Data_Preprocessing() :

# 다중 Raw Data 접근하기

    input_path = r'C:\Users\kjh47_000\Desktop\DR_Profile_Tool\raw_data'             #csv파일 이름은 '사업장명_위치명_YYYY(해당년도)_MM(시작월)_MM(마지막월).csv' 로 통일
    output_file = r'C:\Users\kjh47_000\Desktop\DR_Profile_Tool\output.csv'

    all_files = glob.glob(os.path.join(input_path, '*.csv'))                        # 데이터폴더에 있는 모든 파일 읽기

    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    # Pandas Data Frame으로 연산

    data_total = pd.DataFrame()
    #data_total_temp = pd.DataFrame()
    #data = pd.DataFrame()
    #timestamp =pd.DataFrame()
    num_files = 0

    for file in all_files:
        #data_temp = pd.DataFrame()
        #data_time_temp = pd.DataFrame()

        # print(data_total_temp)

        head, tail = os.path.split(file)  # 폴더 경로명(head)와 파일명(tail) 추출하기
        site_name = tail
        site_name = site_name.split("_")
        print(site_name)

        data_temp = pd.read_csv(file)

        data_temp.rename(columns={data_temp.columns[0]: "time"}, inplace=True)  # 'time' 컬럼 이름적기
        data_temp.time.astype(int)
        data_temp['time'] = pd.Series(map(lambda x: '%04d' % x, data_temp['time']))

        data_time_temp = data_temp['time']  # 시간정보만 따로 추출
        data_time_temp[1:96] = data_temp['time'][0:95]          # 한전데이터와는 다르게 1시 15분 ~ 1시 30분 데이터를 나타낼때, 0115로 써야됨
        data_time_temp[0] = '0000'
        del data_temp['time']  # 'time' 컬럼 지우기
        data_temp = pd.melt(data_temp, var_name='date', value_name=site_name[0] + '_E_Load')  # Multi 컬럼을 Single 컬럼으로 2차, variable과 value 이름 결정

        # 데이터 날짜 및 시간 정보 생성
        num_day = int(len(data_temp) / len(data_time_temp))  # 데이터 총 일수
        data_time = pd.DataFrame(pd.concat([data_time_temp] * num_day, ignore_index=True))
        timestamp = pd.DataFrame(data_temp['date'] + ' ' + data_time['time'].apply(str))  # 날짜(date) + 시간(time) 합하기
        timestamp = pd.to_datetime(timestamp.stack(), format="%Y-%m-%d %H%M")  #
        timestamp = pd.DataFrame({'datetime': timestamp.values})
        data_temp['date'] = timestamp['datetime']
        data_temp.rename(columns={data_temp.columns[0]: "datetime"}, inplace=True)

    # 파일 붙이기
        if num_files == 0:
            data_total = data_temp
        else:
            data_total = pd.merge(data_total, data_temp, on='datetime', how='outer')

        num_files += 1

    return data_total

print(Data_Preprocessing())

def Stamp_Date()          #날짜별 평일, 공휴일(+10), 창립기념일(+20), 하계휴가(+30) 구분하기
