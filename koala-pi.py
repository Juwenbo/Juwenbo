# -*- coding: utf-8 -*-
"""
Created on 2021-11-1

@author: ju
"""

import os

import datetime
import binascii
import time
import serial
import threading
import requests
import pandas as pd

#################################################
#                serial config
#################################################
port = 'COM3'
baudrate= 460800

#################################################
#                base config
#################################################
data_path = "./data"  # 数据文件夹根目录
log_path = "./log"  # 日志文件夹根目录

log_csv = 'log.csv'  # 文件切片信息记录日志csv
log_log = 'log.log'  # python日志文件

# 测试待写入数据
data = 'aaaaf108007ba6ca0073bfc8007f367a007c1e241faaaaf108007ba8ba0073c166007f386a007c201192' \
       'aaaaf108007bac990073c4b7007f3c54007c23ef98aaaaf108007ba71e0073bfec007f36d2007c1e6b37' \
       'aaaaf108007ba23f0073bbc9007f31f4007c199069aaaaf108007ba7400073c05e007f36eb007c1e8903' \
       'aaaaf108007ba9550073c26a007f38fc007c20a458aaaaf108007ba3f70073bde5007f339d007c1b419e' \
       'aaaaf108007ba2970073bcd3007f322c007c19d64baaaaf108007ba4e00073bedd007f3476007c1c223d'
#################################################
#                api config
#################################################
# url
url = r'http://127.0.0.1:8000/api/v2/sleep/upload_raw_data/'
# 许可id
OPEN_ACCESS_KEY = "a2331JQ0MgUHJvZm13d46a"
# 设备出厂id
device_no = '010101'


def gen_curr_folder(path):
    '''
    判断是否存在文件夹如果不存在则创建为文件夹
    :param path: 数据文件路径
    :return: 数据文件路径
    '''
    if not os.path.exists(path):
        os.makedirs(path)
        return path
    else:
        # print('已有该文件夹无需创建')
        return path


def file_list(file_dir):
    '''
    返回当前文件夹下的文件列表 -- 暂未用到
    :param file_dir:
    :return:
    '''
    for root, dirs, files in os.walk(file_dir):
        return len(files)


def gen_logs_csv(data: dict, log_path, log_name):
    '''
    生成log.csv记录文件--如果没有该文件，创建时创建标题（列名）；如果有则不传标题
    :param data: 待处理文件的信息，dict
    :param log_path: 该记录文件目录
    :param log_name: 该记录文件名称
    :return: .csv
    '''
    df = pd.DataFrame(data)
    if os.path.exists(os.path.join(log_path, log_name)):
        df.to_csv(os.path.join(log_path, log_name), mode='a', index=False, header=False)
    else:
        df.to_csv(os.path.join(log_path, log_name), mode='a', index=False, header=True)


def to_bool(value):
    """
    将传入的类bool参数转换为bool值, 如'False'等都会转换为False返回
    :param value: 类bool变量的参数值, 如'False', 'false', 'True', 'true'等
    :return: bool
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if not value:  # 空字符串
            return False
        if value.lower() == 'false':
            return False
        if value.lower() == 'true':
            return True
    raise Exception(u'Bool值转换异常: %s' % value)


def upload_delete():
    '''
    上传和删除数据文件
    触发式上传：读取log.csv记录文件中的状态码status，并将该条记录信息提取上传
    触发式删除：上传接口调用后返回code，‘10000’为上传成功
    '''
    df = pd.read_csv(os.path.join(log_path, log_csv))
    try:
        # 1、通过状态值找出一个符合的条件文件号seq_no
        index = df[df['status'] == 0]['seq_no'].index.tolist()[0]
    except Exception as e:
        # log.info((time_now))
        print('文件已全部上传')
        return False
    # 2、文件号seq_no反向定位到所在行得到行索引index
    series = df.iloc[index, :]  # series
    # 信息提取
    file_name = series['file_name']
    req_no = series['seq_no']
    # code = series['code']
    is_first = str(series['is_first'])
    is_last = str(series['is_last'])
    time = series['time_now']
    # 3、通过唯一索引--更新该行信息的状态值
    df.loc[df.index == index, 'status'] = 1
    df.to_csv(os.path.join(log_path, log_csv), index=False, header=True)

    data = {
        'key': OPEN_ACCESS_KEY,
        'device_no': device_no,
        'req_no': req_no,
        'generated_time': time,
        'is_first': to_bool(is_first),
        'is_last': to_bool(is_last)
    }
    # 文件对象
    data_file = open(os.path.join(data_path, file_name), 'rb')
    files = {
        'data_file': (os.path.join(data_path, file_name), data_file, 'txt')
    }

    try:
        res = requests.post(url=url, data=data, files=files).json()
    except Exception as e:
        print('未建立Http链接......')
        return False
    # 记得关闭文件！！
    data_file.close()
    if res['code'] == 10000:
        print('文件%s上传成功' % req_no)
        os.remove(os.path.join(data_path, file_name))
        print('文件%s删除成功' % req_no)
    else:
        print('文件上传失败')
        print(res['msg'])


def recvice_slice(folder, size: int, log_path, csv_name):
    '''
    pi接收数据并进行数据切片
    :param folder:数据文件目录
    :param size:每个文件大小（M）
    :param log_path:记录文件目录
    :param csv_name:记录文件名称
    :return:切片后的数据文件
    '''
    num = 0
    while True:
        num += 1
        file_name = r'dec001_%s.txt' % num
        file_path = os.path.join(folder, file_name)
        file = open(file_path, 'a+', encoding='utf-8')
        while os.path.getsize(file_path) < 1024 * 1024 * size:
            n = ser.inWaiting()  # 返回接收缓存中的字节数
            data = ser.read(n)
            data = binascii.b2a_hex(data)  # 二进制ASCII值转化为16进制
            data = data.decode('utf-8')
            file.write(data)
            file.flush()  # 重要！立即刷新缓冲区
        file.close()
        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print('第%s文件生成：' % num + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
        code = 10000
        if num == 1:
            is_first = 'true'
            is_last = 'false'
        elif num != 1 and os.path.getsize(file_path) < 1024 * 1024 * size:
            is_first = 'false'
            is_last = 'true'
        else:
            is_first = 'false'
            is_last = 'false'

        info = {
            "file_name": [file_name],
            "seq_no": [num],
            'code': [code],
            'is_first': [is_first],
            'is_last': [is_last],
            'time_now': [time_now],
            'status': [0]           # 0待上传 1已上传
        }
        try:
            gen_logs_csv(info, log_path, csv_name)
            threading.Thread(target=upload_delete).start()
        except Exception as e:
            print('记录存储错误')


def recvice_slice_test(folder, size: int, log_path, csv_name):
    num = 0
    while True:
        num += 1
        file_name = r'dec001_%s.txt' % num
        file_path = os.path.join(folder, file_name)
        try:
            file = open(file_path, 'a+', encoding='utf-8')
            while os.path.getsize(file_path) < 1024 * 1024 * size:
                file.write(data)
                file.flush()
            file.close()
            time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            print('第%s文件生成：' % num + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            code = 10000
        except Exception as e:
            # log.info('open file failed')
            print('open file failed')
            code = 0
            time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        if num == 1:
            is_first = 'true'
            is_last = 'false'
        elif num != 1 and os.path.getsize(file_path) < 1024 * 1024 * size:
            is_first = 'false'
            is_last = 'true'
        else:
            is_first = 'false'
            is_last = 'false'

        info = {
            "file_name": [file_name],
            "seq_no": [num],
            'code': [code],
            'is_first': [is_first],
            'is_last': [is_last],
            'time_now': [time_now],
            'status': [0]           # 0待上传 1已上传
        }
        try:
            gen_logs_csv(info, log_path, csv_name)
            threading.Thread(target=upload_delete).start()
        except Exception as e:
            print('日志存储错误')


if __name__ == '__main__':
    ###########################
    data_path = gen_curr_folder(data_path)
    log_path = gen_curr_folder(log_path)
    ###########################


    # -----------测试--------------
    print('当前是测试接口....')
    recvice_slice_test(data_path, 1, log_path, log_csv)
    # UploadDelete()


    # ----------有串口pi调试---------
    '''
    *只要蓝牙供电，不管端口号和波特率正确与否,串口处于工作状态
    *工作状态下 端口号和波特率同时匹配状态下，is_open返回True
    '''
    # try:
    #     ser = serial.Serial(port=port, baudrate=baudrate)
    #     ser.flushInput()  # 初始清空一下串口
    # except Exception as e:
    #     print('蓝牙未连接')
    #
    # if ser.is_open:
    #     recvice_slice(data_path, 5, log_path, log_csv)
    #     time.sleep(10)
    # else:
    #     print('请检查端口号和波特率是否正确')

