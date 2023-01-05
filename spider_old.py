import json
import re
import os
import requests
import pandas as pd
# import sqlite3
import time
from bs4 import BeautifulSoup as bs
# from database import SqlConn


class Job51Crawler:
    job_edu_list = ['初中及以下', '高中', '中技', '中专', '大专', '本科', '硕士', '博士', '无学历要求']
    job_exp_list = ['在校生/应届生', '经验']
    fieldnames = [
        'job_id',
        'job_name',
        'update_time',
        'com_name',
        'salary',
        'workplace',
        'job_exp',
        'job_edu',
        'job_rent',
        'company_type',
        'company_size',
        'job_welfare',
        'company_ind',
        'job_info',
        'job_type',
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Cookie': '_uab_collina=165725228533409208948513; guid=0fc1a4fea43ddc1dcb0986567ce3a3b4; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; 51job=cuid%3D213551816%26%7C%26cusername%3Dif2hPavP7o%252FJ2uCE4%252Bm6Hzi%252FBBr4vtqbv7lpjQMKz3w%253D%26%7C%26cpassword%3D%26%7C%26cname%3D%26%7C%26cemail%3D%26%7C%26cemailstatus%3D0%26%7C%26cnickname%3D%26%7C%26ccry%3D.0hwifkzc3SQA%26%7C%26cconfirmkey%3D%25241%2524MenG%252Fev3%2524A1yv8iob4NNNZgHb5AuOu0%26%7C%26cautologin%3D1%26%7C%26cenglish%3D0%26%7C%26sex%3D%26%7C%26cnamekey%3D%25241%2524CeYo7pRx%2524XCqInROtHikunFfGEVTTZ%252F%26%7C%26to%3D2f29e85a96effca8fdabb415ac11716962d81f4e%26%7C%26; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22213551816%22%2C%22first_id%22%3A%22181dc0cd553b2-00a4587e6b74f03-74492d21-1327104-181dc0cd554e98%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTgxZGMwY2Q1NTNiMi0wMGE0NTg3ZTZiNzRmMDMtNzQ0OTJkMjEtMTMyNzEwNC0xODFkYzBjZDU1NGU5OCIsIiRpZGVudGl0eV9sb2dpbl9pZCI6IjIxMzU1MTgxNiJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22213551816%22%7D%2C%22%24device_id%22%3A%22181dc0cd553b2-00a4587e6b74f03-74492d21-1327104-181dc0cd554e98%22%7D; search=jobarea%7E%60000000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%B4%F3%CA%FD%BE%DD%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch1%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FACFO%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch2%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch3%7E%60010000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21collapse_expansion%7E%601%7C%21; acw_tc=ac11000116675317055275291e00e2f45e149ff939cc583863486f80fa2087; slife=lastlogindate%3D20221104%26%7C%26; partner=51jobhtml5; acw_sc__v3=6364843d1bfc4576f9560e6087f37a9741a35736; ssxmod_itna=Qui=iKY5YK0KGIP0LAncDyWKdDtqjhwE54dD/F3mDnqD=GFDK40ooBvtD7mtnN3ALqITx57OTwT1GCfr+4v0CB0o1FMox0aDbqGk+mnr4GGAxBYDQxAYDGDDPDogPD1D3qDkD7EZlMBsqDEDYp9DA3Di4D+8MQDmqG0DDtHB4G2D7U9Wmd56ubSKi2PFA=DjqTD/fGWcE=oapa1BTrro10TtqGynPGu0uU/lRbDCxtVRk0zzBmqzi4zt7jAFGwtbBx1KD4o8BoKbD+wt7GNIbv4Kk+qjD8DG8x1IGvPeD===; ssxmod_itna2=Qui=iKY5YK0KGIP0LAncDyWKdDtqjhwE54G9QH0FDBMxDItGa830mZWzz7Nmx8OhQz//gkDdhxphCDislzxWY2EQMOpvupPCZWFHp4r/BdOLYPNmahXl8Xl6k9IOciIWmPNR8M7MOxArHaFzn=YNyopCdheCVaFwnO7rVgmh33uxZhya6OBpz9OTwSphuYO4p/gadYPIpzysdBu+oaMay7B4PfodSIkEl9oNqXkAE=cdp2r410RNj+nNu7MNZt1cHr1EHTQn8N=0+VZNyiPntZvg1wQnvXFnBj1IllwMRwUyix1OSMsl7aHRHl0pZ4cxdvDTHg0cnScDMpZQ7V87ym7P6Fp37ge+hUHy0cD1YnlYtzxdmbw9nLtO3VRxSGoD07AeDLxD2zhDD===',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01'
        }
        self.url = 'https://search.51job.com/list/{},000000,0000,00,9,99,{},2,{}.html'

    # 获取该岗位下总共页码数
    def get_total_page(self, code, job):
        url = self.url.format(code, job, 1)
        print(url)
        resp = self.session.get(url,)
        data = json.loads(resp.content.decode('gbk'))
        total_page = int(data['total_page'])
        # total_page = int(re.search('共(\d+)页，到第', resp).group(1))
        return total_page

    def parse_list(self, job, code, city, num=1):
        print('正在爬取%s页' % num)
        url = self.url.format(code, job, num)
        resp = self.session.get(url)
        json_data = resp.json()
        results = json_data.get('engine_jds')
        data_list = []
        for result in results:
            job_id = result.get('jobid')  # jobid
            job_name = result.get('job_name')  # 职位名称
            # if job not in job_name:
            #     continue
            item = dict()
            item['job_id'] = job_id
            item['job_name'] = job_name
            item['update_time'] = result.get('updatedate')  # 发布日期
            item['com_name'] = result.get('company_name')  # 公司名
            item['salary'] = result.get('providesalary_text')  # 薪水
            item['workplace'] = result.get('workarea_text')  # 工作地点
            attribute_text = result.get('attribute_text')
            item['job_exp'] = ''  # 工作经验
            item['job_edu'] = ''  # 学历
            item['job_rent'] = ''  # 招聘人数
            for attr in attribute_text:
                for job_exp in self.job_exp_list:
                    if job_exp in attr:
                        item['job_exp'] = attr.strip()
                for job_edu in self.job_edu_list:
                    if job_edu in attr:
                        item['job_edu'] = attr.strip()
                if '招' in attr and '人' in attr:
                    num = re.findall('(\d+)', attr)
                    if num:
                        item['job_rent'] = num[0]
            item['company_type'] = result.get('companytype_text')  # 公司类型
            item['company_size'] = result.get('companysize_text')  # 公司规模
            item['job_welfare'] = result.get('jobwelf')  # 职位福利
            item['company_ind'] = result.get('companyind_text')  # 所属行业
            job_href = result.get('job_href')
            print(job_href)
            job_items = self.parse_details(job_href, )
            item['job_info'] = job_items.get('job_info')
            item['job_type'] = job_items.get('job_type')

            print(item)
            data_list.append(item)
        file = '%s_%s.csv' % (city, code)
        self.save(data_list, file)
        return data_list

    # 解析详情页的内容
    def parse_details(self, url):
        resp = self.session.get(url, )
        resp.encoding = resp.apparent_encoding
        soup = bs(resp.text, 'lxml')
        jts = soup.select('div.tCompany_main > div.tBorderTop_box')
        job_info = ''
        job_type = ''
        try:
            for jt in jts:
                if jt.select('h2 > span')[0].get_text() == '职位信息':
                    job_info = jt.select('div')[0].get_text()
            job_type = soup.select('p.fp > a')[0].get_text()
        except IndexError:
            pass
        data = {
            'job_info': job_info,
            'job_type': job_type,
        }
        return data

    @staticmethod
    def save(data, file):
        if not os.path.exists(file):
            header = True
        else:
            header = False
        df2 = pd.DataFrame(data)
        df2.to_csv(file, mode='a', index=False, header=header)

    # 任务调度
    def search_city(self, job, code, city, total_page):
        # total_page = self.get_total_page(code, job)
        # 最多爬取前50页
        if total_page > 50:
            total_page = 50
        data_list = []
        for i in range(1, total_page + 1):
            data = self.parse_list(job, code, city, i)
            data_list.extend(data)
        return data_list


if __name__ == '__main__':
    m = Job51Crawler()
    m.search_city('python', '200200', '西安', 20)