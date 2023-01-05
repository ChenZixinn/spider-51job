import json
import os

import requests
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
import pandas as pd


class Job51Crawler:
    def __init__(self):
        # 表头
        self.fieldnames = [
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
        self.j_id = 1

    def search_city(self, job, code, city, page):
        chrome_driver = './plugin/chromedriver'  # chromedriver的文件位置 https://chromedriver.chromium.org/downloads 此处下载
        opts = Options()
        opts.add_argument('--headless')  # 16年之后，chrome给出的解决办法，抢了PhantomJS饭碗
        opts.add_argument('--disable-gpu')
        opts.add_argument('--no-sandbox')  # root用户不加这条会无法运行
        # 设置无头模式，相当于执行了opt.add_argument('--headless')和opt.add_argument('--disable-gpu')(--disable-gpu禁用gpu加速仅windows系统下执行)。
        opts.headless = True
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2
            }
        }
        opts.add_experimental_option('prefs', prefs)
        try:
            self.web = webdriver.Chrome(options=opts, executable_path=chrome_driver)
            self.web.get(f"https://we.51job.com/pc/search?keyword={job}&searchType=2&sortType=0&metro=")
            self.web.implicitly_wait(30)
        except Exception as e:
            print(e)
            return
        # 选择城市
        if city:
            WebDriverWait(self.web, 20).until(
                EC.visibility_of_element_located((By.CLASS_NAME, f'allcity')))
            self.web.find_element(by=By.CLASS_NAME, value="allcity").click()
            for i in self.web.find_elements(by=By.CLASS_NAME, value="el-tabs__item"):
                i.click()

                # time.sleep(1)
                try:
                    WebDriverWait(self.web, 1).until(
                        EC.visibility_of_element_located((By.XPATH, f"//*[text()='{city}']")))
                    self.web.find_element(By.XPATH, f"//*[text()='{city}']").click()
                except Exception as e:
                    pass


            # WebDriverWait(self.web, 10).until(
            #     EC.visibility_of_element_located((By.XPATH, f"//*[text()='{city}']")))
            # self.web.find_element(By.XPATH, f"//*[text()='{city}']").click()

            WebDriverWait(self.web, 20).until(
                EC.visibility_of_element_located((By.XPATH, f'//*[@id="dilog"]/div/div[3]/span/button/span')))
            self.web.find_element(By.XPATH, f'//*[@id="dilog"]/div/div[3]/span/button/span').click()

            WebDriverWait(self.web, 20).until(
                EC.visibility_of_element_located((By.CLASS_NAME, f'btn-next')))

        page_num = int(self.web.find_element(By.XPATH, '//*[@id="app"]/div/div[2]/div/div/div[2]/div/div[2]/div/div[3]/div/div/div/ul/li[last()]').text)
        print(f"page_num:{page_num}")
        all_data_list = []
        if page > page_num:
            page = page_num
        # 翻页操作
        for i in range(page):
            # print(self.json_data)
            # 输出每一页的数据
            all_data_list.extend(self.get_data())
            # 翻页
            self.web.find_element(by=By.CLASS_NAME, value="btn-next").click()
            print(f"page={i}")
        file = './data/%s_%s.csv' % (city, job)
        # 输出到文件
        self.save(all_data_list, file)
        return all_data_list

    def get_data(self):
        WebDriverWait(self.web, 20).until(
            EC.visibility_of_element_located((By.CLASS_NAME, 'j_joblist')))
        job_list = self.web.find_element(by=By.CLASS_NAME, value='j_joblist')
        data_list = []
        # 每一条数据
        for job in job_list.find_elements(By.CLASS_NAME, value="sensors_exposure"):
            # 解析，存到字典中
            item = dict()
            item['job_id'] = self.j_id
            self.j_id += 1
            item['job_name'] = job.find_element(by=By.CLASS_NAME, value="jname").text
            item['update_time'] = job.find_element(by=By.CLASS_NAME, value="time").text  # 发布日期
            item['com_name'] = job.find_element(by=By.CLASS_NAME, value="cname").text  # 发布日期
            item['salary'] = job.find_element(by=By.CLASS_NAME, value="sal").text  # 发布日期
            item['workplace'] = job.find_element(by=By.XPATH, value='//div/a/p[1]/span[2]/span[1]').text  # 发布日期
            item['job_exp'] = job.find_element(by=By.XPATH, value='//div/a/p[1]/span[2]/span[3]').text  # 工作经验
            item['job_edu'] = job.find_element(by=By.XPATH, value='//div/a/p[1]/span[2]/span[5]').text  # 学历
            item['job_rent'] = ''  # 招聘人数
            s = job.find_element(by=By.CLASS_NAME, value="dc").text
            print(f"s:{s}")
            if "|" in s:
                item['company_type'] = s.split("|")[0]  # 公司类型
                item['company_size'] = s.split("|")[1]  # 公司规模
            else:
                item['company_type'] = s  # 公司类型
                item['company_size'] = ""  # 公司规模
            item['job_welfare'] = ""  # 职位福利
            item['company_ind'] = job.find_element(by=By.CLASS_NAME, value="int").text  # 所属行业int
            item['job_info'] = ""
            item['job_type'] = ""
            print(item)
            print("-------------")
            data_list.append(item)
        return data_list

    def save(self, data, file):
        # 判断是否需要表头
        if not os.path.exists(file):
            header = True
        else:
            header = False
        df2 = pd.DataFrame(data)
        # 输出成文件
        df2.to_csv(file, mode='a', index=False, header=header, columns=self.fieldnames)


if __name__ == '__main__':
    spider = Job51Crawler()
    print(spider.search_city("python", 111, "西安", 3))
