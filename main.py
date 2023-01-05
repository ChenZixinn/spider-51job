import json
import os.path
from collections import Counter
import matplotlib
import wordcloud
from threading import Thread
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.pylab import mpl
import pandas as pd
import jieba
from tkinter import *
from tkinter import ttk
from tkinter.messagebox import showinfo
from PIL import ImageTk, Image
from spider import Job51Crawler
from database import SqlConn
import re

matplotlib.use('TkAgg')
mpl.rcParams['font.sans-serif'] = ['SimHei']  # 中文显示
mpl.rcParams["font.family"] = 'Arial Unicode MS'  # 中文显示
# mpl.rcParams['axes.unicode_minus'] = False  # 负号显示

background = '#1ecbf9'
active_background = '#289dff'


class MainUI(Tk):
    def __init__(self):
        super().__init__()
        self.canvas = None
        self.figure = None
        self.name = None
        self.data_list = None
        self.df = None
        self.cleaned = False
        self.w = 800
        self.h = 600
        self.center()
        self.resizable(0, 0)
        self.title('招聘网站数据分析')
        self.job = StringVar()
        self.city = StringVar()
        self.page = StringVar()
        self.tips = StringVar()
        self.j = Job51Crawler()
        self.setup_ui()


    def center(self):
        ww = self.winfo_screenwidth()
        wh = self.winfo_screenheight()
        x = (ww - self.w) / 2
        y = (wh - self.h) / 2
        self.geometry('%dx%d+%d+%d' % (self.w, self.h, x, y))

    def setup_ui(self):
        Label(self, bg='pink').place_configure(x=0, y=0, width=1600, height=80)
        Label(self, text='职位搜索', bg='pink').place_configure(x=30, y=30)
        ttk.Entry(self, textvariable=self.job).place_configure(x=60, y=30)

        Label(self, text='页数', bg='pink').place_configure(x=260, y=30)
        ttk.Entry(self, textvariable=self.page).place_configure(x=290, y=30)

        Label(self, text='城市(默认全国)', bg='pink').place_configure(x=500, y=30)
        ttk.Entry(self, textvariable=self.city).place_configure(x=530, y=30)
        self.search_btn = ttk.Button(self, text='搜索', command=self.search, )
        self.search_btn.place_configure(x=720, y=29)
        Button(self, text='词云', command=lambda: self.show_word_cloud(self.job.get(), self.city.get()),
               bg='pink').place_configure(x=50, y=100, width=100, height=60, )
        Button(self, text='公司规模', command=lambda: self.show_company_size_pie(self.job.get(), self.city.get()),
               bg='pink').place_configure(x=170, y=100, width=100, height=60, )
        Button(self, text='薪资', command=lambda: self.show_salary(self.job.get(), self.city.get()),
               bg='pink').place_configure(x=290, y=100, width=100, height=60, )
        Button(self, text='经验需求', command=lambda: self.show_exp(self.job.get(), self.city.get()),
               bg='pink').place_configure(x=410, y=100, width=100, height=60, )
        Button(self, text='学历要求', command=lambda: self.show_edu(self.job.get(), self.city.get()),
               bg='pink').place_configure(x=530, y=100, width=100, height=60, )
        # Button(self, text='名牌公司', bg='pink').place_configure(x=650, y=100,  width=100, height=60,)
        self.canvas = Label(self, )
        self.canvas.place_configure(x=10, y=180, width=self.w - 20, height=(self.w - 20) // 2)
        Label(self, textvariable=self.tips, foreground='red').place_configure(x=30, y=570, )
        Button(self, text='更新数据库', command=self.submit, bg='pink').place_configure(x=700, y=570, width=100, height=30, )

    def search(self):
        city = self.city.get()
        job = self.job.get()
        try:
            self.df = pd.read_csv(f"./data/{city}_{job}.csv")
            self.tips.set('爬取完毕！')
            self.show_word_cloud(job, city)
            df = self.df
            df = df.fillna(value=0)
            self.data_list = df.values.tolist()
            return
        except Exception as e:
            print(e)
        if not job:
            return showinfo('提示', '请输入职位名称！')
        # with open('city.json', 'r', encoding='utf-8') as f:
        #     cities = json.loads(f.read())
        # code = None
        # stopped = False
        # for k, v in cities.items():
        #     for c in v:
        #         print(c)
        #         print(c['v'])
        #         if c['v'] == city:
        #             code = c['k']
        #             print(code)
        #             stopped = True
        #             break
        #     if stopped:
        #         break
        # if not code:
        #     return showinfo('提示', '没有找到该城市！')

        def _sub_search():
            self.search_btn.config(state='disabled')
            # total_page = self.j.get_total_page(code, job)
            # print('共找到%s页职位信息！' % total_page)
            # if total_page > 50:
            #     total_page = 50
            # data_list = []
            # for i in range(1, total_page + 1):
            #     self.tips.set('正在爬取%s/%s页...' % (i, total_page))
            #     data = self.j.parse_list(job, code, city, i)
            #     data_list.extend(data)
            # self.tips.set('爬取完毕！')
            page = 10
            try:
                p = self.page.get()
                if p:
                    page = int(p)
            except Exception as e:
                page = 10
            self.data_list = self.j.search_city(job,"0",city, page)
            self.tips.set('爬取完毕！')
            df = pd.DataFrame(self.data_list)
            print(df)
            self.search_btn.config(state='normal')
            self.df = df
            self.show_word_cloud(job, city)

        t = Thread(target=_sub_search)
        t.setDaemon(True)
        t.start()

    def show_word_cloud(self, job, city):
        status = self.check_df()
        if not status:
            return
        self.canvas = Label(self, )
        self.canvas.place_configure(x=10, y=180, width=self.w - 20, height=(self.w - 20) // 2)
        job_names = self.df['job_name'].values.tolist()
        words = []
        for job_name in job_names:
            st1 = re.sub('[，。、“”‘ ’]', '', str(job_name))  # 使用正则表达式将符号替换掉。
            words.extend(jieba.lcut(st1))
        content = ' '.join(words)  # 此处分词之间要有空格隔开，联想到英文书写方式，每个单词之间都有一个空格。
        # 导入图片
        w = wordcloud.WordCloud(
            # font_path='C:\\Windows\\Fonts\\STFANGSO.TTF',
            font_path='/System/Library/fonts/PingFang.ttc',
            max_words=2000,
            height=400,
            width=400,
            background_color='white', repeat=False, mode='RGBA')  # 设置词云图对象属性
        con = w.generate(content)
        con.to_image()
        con.to_file('./img/%s_%s.png' % (job, city))  # 保存图片
        image = Image.open('./img/%s_%s.png' % (job, city)).resize((600, 400), Image.ANTIALIAS)
        image_tk = ImageTk.PhotoImage(image=image)
        self.canvas.config(image=image_tk)
        self.canvas.image = image_tk

    # 公司规模饼图
    def show_company_size_pie(self, job, city):
        status = self.check_df()
        if not status:
            return
        self.figure = Figure(figsize=(16, 12), dpi=80, facecolor=active_background, edgecolor='green', frameon=True)
        self.canvas = FigureCanvasTkAgg(self.figure, self)
        self.canvas.get_tk_widget().place_configure(x=10, y=180, width=self.w - 20, height=(self.w - 20) // 2)
        company_sizes = self.df['company_size']
        company_sizes=company_sizes.dropna(axis=0,how='any')
        c1 = Counter(company_sizes).most_common()
        print(f"c1:{c1}")
        x1_list = [i[0] for i in c1]
        y1_list = [j[1] for j in c1]
        fig = self.figure.add_subplot(111)
        self.figure.text(0.35, 0.94, '%s %s 公司规模饼图' % (city, job))
        fig.pie(
            y1_list,
            labels=x1_list,
            autopct='%3.1f%%',  # 数值保留小数位数
            shadow=True,  # 有、无阴影设置
            startangle=90,  # 逆时针起始角度设置
            pctdistance=0.6,  # 标签数字和圆心的距离
            textprops={
                'fontsize': 8,
                'color': '#000080'
            }
        )
        self.canvas.draw()

    # 薪资条形图
    def show_salary(self, job, city):
        status = self.check_df()
        if not status:
            return
        self.figure = Figure(figsize=(16, 12), dpi=80, facecolor=active_background, edgecolor='green', frameon=True)
        self.canvas = FigureCanvasTkAgg(self.figure, self)
        self.canvas.get_tk_widget().place_configure(x=10, y=180, width=self.w - 20, height=(self.w - 20) // 2)
        df = self.df['salary']
        df.dropna(axis=0,how='any')
        raw_salary_list = df.values.tolist()
        salary_list = []
        for raw_salary in raw_salary_list:
            salary = self.format_salary(raw_salary)
            salary_list.append(salary)
        #
        # if not self.cleaned:
        #     df['salary'] = df.apply(lambda x: self.format_salary(x['salary']), axis=1)
        # salary_list = df['salary'].values.tolist()
        print(salary_list)
        salary_range = [0, 0, 0, 0, 0, 0]
        salary_label = ['3k以下', '3-5k', '5-7k', '7-10k', '10-15k', '15k以上']
        for salary in salary_list:
            try:
                s1 = salary.split('-')
                avg_salary = sum([float(s) for s in s1]) / len(s1)
            except Exception as e:
                continue
            if avg_salary < 3:
                salary_range[0] += 1
            elif 3 <= avg_salary < 5:
                salary_range[1] += 1
            elif 5 < avg_salary < 7:
                salary_range[2] += 1
            elif 7 < avg_salary < 10:
                salary_range[3] += 1
            elif 10 < avg_salary < 15:
                salary_range[4] += 1
            else:
                salary_range[5] += 1

        fig = self.figure.add_subplot(111)
        self.figure.text(0.35, 0.94, '%s %s薪资条形图' % (city, job))
        fig.barh(
            salary_label,
            salary_range
        )
        fig.grid(which='major', axis='x', color='gray', linestyle='-', linewidth=0.5, alpha=0.2)  # 设置网格
        fig.legend(loc='lower right', facecolor='pink', frameon=True, shadow=True, framealpha=0.7)
        self.canvas.draw()

    # 经验要求条形图
    def show_exp(self, job, city):
        status = self.check_df()
        if not status:
            return
        self.figure = Figure(figsize=(16, 12), dpi=80, facecolor=active_background, edgecolor='green', frameon=True)
        self.canvas = FigureCanvasTkAgg(self.figure, self)
        self.canvas.get_tk_widget().place_configure(x=10, y=180, width=self.w - 20, height=(self.w - 20) // 2)
        df = self.df
        df.dropna(subset=['job_exp'], inplace=True)
        exp = df['job_exp'].values.tolist()
        c = Counter(exp)
        res = c.most_common()
        labels = [i[0] for i in res]
        values = [i[1] for i in res]
        fig = self.figure.add_subplot(111)
        self.figure.text(0.35, 0.94, '%s %s经验要求条形图' % (city, job))
        fig.bar(
            labels,
            values
        )
        fig.grid(which='major', axis='x', color='gray', linestyle='-', linewidth=0.5, alpha=0.2)  # 设置网格
        fig.legend(loc='lower right', facecolor='pink', frameon=True, shadow=True, framealpha=0.7)
        self.canvas.draw()

    def check_df(self):
        if self.df is None:
            showinfo('提示', '没有数据！')
            return False
        if self.canvas:
            try:
                self.canvas.place_forget()
            except AttributeError:
                self.canvas.get_tk_widget().place_forget()
        return True

    # 学历分布饼图
    def show_edu(self, job, city):
        status = self.check_df()
        if not status:
            return
        self.figure = Figure(figsize=(16, 12), dpi=80, facecolor=active_background, edgecolor='green', frameon=True)
        self.canvas = FigureCanvasTkAgg(self.figure, self)
        self.canvas.get_tk_widget().place_configure(x=10, y=180, width=self.w - 20, height=(self.w - 20) // 2)
        company_sizes = self.df['job_edu']
        c1 = Counter(company_sizes).most_common()
        x1_list = [i[0] for i in c1]
        y1_list = [j[1] for j in c1]
        fig = self.figure.add_subplot(111)
        self.figure.text(0.35, 0.94, '%s %s 学历分布饼图' % (city, job))
        fig.pie(
            y1_list,
            labels=x1_list,
            radius=0.7,
            autopct='%3.1f%%',  # 数值保留小数位数
            shadow=True,  # 有、无阴影设置
            startangle=90,  # 逆时针起始角度设置
            pctdistance=0.6,  # 标签数字和圆心的距离
            wedgeprops={'edgecolor': 'white',
                        'linewidth': 1,
                        'linestyle': '-'
                        },
            textprops={
                'fontsize': 8,
                'color': '#000080'
            }
        )
        self.canvas.draw()

    @staticmethod
    # 格式化薪水
    def format_salary(string):
        try:
            new_salary = ""
            string = str(string)
            if not string:
                return None
            elif '千' in string and '万' in string:
                if '·' in string:
                    new_salary_list = string.split('·')[0].split("-")
                else:
                    new_salary_list = string.split('-')
                new_salary = f'{float(new_salary_list[0].strip("千"))}-{float(new_salary_list[1].strip("万"))*10}'
            elif "千·" in string:
                new_salary_list = string.strip('薪').split('·')[0].split('-')
                new_salary = '-'.join([str(float(i.strip('千'))) for i in new_salary_list])
            elif '千' in string:
                new_salary_list = string.strip('千').split('-')
                new_salary = '-'.join(new_salary_list)
            elif '万/年' in string:
                new_salary_list = string.strip('万/年').split('-')
                new_salary = '-'.join([str(float(i) * 12) for i in new_salary_list])
            elif '万·' in string:
                new_salary_list = string.strip('薪').split('·')[0].split('-')
                new_salary = '-'.join([str(float(i.strip('万')) * 10) for i in new_salary_list])

            elif '万' in string:
                new_salary_list = string.strip('万').split('-')
                new_salary = '-'.join([str(float(i) * 10) for i in new_salary_list])

            elif '元' in string:
                if "元/天" in string:
                    new_salary_list = string.strip('元/天')
                    new_salary = '-'.join([str(float(i) * 30 / 1000) for i in new_salary_list])
                else:
                    new_salary = float(string.split("元")[0])*8*22/1000
            elif "千以下" in string:
                new_salary = string.strip('千以下')
            elif '千' in string:
                new_salary = string.strip('千')
            else:
                new_salary = None
            return new_salary
        except Exception as e:
            print(e)
            print(string)

    def submit(self):
        if self.data_list:
            print(self.data_list)
            s = SqlConn()
            for data in self.data_list:
                s.insert(data)

            return showinfo('提示', '更新完成！')


if __name__ == '__main__':
    m = MainUI()
    m.mainloop()
