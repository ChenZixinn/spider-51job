import sqlite3


class SqlConn:
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
        self.conn = sqlite3.Connection('data.db')
        self.cursor = self.conn.cursor()

    def create_table(self):
        sql = """CREATE TABLE IF NOT EXISTS job51(
        job_id int ,
        job_name varchar(255) ,
        update_time date ,
        com_name varchar,
        salary varchar ,
        workplace varchar ,
        job_exp varchar ,
        job_edu varchar ,
        job_rent varchar ,
        company_type varchar ,
        company_size varchar ,
        job_welfare varchar ,
        company_ind varchar ,
        job_info varchar ,
        job_type varchar 
        );"""
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e.args)

    def insert(self, data):
        keys = ', '.join(self.fieldnames)
        values = ', '.join(['?'] * len(self.fieldnames))
        sql = 'INSERT INTO job51(%s) values(%s)' % (keys, values)
        print(sql)
        try:
            self.cursor.execute(sql, data)
        except Exception as e:
            print(e.args)
            self.conn.rollback()
        else:
            self.conn.commit()


if __name__ == '__main__':
    s = SqlConn()
    s.create_table()