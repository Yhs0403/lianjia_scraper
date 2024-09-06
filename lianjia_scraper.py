import requests
import time
import random
from bs4 import BeautifulSoup
import pymysql

#获取房屋信息
def get_house_info(city_url, city_name):
    session0_header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Host': city_url.split('/')[-2],
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
    }

# 会话构建，首先访问该城市首页url，获取cookies信息
    session0 = requests.session()
    session0.get(url=city_url, headers=session0_header)

# 直接生成一个列表，列表内包含该城市所有待访问的url
    page_url = [city_url, city_url + '/ershoufang'] + [city_url + '/ershoufang/pg{}/'.format(str(i)) for i in range(2, 101)]
    all_house_list = []
    for i in range(1,40):
# 为每一个页面构建不同的Referer信息
        header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Host': city_url.split('/')[-2],
    'Referer': page_url[i - 1],
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
    }
        index_response = session0.get(url=page_url[i], headers=header)

# 有些城市可能没有100页的二手房信息，因此执行完最后一页就需要跳出循环
# 或者没有成功访问页面，返回的状态码不是200，跳出循环
        if index_response.status_code != 200:
            print(city_name, 'page', str(i), 'pass')
            break

        time.sleep(random.uniform(2, 4))
        index_soup = BeautifulSoup(index_response.text, 'lxml')

        try:
            for each_house in index_soup.find_all('li', class_='clear LOGVIEWDATA LOGCLICKDATA'):
                each_house_dict = {
                'house_code': each_house.find('div', class_='title').find('a')['data-housecode'],
                'house_url': each_house.find('div', class_='title').find('a')['href'],
                'house_name': each_house.find('div', class_='title').find('a').get_text(),
                'house_desc': each_house.find('div', class_='houseInfo').get_text().replace(' ', ''),
                'xiaoqu_info': each_house.find('div', class_='positionInfo').get_text().replace(' ', ''), #小区信息
                'house_tag': each_house.find('div', class_='tag').get_text('/'), # 房屋标签
                'house_totalPrice': each_house.find('div', class_='totalPrice').get_text(), # 总价
                'house_unitPrice': each_house.find('div', class_='unitPrice').get_text(), # 单价
                'city': city_name
                }
                all_house_list.append(each_house_dict)
                print(city_name, 'page', str(i), 'done', len(all_house_list))
        except:
            print(city_name, 'done, no other left.')
            break

# 因为发现有些城市可能会没有二手房界面，比如滁州。因此加入一个条件判别，如果没有就跳出循环
        if i > 4 and len(all_house_list) == 0:
            print(city_name, '获取失败')
            break
    return all_house_list

#MySQL中创建表
def create_table_mysql(host,password,database,user='root',charset='utf8'):
    db = pymysql.connect(host=host, user=user, password=password, db=database,charset=charset)
    cursor = db.cursor()
    cursor.execute('DROP TABLE IF EXISTS ljesf') #数据库的游标
    create_table_mysql = '''
    CREATE TABLE ljesf(
    house_code CHAR(30) COMMENT '房屋编号',
    house_url CHAR(100) COMMENT '房屋url',
    house_name CHAR(100) COMMENT '房屋名字',
    house_desc CHAR(100) COMMENT '房屋描述',
    xiaoqu_info CHAR(100) COMMENT '小区描述',
    house_tag CHAR(100) COMMENT '房屋标签',
    house_total_price CHAR(20) COMMENT '总价',
    house_unit_price CHAR(40) COMMENT '单价',
    city CHAR(40) COMMENT '城市'
    )
    '''
    try:
        cursor.execute(create_table_mysql)
        db.commit()
        print('create table done')
    except:
        db.rollback() #如果有误，进行数据回滚
        print('create table not done')
    return db, cursor

#插入到数据库
def insert_into_mysql(db,cursor,all_house_list):
    insert_sql = '''
    INSERT INTO ljesf(house_code, house_url, house_name, house_desc, xiaoqu_info, house_tag, house_total_price, house_unit_price, city)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for each in all_house_list:
        insert_data = [values for key, values in each.items()]
        cursor.execute(insert_sql, insert_data)

    try:
        db.commit()
        print('insert done')
    except:
        db.rollback()
        print('insert not done')

if __name__ == 'main':
    all_house_list = get_house_info(city_url="https://nj.lianjia.com/",city_name="南京") #爬取南京链家的数据，存储在"all_house_list"表中
    db, cursor = create_table_mysql("localhost",'123456',"lianjia") #创建sql表，创建数据库和游标的索引
    insert_into_mysql(db,cursor,all_house_list) #将"all_house_list"中的数据插入sql表中
    print("南京导入",'done') #汇报执行结果
    cursor.close()
    db.close() #关闭游标和数据库

