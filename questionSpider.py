# -*- coding: utf-8 -*-
import re
import time
import requests
import random
import json
import pymysql
import sys
# create table question(
#     pk_id bigint(20) UNSIGNED not null primary key auto_increment comment "主键",
#     question_id bigint UNSIGNED not null comment "问题ID",
#     answer_count mediumint UNSIGNED not null comment "回答数",
#     follower_count mediumint UNSIGNED not null comment "问题关注数",
#     created_time datetime not null comment "问题创建时间"
# )charset=utf8 comment "问题表";

headers = {
    "authority": "www.zhihu.com",
    "method": "POST",
    "scheme": "https",
    "accept-language": "zh-CN,zh;q=0.9",
    "cookie": '_zap=4b8fd0b0-5ece-4710-8a39-4690be3cc915; d_c0="ACDn4-HhLA-PTloTkzkSI1g9NSQ0UNbecnY=|1553490041"; _xsrf=iqWraCpzOAEVVHNZGwDfyaUPzBb7lkuI; z_c0="2|1:0|10:1553513989|4:z_c0|92:Mi4xTHpaZUJBQUFBQUFBSU9majRlRXNEeVlBQUFCZ0FsVk5CUXlHWFFCVjdwTFIwbjFVeXdZWmREdDVybTVvVWtVa0NR|e97ba19d5423a0bb2269441eb310b80853aaed3e4cfdcd555c5b4517e681824d"; __gads=ID=ef86bad0aef0dc13:T=1553514097:S=ALNI_MaIcscAZVawHrwdA_5OzAq3gGMLfg; __utmv=51854390.100-1|2=registration_date=20170314=1^3=entry_date=20170314=1; _ga=GA1.2.1820027566.1554478077; tst=r; q_c1=c09535e464704c7e8aa93032d032f507|1556906107000|1553490042000; __utma=51854390.1820027566.1554478077.1555816095.1556906108.7; __utmz=51854390.1556906108.7.7.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; tgw_l7_route=060f637cd101836814f6c53316f73463',
    "origin": "https://www.zhihu.com",
    "referer": "https://www.zhihu.com/topic",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "x-xsrftoken": "iqWraCpzOAEVVHNZGwDfyaUPzBb7lkuI",
    "_xsrf":"697157726143707a4f41455656484e5a47774466796155507a4262376c6b7549"
}

topicID = []

# 查询数据库中的话题ID
try:
    db = pymysql.connect(host='127.0.0.1',user='',passwd='!123',db='',port=3306,charset='utf8')
    # 检验数据库是否连接成功
    cursor = db.cursor()
    # 这个是执行sql语句，返回的是影响的条数
    sql_select = "SELECT * FROM topic where get_question=0"
    data = cursor.execute(sql_select)
    # 得到一条数据
    # one = cursor.fetchone()
    all = cursor.fetchall()
    for one in all:
        topicID.append(one[1])
except pymysql.Error as e:
    print(e)
    print('操作数据库失败')
finally:
# 如果连接成功就要关闭数据库
    if db.open:
        db.close()

topicSum = len(topicID)
topicCnt = 0
topicID.reverse()
# # tmp = 19
# for i in range(0,tmp):
#     topicID.pop(i)

proxy = requests.get("http://127.0.0.1:5010/get/", timeout=10).json().get("proxy")
db = pymysql.connect(host='127.0.0.1',user='',passwd='',db='',port=3306,charset='utf8')
for i in topicID:
    topicCnt += 1
    newURL = r'http://www.zhihu.com/api/v4/topics/' + str(i) + '/feeds/timeline_question?include=data%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Danswer%29%5D.target.content%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Danswer%29%5D.target.is_normal%2Ccomment_count%2Cvoteup_count%2Ccontent%2Crelevant_info%2Cexcerpt.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Darticle%29%5D.target.content%2Cvoteup_count%2Ccomment_count%2Cvoting%2Cauthor.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Dpeople%29%5D.target.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Canswer_type%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.paid_info%3Bdata%5B%3F%28target.type%3Darticle%29%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Cauthor.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dquestion%29%5D.target.annotation_detail%2Ccomment_count%3B&limit=10&offset=0'

    # 遍历一个话题下的所有问题
    while True:
        try:
            # 增加代理访问
            while True:
                time.sleep(0.5)
                start = time.time()
                html = requests.get(newURL, headers=headers, proxies={"http": "http://{}".format(proxy)}, timeout=10)
                # html = requests.get(newURL, headers=headers)
                print(proxy + ' ' + str(html.status_code))
                if html.ok == True:
                    break
                else:
                    # print("请求异常，状态码为：{code}".format(code = html.status_code))
                    # sys.exit(0)
                    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy), timeout=10)
                    proxy = requests.get("http://127.0.0.1:5010/get/", timeout=10).json().get("proxy")
            # print(html.json()['data']['id'][0])
            # html = requests.get(newURL, headers=headers)
            end1 = time.time()

            cursor = db.cursor()
            # print(html.status_code)
            dataList = html.json()['data']
            for data in dataList:
                questionID = data['target']['id']
                # print(questionID)
                sql_select = "SELECT * FROM question WHERE question_id={questionID}".format(questionID = questionID)
                cursor.execute(sql_select)
                result = cursor.fetchall()
                db.commit()
                if result:
                    # question 表中有该ID，不插入
                    # print("failed insert!")
                    continue
                else:
                    # question 表中无该ID，插入
                    print("Successfully insert!")
                    answerCount = data['target']['answer_count']
                    followerCount = data['target']['follower_count']
                    created = data['target']['created']
                    questionCreatedTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created))
                    
                    sql_insert = "INSERT INTO question(question_id,answer_count,follower_count,created_time) VALUE ({questionID},{answerCount},{followerCount},'{questionCreatedTime}')".format(questionID=questionID,answerCount=answerCount,followerCount=followerCount,questionCreatedTime=questionCreatedTime)
                    cursor.execute(sql_insert)
                    db.commit()
            end2 =time.time()
            print(topicCnt)
            print("get proxy time: {time1:.2f} over time: {time2:.2f}".format(time1 = (end1 - start), time2 = (end2 - start)))
            if html.json()['paging']['is_end']:
                break
            newURL = html.json()["paging"]["next"]
            
        except Exception as e:
            print(e)
            print("topicID = {i}".format(i=i))
            requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy), timeout=10)
            proxy = requests.get("http://127.0.0.1:5010/get/", timeout=10).json().get("proxy")
               
    print("have already collect " + str(topicCnt) + ', and the rest of number is ' + str(topicSum - topicCnt))
    sql_update = "UPDATE topic SET get_question=1 WHERE topic_id = {topicID}".format(topicID = i)
    cursor.execute(sql_update)
    db.commit()
db.close()
