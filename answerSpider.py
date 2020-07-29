# -*- coding: utf-8 -*-
import re
import time
import requests
import random
import json
import pymysql

# 回答ID不唯一，经过测试发现在不同问题下会有相同回答ID情况出现

# create table answer(
#     pk_id bigint(20) UNSIGNED not null primary key auto_increment comment "主键",
#     answer_id bigint UNSIGNED not null comment "回答ID",
#     question_id bigint UNSIGNED not null comment "问题ID",
#     upvoters_count mediumint UNSIGNED not null comment "赞同数",
#     comment_count mediumint UNSIGNED not null comment "评论数",
#     created_time datetime not null comment "回答创建时间",
#     is_updated tinyint(1) default 0 comment "是否更新过：0->否，1->是",
#     UNIQUE KEY answerid_questionid (answer_id, question_id)
# )charset=utf8 comment "回答表";

def answerSpider():
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    }
    
    # 每次循环从数据库中拿 1 万个问题出来
    while True:
        databaseFlag = 0
        questionTuple = ()
        try:
            db = pymysql.connect(host='127.0.0.1',user='',passwd='',db='',port=3306,charset='utf8')
            # 检验数据库是否连接成功
            cursor = db.cursor()
            
            # 这个是执行sql语句，返回的是影响的条数
            sql_select = "SELECT question_id FROM question where answer_count<>0 and get_answer=0 limit 10000"
            databaseFlag = cursor.execute(sql_select)
            questionTuple = cursor.fetchall()
            db.commit()
        except pymysql.Error as e:
            print(e)
            print(111)
            print('操作数据库失败')
        finally:
            if db.open:
                db.close()

        # 数据库查询无数据 => 退出
        if databaseFlag == 0:
            break

        try:
            db = pymysql.connect(host='127.0.0.1',user='',passwd='',db='',port=3306,charset='utf8')
            # 检验数据库是否连接成功
            cursor = db.cursor()

            for question in questionTuple:
                url = 'https://www.zhihu.com/api/v4/questions/'+ str(question[0]) + '/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset=0&platform=desktop&sort_by=updated'
                
                # 爬取一个问题下的所有回答
                while True:
                    time.sleep(0.5)
                    html = requests.get(url, headers=headers)
                    dataList = html.json()['data']
                    print(len(dataList))
                    for data in dataList:
                        answer_id = data['id']
                        question_id = question[0]
                        upvoters_count = data['voteup_count']
                        comment_count = data['comment_count']
                        created_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data['created_time']))

                        sql_insert = "insert into answer(answer_id,question_id,upvoters_count,comment_count,created_time) value ({answer_id},{question_id},{upvoters_count},{comment_count},'{created_time}')".format(answer_id = answer_id, question_id = question_id, upvoters_count = upvoters_count, comment_count = comment_count, created_time = created_time)
                        cursor.execute(sql_insert)
                        db.commit()
                    url = html.json()['paging']['next']
                    if html.json()['paging']['is_end'] == True:
                        break

                sql_update = "update question set get_answer=1 WHERE question_id={question_id}".format(question_id = question[0])
                cursor.execute(sql_update)
                db.commit()
                print("question ID :" + str(question_id) + " is over")
        except pymysql.Error as e:
            print(e)
            print(222222)
            print('操作数据库失败')
        finally:
            if db.open:
                db.close()
    

answerSpider()
