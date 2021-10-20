import pandas as pd
import numpy as np
import seaborn as sns
import pandahouse as ph
import logging
import io 
import os
import matplotlib.pyplot as plt
import telegram
from telegram.files.document import Document

sns.set()
dir_path = '/Users/tsyrdugar/Karpov_Courses'

def metrics(chat=None):
    chat_id = 139912570
    token='2069820290:AAHJh4HB8M7e0n9FVxG2dLF5U42RBwrYTwQ'
    bot = telegram.Bot(token)

    message = "Отчет: MAU, Просмотры, Лайки, CTR"
    bot.sendMessage(chat_id=chat_id, message=message)

    # Метрики за предыдущий день 
    connection = {'host':'https://clickhouse.lab.karpov.courses', 
             'password':'dpo_python_2020', 
             'user':'student', 
             'database':'simulator'}
    dau = """
    SELECT toStartOfDay(toDateTime(time)) AS _timestamp,
       count(DISTINCT user_id) AS "Уникальные пользователи"
    FROM simulator.feed_actions
    WHERE toDate(time) = today() - 1
    GROUP BY toStartOfDay(toDateTime(time))
    ORDER BY "Уникальные пользователи" DESC
    LIMIT 50000
    """
    daily_active_users = ph.read_clickhouse(query=dau, connection=connection)

    file_object = io.StringIO()
    daily_active_users.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'daily_active_users.csv'

    bot.sendDocument(chat_id=chat_id, Document=file_object)

    views_likes = """
    SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
       action AS action,
       count(user_id) AS "COUNT(user_id)"
    FROM simulator.feed_actions
    WHERE toDate(time) = today() - 1
    GROUP BY action,
            toStartOfDay(toDateTime(time))
    ORDER BY "COUNT(user_id)" DESC
    LIMIT 50000
    """

    views_likes = ph.read_clickhouse(query=views_likes, connection=connection)

    file_object = io.StringIO()
    views_likes.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'views_likes.csv'

    bot.sendDocument(chat_id=chat_id, Document=file_object)


    ctr = """
    SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
       COUNTIf(user_id, action='like') / COUNTIf(user_id, action='view') AS "CTR"
    FROM simulator.feed_actions
    WHERE toDate(time) = today() - 1
    GROUP BY toStartOfDay(toDateTime(time))
    ORDER BY "CTR" DESC
    LIMIT 50000
    """

    ctr = ph.read_clickhouse(query=ctr, connection=connection)

    file_object = io.StringIO()
    ctr.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'CTR.csv'

    bot.sendDocument(chat_id=chat_id, Document=file_object)



    #Метрики за предыдущие 7 дней 

    dau_7 = """
    SELECT toStartOfDay(toDateTime(time)) AS _timestamp,
       count(DISTINCT user_id) AS "Уникальные пользователи"
    FROM simulator.feed_actions
    WHERE toDate(time) = today() - 7
    GROUP BY toStartOfDay(toDateTime(time))
    ORDER BY "Уникальные пользователи" DESC
    LIMIT 50000
    """
    daily_active_users = ph.read_clickhouse(query=dau_7, connection=connection)

    file_object = io.StringIO()
    daily_active_users.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'daily_active_users.csv'

    bot.sendDocument(chat_id=chat_id, Document=file_object)

    views_likes_7 = """
    SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
       action AS action,
       count(user_id) AS "COUNT(user_id)"
    FROM simulator.feed_actions
    WHERE toDate(time) = today() - 7
    GROUP BY action,
            toStartOfDay(toDateTime(time))
    ORDER BY "COUNT(user_id)" DESC
    LIMIT 50000
    """

    views_likes = ph.read_clickhouse(query=views_likes_7, connection=connection)

    file_object = io.StringIO()
    views_likes.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'views_likes.csv'

    bot.sendDocument(chat_id=chat_id, Document=file_object)


    ctr_7 = """
    SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
       COUNTIf(user_id, action='like') / COUNTIf(user_id, action='view') AS "CTR"
    FROM simulator.feed_actions
    WHERE toDate(time) = today() - 7
    GROUP BY toStartOfDay(toDateTime(time))
    ORDER BY "CTR" DESC
    LIMIT 50000
    """

    ctr = ph.read_clickhouse(query=ctr_7, connection=connection)

    file_object = io.StringIO()
    ctr.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'CTR.csv'

    bot.sendDocument(chat_id=chat_id, Document=file_object)


logging.basicConfig(level='INFO', filename=os.path.join(dir_path, 'logdata.txt'))
now = pd.Timestamp('now')
logging.info('TEST REPORT START BUILDING {}'.format(now))
try: 
    metrics()
    logging.info('TEST REPORT SENT')
except Exception as e:
    logging.exception(e)