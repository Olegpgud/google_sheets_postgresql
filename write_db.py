import httplib2 
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials	
import numpy as np
import psycopg2
import requests
import datetime
import time

CREDENTIALS_FILE = 'mypython-380610-a030b0d138ed.json'  # Имя файла с закрытым ключом

# Читаем ключи из файла
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 
'https://www.googleapis.com/auth/drive'])

httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
 

spreadsheetId='1-_nNFEeXJg9CPT_9DZd4NbqJnnZSY9g7PnlsotGYN4Y' # Id документа




def take_date(date0): # Функция для преобразования формата записи даты
    st=date0.split('.')
    stt=st[2]+'-'+st[1]+'-'+st[0]
    return stt

def write_db(): # Функция для записи данные из таблицы в базу данных
    utc_now = datetime.datetime.utcnow() # Фиксируем время обновления базы данных

    service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API
    ranges = ["Sheet1!A1:D999"] 
    results = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheetId, ranges = ranges, valueRenderOption = 'FORMATTED_VALUE',               dateTimeRenderOption = 'FORMATTED_STRING').execute() # Считываем данные из таблицы
    sheet_values = results['valueRanges'][0]['values']
    # Преобазуем данные в нужный формат
    data=np.array(sheet_values)
    data = np.delete(data,(0), axis = 0)
    l=data.shape[0]
    # Получаем курс USD с ЦБ РФ
    data_usd = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
    USD=data_usd['Valute']['USD']
    usd_rub=float(USD['Value'])

    try:
        # пытаемся подключиться к базе данных
        conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5432/test')
    except:
        print('Can`t establish connection to database')
    cursor = conn.cursor() 
    cursor.execute("""DELETE FROM data""") # Очищаем базу данных
    for i in range(l): # Записываем данные
        if (len(data[i])<4): # Проверка целостности данных для записи в базу
            datadone='false'
            pass
        else:
            datadone='true'
            cursor.execute("""insert into data (id, n, cost_us, data, cost_ru) VALUES (%s,%s,%s,%s,%s)""", (data[i][0],data[i][1],data[i][2],take_date(data[i][3]), (usd_rub*float(data[i][2]))))
    conn.commit()
    conn.close()
    if (datadone=='true'):
        date_time = utc_now.strftime('%Y-%m-%d %H:%M:%S.%f')
        f = open('time.dat', 'w')
        f.write(date_time + '\n')
        f.close()
    else:
        pass

    return 0

def time_chek(): # Функция для проверки нужно ли обновить базу данных
    driveService = apiclient.discovery.build('drive', 'v3', http = httpAuth) # Выбираем работу с Google Drive и 3 версию API
    date_update=driveService.files().get(fileId=spreadsheetId,fields="modifiedTime").execute() # Получаем время последнего редактирования таблицы
    date_update_time=date_update['modifiedTime']
    date_update_time_obj=datetime.datetime.strptime(date_update_time,'%Y-%m-%dT%H:%M:%S.%fZ') # Переводим время в формат datetime
    f = open('time.dat', 'r')
    for line in f:
        base_update_time=line[:-1]
    f.close()

    base_update_time_obj=datetime.datetime.strptime(base_update_time,'%Y-%m-%d %H:%M:%S.%f')

    if ((base_update_time_obj-date_update_time_obj).total_seconds()<180):
        return 'faulse'
    else:  
        return 'true' 

while True:
    a=time_chek()
    if (a=='true'):
        print('database has already been updated')
        pass
    else:
        print('database is being updated...')
        write_db()
    time.sleep(2)

