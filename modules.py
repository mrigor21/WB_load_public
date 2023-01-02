'''
Этот файл содержит полезные функции для работы с регулярными выгрузками для клиентов.
Здесь есть модули для работы с MySQL, Google Sheets, Google BigQuery, Gmail, Google Drive.
Этот файл в счастливом будущем должен стать библиотекой =)
Но пока что он импортируется в скрипты через изменение пути sys =(
'''

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import time
import datetime
import traceback
import json
import smtplib
from email.message import EmailMessage
from google.cloud import bigquery
from loguru import logger
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy import MetaData
import numpy as np


# Декоратор try/except для произвольной функции
def try_func(loading_id, test_mode):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                ans = func(*args, **kwargs)
                return ans
            except Exception:
                error_message = traceback.format_exc()
                logger.exception('Функция не выполнена, возникла ошибка')
                if not test_mode:
                    send_error_email(loading_id, error_message)
        return wrapper
    return decorator


# Декоратор try/except для запроса библиотеки requests
def try_request(expected_response=200, try_amount=5, pause=10, service='Сервис'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    if ans.status_code == expected_response:
                        break
                    else:
                        logger.error(f'Response: {ans.text}')
                        raise Exception(f"Код полученного ответа не соответствует ожидаемому ({expected_response}). Ответ: {ans.text}")
                except Exception as error:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Сервис "{service}" не отвечает, повторная попытка подключения через {pause} секунд')
                        time.sleep(pause)
                    else:
                        logger.error(f'Сервис "{service}" не ответил за предельное количество попыток')
                        raise error
            return ans
        return wrapper
    return decorator


# Декоратор try/except для произвольного
def try_custom_request(try_amount=5, pause=10, service='Сервис'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    break
                except Exception as error:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Сервис "{service}" не отвечает, повторная попытка подключения через {pause} секунд')
                        time.sleep(pause)
                    else:
                        logger.exception(f'Сервис "{service}" не ответил за предельное количество попыток')
                        raise error
            return ans
        return wrapper
    return decorator


# Функция для запроса параметров регулярных загрузок из Гугл-таблицы
def get_loading_parameters(loading_id):
    creds_file = 'producercenter21_registry_creds.json'
    spreadsheet_id = '1-hOSjg9LFy_KVA18glnp_S5Ew2vSiZYQivgpKrt1ANo'
    loadings_table_service = create_gs_service(creds_file)
    google_request = try_custom_request(service='Google Sheets')(
        loadings_table_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='A1:J1000',
        majorDimension='ROWS').execute)
    values = google_request()
    registry = pd.DataFrame(values['values'][1:], columns=values['values'][0])
    index = registry.index[registry.id == str(loading_id)][0]
    loading_parameters = registry.loc[index].squeeze()
    # + 2 потому что индексирование датафрейма начинается с нуля, плюс в таблице
    # с реестром есть шапка. В loading_parameters сохраняем, чтобы к этой строке
    # в этой таблице впоследствии обратилась функция сохранения и записала время окончания
    # выполнения скрипта
    row = index + 2
    loading_parameters['Номер строки в реестре'] = row
    loading_parameters['id таблицы реестра загрузок'] = spreadsheet_id
    json_cols = ['Параметры запуска скрипта', 'Параметры Куда', 'Свойство последнего запуска']
    for col in json_cols:
        if not loading_parameters[col] is None:
            loading_parameters[col] = json.loads(loading_parameters[col])#.replace('\n', '')
    t = datetime.datetime.now()
    t = t - datetime.timedelta(microseconds=t.microsecond) + \
        datetime.timedelta(hours=7)
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {
                'range': f'Лист1!F{row}',
                'values': [[str(t)]]
            }
        ]
    }
    google_request = try_custom_request(service='Google Sheets')(
        loadings_table_service.spreadsheets().values().batchUpdate(
        spreadsheetId=loading_parameters['id таблицы реестра загрузок'],
        body=body).execute)
    google_request()
    return loadings_table_service, loading_parameters


# Функция для конвертации датафрейма в двумерный массив
def df_to_list(df):
    if isinstance(df, pd.DataFrame):
        df.fillna('', inplace=True)
        columns = df.columns.tolist()
        values = df.values.tolist()
        lst = [columns] + values
    elif isinstance(df, list):
        lst = df
    else:
        raise Exception('Данные должны иметь структуру pd.DataFrame или list')
    return lst


# Функция для конвертации списков из API Гугл-таблиц в датафреймы
def lists_to_dfs(lists):
    dfs = []
    for lst in lists:
        if not isinstance(lst, list):
            raise Exception('Данные должны иметь структуру list')
        cols = lst[0]
        if len(lst) > 1:
            data = lst[1:]
            row_mismatch = [len(row) < len(cols) for row in data]
            if all(row_mismatch):
                data[0] = data[0] + [np.nan] * (len(cols) - len(data[0]))
        else:
            data = []
        dfs.append(pd.DataFrame(data, columns=cols))
    return dfs


# Функция для создания движка MySQL
def create_mysql_service(creds_file, return_session=False):
    f = open(creds_file, "r")
    creds = json.loads(f.read())
    f.close()
    conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**creds)
    mysql_service = sqlalchemy.create_engine(conn_str, echo=False)
    if return_session:
        mysql_session = Session(mysql_service)
        return mysql_service, mysql_session
    else:
        return mysql_service


# Функция для запроса нескольких таблиц из MySQL, возврат в датафреймах
def get_mysql_tables(tables, mysql_service):
    dfs = []
    for t in tables:
        query = f'SELECT * FROM {t}'
        df_request = try_custom_request(service='MySQL', try_amount=2)(pd.read_sql)
        df = df_request(query, mysql_service)
        dfs.append(df)
    return dfs


# Функция для формирования движка Гугл-таблиц и Гугл-диска
def create_gs_service(creds_file):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        creds_file, [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
    httpAuth = credentials.authorize(httplib2.Http())
    gs_service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)
    return gs_service


# Функция для формирования движка Гугл-диска
def create_gdrive_service(creds_file):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        creds_file, ['https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    gdrive_service = apiclient.discovery.build('drive', 'v3', http=httpAuth)
    return gdrive_service


# Функция для запроса списка листов Гугл-таблицы
def get_sheets_GS(ssId, gs_service):
    google_request = try_custom_request(service='Google Sheets')(
        gs_service.spreadsheets().get(spreadsheetId=ssId).execute)
    sheets_metadata = google_request()
    sheets = [s['properties'] for s in sheets_metadata['sheets']]
    df = pd.DataFrame(sheets)[['sheetId', 'title']]
    return df


# Функция для считывания данных с листов в Гугл-таблице
def get_gs_tables(ranges, ssId, gs_service):
    # Пример задания параметров скачивания:
    # ranges = [
    #     {
    #         'range': "Паспорт!A1:C",
    #         'rename_cols': {
    #             'Объект': 'objects_id',
    #             'Вид ЕП': 'ep_types_id',
    #             'Название': 'name'
    #         },
    #         'main_required_col': 'Объект',
    #         'add_id_col': True,
    #         'drop_second_row': True
    #     }
    # ]
    google_ranges = [dct['range'] for dct in ranges]
    google_request = try_custom_request(service='Google Sheets', try_amount=3, pause=1)(
        gs_service.spreadsheets().values().batchGet(
        spreadsheetId=ssId,
        ranges=google_ranges,
        majorDimension='ROWS',
        valueRenderOption='UNFORMATTED_VALUE',
        # dateTimeRenderOption='FORMATTED_STRING'
    ).execute) # опционально может быть FORMATTED_VALUE
    values = google_request()['valueRanges']
    tables = [sub['values'] for sub in values]
    dfs = lists_to_dfs(tables)
    for i, (df, params) in enumerate(zip(dfs, ranges)):
        df = df.replace('', np.nan)
        if 'drop_second_row' in params.keys() and not params['drop_second_row'] in [None, False]:
            df.drop(index=0, inplace=True)
        if 'main_required_col' in params.keys() and params['main_required_col'] is not None:
            col = params['main_required_col']
            df_copy = df.copy()
            df_copy[col] = df_copy[col].fillna(method='bfill')
            ind = pd.notna(df_copy[col])
            df = df.loc[ind]
        if 'remain_cols' in params.keys() and params['remain_cols'] is not None:
            df = df[params['remain_cols']]
        if 'rename_cols' in params.keys() and params['rename_cols'] is not None:
            df = df[list(params['rename_cols'].keys())]
            df.rename(columns=params['rename_cols'], inplace=True)
        if 'add_id_col' in params.keys() and not params['add_id_col'] in [None, False]:
            if 'id' not in df.columns:
                df['id'] = range(1, len(df)+1, 1)
            else:
                logger.info(f'В таблице {params["range"]} уже есть колонка "id"')
        dfs[i] = df
    return dfs


# # Функция сохранения датафреймов в MySQL
# def save_to_MySQL(mysql_dfs, mysql_tables_params, session):
#     for df, table_params in zip(mysql_dfs, mysql_tables_params):
#         table_id = table_params['table_id']
#         logger.info(f'Сохранение в таблицу {table_id}')
#         table = meta.tables[table_id]
#         insert_list = df.to_dict(orient='records')
#         for i in range(len(insert_list)):
#             for k, v in insert_list[i].items():
#                 if isinstance(v, (int, float)):
#                     if np.isnan(v):
#                         insert_list[i][k] = None
#         if len(insert_list) != 0:
#             session.execute(table.insert(), insert_list)


# Функция сохранения данных в разные источники в зависимости от json объекта (Параметры Куда)
def save_info_v3(dfs_to_save, loadings_table_service, loading_parameters, fix_time_in_registry=True):
    all_params = loading_parameters['Параметры Куда']
    tables_params = all_params['tables_params']
    if isinstance(loading_parameters, pd.Series):
        if 'Время успешного окончания' in loading_parameters.index:
            has_been_run_earlier = bool(loading_parameters['Время успешного окончания'])
        else:
            has_been_run_earlier = False
    elif isinstance(loading_parameters, dict):
        if 'Время успешного окончания' in loading_parameters.keys():
            has_been_run_earlier = bool(loading_parameters['Время успешного окончания'])
        else:
            has_been_run_earlier = False
    if len(all_params['tables_params']) != len(dfs_to_save):
        raise Exception(f'Количество наборов параметров сохранения \
({len(all_params["tables_params"])}) не совпадает с количеством таблиц ({len(dfs_to_save)}), которые Вы хотите записать!')
    tables_params = all_params['tables_params']
    mysql_ind = ['MySQL' in [s['destination'] for s in table['save_to']] for table in tables_params]
    gs_ind = ['GS' in [s['destination'] for s in table['save_to']] for table in tables_params]
    bq_ind = ['BQ' in [s['destination'] for s in table['save_to']] for table in tables_params]
    if any(mysql_ind):
        mysql_service, session = create_mysql_service(all_params['creds']['MySQL'], return_session=True)
        mysql_tables_params = [
            {**{'rewrite': table['rewrite']}, **s} \
                for table in tables_params \
                    for s in table['save_to'] \
                        if 'MySQL' in s['destination']]
        mysql_rewrite_tables_ids = [t['table_id'] for t in mysql_tables_params if t['rewrite']]
        mysql_dfs = [df for flag, df in zip(mysql_ind, dfs_to_save) if flag]
        meta = MetaData(bind=mysql_service)
        MetaData.reflect(meta)
        with session.begin():
            session.execute = try_custom_request(try_amount=3, service='MySQL')(session.execute)
            session.commit = try_custom_request(try_amount=3, service='MySQL')(session.commit)
            try:
                for table_id in mysql_rewrite_tables_ids[::-1]:
                    logger.info(f'Очистка таблицы {table_id}')
                    table = meta.tables[table_id]
                    session.execute(table.delete())
                    # Сброс автозаполнения идентификатора до единицы можно
                    # раскомментировать только если на 100 % уверен, что ошибки при синхронизации не возникнет,
                    # потому что при наличии запросов на SQL в сессии, откат session.rollback() не сработает.
                    # При необходимости сброса автозаполняемого идентификатора нужно скопировать существующую БД на случай
                    # возникновения ошибок при синхронизации, иначе БД таблицы БД будут очищены безвозвратно.
                    # # session.execute(sqlalchemy.text(f'ALTER TABLE `{table_id}` AUTO_INCREMENT=1').execution_options(autocommit=True))
                for df, table_params in zip(mysql_dfs, mysql_tables_params):
                    table_id = table_params['table_id']
                    logger.info(f'Сохранение в таблицу {table_id}')
                    table = meta.tables[table_id]
                    insert_list = df.to_dict(orient='records')
                    for i in range(len(insert_list)):
                        for k, v in insert_list[i].items():
                            if isinstance(v, (int, float)):
                                if np.isnan(v):
                                    insert_list[i][k] = None
                    if len(insert_list) != 0:
                        session.execute(table.insert(), insert_list)
                session.commit()
            except:
                session.rollback()
                raise Exception('Откат изменений')
    if any(gs_ind):
        gs_service = create_gs_service(all_params['creds']['Google'])
        gs_tables_params = [
            {**{'rewrite': table['rewrite']}, **s} \
                for table in tables_params \
                    for s in table['save_to'] \
                        if 'GS' in s['destination']]
        gs_dfs = [df for flag, df in zip(gs_ind, dfs_to_save) if flag]
        for df, table_params in zip(gs_dfs, gs_tables_params):
            save_to_Google_Sheets_v2(
                df, gs_service,
                table_params, has_been_run_earlier
            )
    if any(bq_ind):
        logger.info('starting BQ')
        bq_service = create_bq_service(all_params['creds']['BQ'])
        bq_tables_params = [
            {**{'rewrite': table['rewrite']}, **s} \
                for table in tables_params \
                    for s in table['save_to'] \
                        if 'BQ' in s['destination']]
        logger.debug(f'bq_tables_params:\n{bq_tables_params}')
        bq_dfs = [df for flag, df in zip(bq_ind, dfs_to_save) if flag]
        logger.debug(f'bq_dfs:\n{bq_dfs}')
        for df, table_params in zip(bq_dfs, bq_tables_params):
            logger.debug(df)
            logger.debug(f'table_params:\n{table_params}')
            save_to_Google_BigQuery(df, bq_service, table_params['rewrite'], table_params['save_to'])

    t = datetime.datetime.now()
    t = t - datetime.timedelta(microseconds=t.microsecond) + \
        datetime.timedelta(hours=7)
    if fix_time_in_registry:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {
                    'range': f'Лист1!G{loading_parameters["Номер строки в реестре"]}',
                    'values': [[str(t)]]
                }
            ]
        }
        google_request = try_custom_request(service='Google Sheets')(
            loadings_table_service.spreadsheets().values().batchUpdate(
            spreadsheetId=loading_parameters['id таблицы реестра загрузок'],
            body=body).execute)
        google_request()
    return str(t)


# Функция сохранения датафрейма в Гугл-таблицы
def save_to_Google_Sheets_v2(df, gs_service, save_params, has_been_run_earlier=False):
    ssId = save_params['ssId']
    rewrite = save_params['rewrite']
    sheetName = save_params['sheetName']
    logger.info(f'Сохранение на лист {sheetName}')
    table = df_to_list(df)
    # logger.debug(table)
    if rewrite == 1:
        table_to_save = table
        insertDataOption = 'OVERWRITE'
        # Вытащим id листа по его названию
        google_request = try_custom_request(service='Google Sheets')(
            gs_service.spreadsheets().get(spreadsheetId=ssId).execute)
        sheet_metadata = google_request()
        sheets = sheet_metadata.get('sheets')
        for s in sheets:
            title = s.get("properties", {}).get("title")
            if title == sheetName:
                sheetId = s.get("properties", {}).get("sheetId")
        if not 'sheetId' in locals():
            raise Exception(f'Лист с именем "{sheetName}" не найден!')
        body = {
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheetId
                        },
                        "fields": "userEnteredValue"
                    }
                }
            ]
        }
        # Очищаем все данные с листа, на который будем производить запись
        google_request = try_custom_request(service='Google Sheets')(
            gs_service.spreadsheets().batchUpdate(spreadsheetId=ssId,
            body=body).execute)
        google_request()
    elif rewrite == 0: # Если присоединяем к имеющимся данным и скрипт до этого
    # выполнялся, то названия колонок присоединять не нужно
        insertDataOption = 'INSERT_ROWS'
        if has_been_run_earlier:
            table_to_save = table[1:]
        else:
            table_to_save = table
    else:
        raise Exception('Параметр "rewrite" должен принимать одно из значений: 0, 1!')
    # Записываем данные в таблицу
    google_request = try_custom_request(service='Google Sheets', try_amount=1)(
        gs_service.spreadsheets().values().append(
        spreadsheetId=ssId,
        range=f'{sheetName}!A1:D1',
        valueInputOption='USER_ENTERED',
        insertDataOption=insertDataOption,
        body={'values': table_to_save,
              "majorDimension": "ROWS"}).execute)
    google_request()


# Функция создания движка Google BigQuery
def create_bq_service(creds_file):
    bq_service = bigquery.Client.from_service_account_json(json_credentials_path=creds_file)
    return bq_service


# Функция сохранения в Google BigQuery
def save_to_Google_BigQuery(df, bq_service, rewrite, save_params):
    table_id = save_params['table_id']
    logger.info(f'Сохранение в таблицу {table_id}')
    job_config = bigquery.LoadJobConfig()
    if rewrite == 1:
        job_config.write_disposition = 'WRITE_TRUNCATE'
    elif rewrite == 0:
        pass
    else:
        raise Exception('Параметр "rewrite" должен принимать одно из значений: 0, 1!')
    google_request = try_custom_request(service='Google BigQuery', pause=1)(
        bq_service.load_table_from_dataframe)
    google_request(df, table_id, job_config=job_config)


# Функция сохранения одного датафрейма в MySQL
def save_to_MySQL(df, mysql_service, save_params):
    table_id = save_params['table_id']
    logger.info(f'Сохранение в таблицу {table_id}')
    save_df_request = try_custom_request(service='MySQL', try_amount=1)(df.to_sql)
    save_df_request(table_id, mysql_service, index=False, if_exists='append')


# Отправка ошибки на почту, если загрузка сломалась
def send_error_email(loading_id, traceback_info):
    logger.error('Отправляется сообщение  на почту')
    try:
        keys_file = 'keys_for_emails.json'
        f = open(keys_file)
        email_keys = json.load(f)
        f.close()
        msg = EmailMessage()
        msg['Subject'] = 'Ошибка при выполнении скрипта загрузки в PythonAnywhere'
        msg['From'] = email_keys['email']
        msg['To'] = email_keys['receivers']
        content = f'''ID загрузки в реестре: {loading_id}
______________________________________

{traceback_info}'''
        msg.set_content(content)
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(email_keys['email'], email_keys['password'])
            smtp.send_message(msg)
        logger.success('Сообщение отправлено')
    except Exception:
        logger.exception('Ошибка при отправке сообщения на почту')


# Стандартный сценарий регулярной загрузки
def run_v2(loading_id, main_load, test_mode, *args, **kwargs):
    try:
        logger.info(f'Началась загрузка с идентификатором {loading_id}')
        loadings_table_service, loading_parameters = get_loading_parameters(loading_id)
        logger.success('Параметры получены, выполняется загрузка')
        dfs = main_load(*args, **kwargs)
        logger.success('Загрузка окончена, сохранение данных')
        end_time = save_info_v3(dfs, loadings_table_service, loading_parameters)
        logger.success('Скрипт выполнен успешно!')
        return end_time
    except Exception:
        error_message = traceback.format_exc()
        logger.exception("Ошибка при выполнении скрипта привела к его остановке")
        if not test_mode:
            send_error_email(loading_id, error_message)



