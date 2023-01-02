'''
Команда для запуска в bash консоли:
workon regular_loadings_standard_venv && cd /home/user/regular_loadings/ && python metrix/main.py

Этот скрипт запускается на сервере раз в час, чтобы выгрузить актуальную информацию по заказам
продавцов Wildberries и остаткам их товара на складах. Конечной целью является построение main_table - таблица
позаказного учета, где каждый заказ представлен одной строчкой, и в этой строчке собрана информация из всех таблиц,
где фигурирует этот заказ. На ее основе другие сотрудники делали отчеты в PowerBI продавцам из WB по
подписке. Продавцы WB и их данные содержатся в Гугл-таблице. Туда же записываются отметки времени последних
загрузок каждой таблицы. Данные сохраняются в Google BigQuery. Некоторые таблицы должны выгружаться из WB каждый час,
некоторые - раз в день или раз в неделю. Но main_table формируется каждый час и для нее нужны все таблицы,
поэтому часть данных может браться из GBQ. API Wildberries отличается нестабильностью работы, изменением полей
по одним и тем же маршрутам без предупреждения. Часть усложнений кода связана именно с этим.

Пароли, ссылки, файлы учетных записей просто удалены из кода. Они не были встроены в переменное окружение =(
'''


import pandas as pd
from loguru import logger
import datetime
from google.cloud.exceptions import NotFound
import requests
from google.cloud import bigquery
import sys
import pandas_gbq
import warnings
from google.oauth2.service_account import Credentials
# Нужно сделать modules библиотекой, но руки не доходят
sys.path.append('/home/user/regular_loadings')
from modules import *
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None


# Сохранение логов
current_time = pd.Timestamp.now().strftime(format='%Y-%m-%d_%H-%M-%S_%f')
# Имя скрипта можно получать автоматически, но это не работает, если код
# запускается с помощью API, как починить не знаю, поэтому задаю имя вручную
scriptname = 'main'
logger.add(f'metrix/{scriptname}.{current_time}.log')

# Запросы пытаемся делать несколько раз, пока не получится, если API не отвечает
requests.get = try_request(service='WB', try_amount=5, pause=30)(requests.get)
pandas_gbq.to_gbq = try_custom_request(service='Google BQ', try_amount=3)(pandas_gbq.to_gbq)

# Учетные записи
creds = 'metrix/creds_bq.json'
bq_dataset_id = 'some_project.some_dataset'
gs_service = create_gs_service(creds)
bq_service = create_bq_service(creds)
credentials = Credentials.from_service_account_file(creds)
pandas_gbq.context.credentials = credentials


def main_load():
    clients = get_clients()
    for i, client_data in clients.iterrows():
        # Если клиент новый, создаем таблицы для него в GBQ
        create_bq_tables_if_needed(client_data.id)
        logger.info(f'c_{client_data.id} | Обработка началась')
        # Проверяем, первый раз он загружается или нет - от этого зависит сценарий выгрузки
        client_data['first_load_flag'] = clients_first_load(client_data.id)
        if client_data['first_load_flag']:
            # Декоратор с параметрами fix_result_decorator записывает отметку времени выгрузки таблицы WB в Гугл-таблицу с клиентами
            report = fix_result_decorator('report', client_data)(load_and_save_table_WB)('report', client_data)
            stocks = fix_result_decorator('stocks', client_data)(load_and_save_table_WB)('stocks', client_data)
            # Если первая выгрузка то дата для ордерс и сэйлс - минимальная дата репорта
            if report is not None:
                client_data['start_date'] = report.rr_dt.min().tz_convert(None)
        else:
            # Реализация должна обновляться каждый вторник в полночь по UTC, склад - каждую полночь
            report_load_flag, stocks_load_flag = check_if_tables_must_be_updated(client_data.id, client_data.Report_ts, client_data.Stocks_ts)
            client_data['start_date'] = get_actual_part_start_date(report_load_flag, client_data.id)
            if report_load_flag:
                report = fix_result_decorator('report', client_data)(load_and_save_table_WB)('report', client_data)
            else:
                # Если реализация не выгружалась из WB, она должна быть полностью загружена из GBQ
                report = load_report_from_bq(client_data)
            if stocks_load_flag:
                stocks = fix_result_decorator('stocks', client_data)(load_and_save_table_WB)('stocks', client_data)
        # Загрузка заказов и продаж происходит без условий каждый час
        orders = fix_result_decorator('orders', client_data)(load_and_save_table_WB)('orders', client_data)
        sales = fix_result_decorator('sales', client_data)(load_and_save_table_WB)('sales', client_data)
        # Если только что обновили все таблицы за актуальный период (с первого числа прошлого месяца),
        # то формируем актуальную часть мэйна
        if report is not None and sales is not None and orders is not None:
            main = fix_result_decorator('main', client_data)(process_main_table)(
                report, orders, sales, client_data.id, client_data.row_num)
        else:
            main = None
        # Формируем заново весь main если у клиента стоит соответствующий флаг
        if client_data.rewrite_main and not client_data.first_load_flag:
            fix_result_decorator('main_rewrite', client_data)(update_entire_main_table)(client_data, main)
        logger.success(f'c_{client_data.id} | Обработка клиента завершилась')


# Декоратор с параметрами fix_result_decorator записывает отметку времени выгрузки таблицы WB в Гугл-таблицу с клиентами
def fix_result_decorator(table_name, client_data):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                ans = func(*args, **kwargs)
                error_to_write = ''
                return ans
            except Exception as error:
                error_to_write = str(error)
                logger.exception('Ошибка при выполнении')
            finally:
                if error_to_write != '':
                    logger.error(f'c_{client_data.id} | Таблица {table_name} не обработана')
                    logger.error(error_to_write)
                fix_result(table_name, client_data.row_num, error_to_write)
        return wrapper
    return decorator


def fix_result(table_name, row_num, error_to_write):
    global gs_service
    ts_cols = {
        'report': 11,
        'orders': 13,
        'sales': 15,
        'main': 17,
        'stocks': 19,
        'main_rewrite': 21
    }
    col_num = ts_cols[table_name]
    if error_to_write == '':
        t = datetime.datetime.now()
        t = t - datetime.timedelta(microseconds=t.microsecond) + \
            datetime.timedelta(hours=7)
        values = [[str(t), error_to_write]]
        gs_range = f'Клиенты!R{row_num}C{col_num}:R{row_num}C{col_num+1}'
    else:
        values = [[error_to_write]]
        gs_range = f'Клиенты!R{row_num}C{col_num+1}'
    body = {
        "valueInputOption": "USER_ENTERED","data": [{
            'range': gs_range,
            'values': values
        }]
    }
    google_request = try_custom_request(service='Google Sheets', try_amount=3)(
        gs_service.spreadsheets().values().batchUpdate(
        spreadsheetId='1AJR98Bd-Dr9aISu3BsAbsNMLv0kKPwP0rnTptGJUONA',
        body=body).execute)
    google_request()
    logger.success('В таблицу клиентов записан результат выгрузки')


# В таблице с клиентами может стоять флаг о том, что нужно удалить и перезаписать
# весь позаказный учет, тогда мы смотрим на все id заказов в таблице и перестраиваем
# таблицу по 30 000 записей
def update_entire_main_table(client_data, main):
    logger.info(f'c_{client_data.id} | Сценарий | Перезапись таблицы main')
    query = f'''
    SELECT unique_key FROM `{bq_dataset_id}.c_{client_data.id}_orders`
    UNION DISTINCT SELECT unique_key from `{bq_dataset_id}.c_{client_data.id}_sales`
    UNION DISTINCT SELECT unique_key from `{bq_dataset_id}.c_{client_data.id}_report`
    '''
    u_keys = bq_service.query(query)
    u_keys = u_keys.to_dataframe().squeeze()
    if main is not None:
        logger.info(f'c_{client_data.id} | Актуальная часть таблицы main только что обновлена, не учитываем ее при перезаписи')
        u_keys = u_keys.loc[np.logical_not(u_keys.isin(main.unique_key))]
    u_keys = list(u_keys)
    part_size = 30000
    queries_amount = len(u_keys) // part_size + 1
    queries_done = 0
    for i in range(0, len(u_keys), part_size):
        u_keys_part = u_keys[i:i+part_size]
        u_keys_part = str(u_keys_part)[1:-1]
        report, orders, sales = bq_load_report_orders_sales_by_u_keys(client_data.id, u_keys_part)
        main = process_main_table(report, orders, sales, client_data.id, client_data.row_num)
        if main is not None:
            queries_done += 1
            logger.info(f'c_{client_data.id} | Перезапись таблицы main | Обработано частей: {queries_done} из {queries_amount}')
        else:
            raise Exception(f'c_{client_data.id} | При перезаписи main возникла ошибка')
    flag = not (queries_done == queries_amount)
    fix_main_rewrite_result(client_data.id, flag, client_data.row_num)
    logger.success(f'c_{client_data.id} | Таблица main перезаписана полностью')


# Запись отметки времени о перезаписи позаказного учета
def fix_main_rewrite_result(client_id, flag, row_num):
    global gs_service
    col_num = 9
    body = {
        "valueInputOption": "USER_ENTERED", "data": [{
            'range': f'Клиенты!R{row_num}C9',
            'values': [[flag]]
        }]
    }
    google_request = try_custom_request(service='Google Sheets', try_amount=3)(
        gs_service.spreadsheets().values().batchUpdate(
        spreadsheetId='1AJR98Bd-Dr9aISu3BsAbsNMLv0kKPwP0rnTptGJUONA',
        body=body).execute)
    google_request()
    logger.success(f'c_{client_id} | В таблице клиентов исправлена отметка о необходимости перезаписи main')


# Обработка позаказного учета включает в себя его формирование, удаление части таблицы в GBQ, чтобы
# по заказам не было дубликатов и загрузка сформированного учета в GBQ
def process_main_table(report, orders, sales, client_id, row_num, ):
    main = make_main_table(report, orders, sales, client_id)
    delete_actual_part_from_bq('main', client_id, id_list=main.unique_key)
    load_dataframe_to_bq(main, client_id, 'main')
    return main


# Построение позаказного учета
def make_main_table(report, orders, sales, client_id):
    logger.info(f'c_{client_id} | Началось формирование таблицы main')
    orders_sales_start_date = pd.Timestamp.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - pd.Timedelta('14 days')
    orders_14_days = orders.loc[orders.lastChangeDate >= orders_sales_start_date]
    sales_14_days = sales.loc[sales.lastChangeDate >= orders_sales_start_date]
    main = pd.concat([
        report[['unique_key', 'srid', 'rid', 'barcode', 'gi_id']],
        orders_14_days[['unique_key', 'srid', 'odid', 'barcode', 'incomeID']].rename(columns={'incomeID': 'gi_id'}),
        sales_14_days[['unique_key', 'srid', 'odid', 'barcode', 'incomeID']].rename(columns={'incomeID': 'gi_id'})
    ], axis=0, ignore_index=True)
    main.drop_duplicates(subset=['unique_key'], keep='first', inplace=True)
    main[['srid', 'odid', 'rid']] = main[['srid', 'odid', 'rid']].fillna('')
    init_len = len(main)
    logger.debug(f'MAIN LENGTH BEGINNING: {init_len}')

    # Дата заказа
    tmp1 = report[['unique_key', 'order_dt']].groupby('unique_key', as_index=False).agg('min')
    tmp2 = orders_14_days[['unique_key', 'date']].rename(columns={'date': 'order_dt'})
    tmp3 = pd.concat([tmp1, tmp2], axis=0, ignore_index=True)
    # dropna выкидывает даты, где нужное поле не определено,
    # drop_duplicates оставляет в приоритете данные из репорт, т.к. он первый в списке
    # клеющихся фреймов (tmp1) и параметр keep='first'
    field = tmp3.dropna(
        subset=['order_dt']
    ).drop_duplicates(subset=['unique_key'], keep='first')
    main = pd.merge(main, field, how='left', on='unique_key')
    del tmp1, tmp2, tmp3, field

    # Дата покупки / отказа
    tmp1 = report[['unique_key', 'sale_dt']].rename(
        columns={'sale_dt': 'buy_or_cancel_dt'}
    ).loc[report.supplier_oper_name.isin(['Продажа','Корректная продажа'])] # Продажа это ['Продажа','Корректная продажа']
    # # Старый алгоритм
    # _________________
    # tmp2 = report[['unique_key', 'sale_dt']].rename(
    #     columns={'sale_dt': 'buy_or_cancel_dt'}
    # ).loc[
    #     (report.supplier_oper_name == 'Логистика') &
    #     (report.delivery_rub == 33) &
    #     (report.return_amount == 0)
    # ]
    # _________________
    u_keys_with_return_status = report.unique_key.loc[
        report.supplier_oper_name == "Возврат"
    ]
    tmp2 = report[['unique_key', 'sale_dt']].rename(
        columns={'sale_dt': 'buy_or_cancel_dt'}
    ).loc[
        (report.supplier_oper_name == 'Логистика') &
        (report.delivery_rub == 33) &
        (np.logical_not(report.unique_key.isin(u_keys_with_return_status)))
    ]
    tmp3 = orders_14_days[['unique_key', 'cancel_dt']].rename(
        columns={'cancel_dt': 'buy_or_cancel_dt'}
    ).loc[orders_14_days.cancel_dt <= pd.Timestamp(year=2000, month=1, day=1)]
    tmp4 = sales_14_days[['unique_key', 'date']].rename(
        columns={'date': 'buy_or_cancel_dt'}
    ).loc[sales.saleID.str.startswith('S')]
    field = pd.concat([tmp1, tmp2, tmp3, tmp4], axis=0, ignore_index=True).dropna(
        subset=['buy_or_cancel_dt']
    ).drop_duplicates(subset=['unique_key'], keep='first')
    main = pd.merge(main, field, how='left', on='unique_key')
    del tmp1, tmp2, tmp3, tmp4, field

    # Дата возврата return_dt
    tmp1 = report[['unique_key', 'sale_dt']].rename(
        columns={'sale_dt': 'return_dt'}
    ).loc[report.supplier_oper_name == 'Возврат']
    tmp2 = sales_14_days[['unique_key', 'date']].rename(
        columns={'date': 'return_dt'}
    ).loc[sales.saleID.str.startswith('R')]
    field = pd.concat(
        [tmp1, tmp2], axis=0, ignore_index=True
    ).drop_duplicates(subset=['unique_key'], keep='first')
    main = pd.merge(main, field, how='left', on='unique_key')
    del tmp1, tmp2, field

    # Цена заказа order_price
    tmp1 = report[['unique_key', 'retail_price_withdisc_rub']].rename(
        columns={'retail_price_withdisc_rub': 'order_price'}
    ).loc[report.supplier_oper_name.isin(['Продажа','Корректная продажа'])]
    tmp2 = orders_14_days[['unique_key', 'totalPrice', 'discountPercent']]
    tmp2['order_price'] = tmp2.totalPrice * (1 - tmp2.discountPercent/100)
    tmp2.drop(columns=['totalPrice', 'discountPercent'], inplace=True)
    tmp3 = sales_14_days[['unique_key', 'finishedPrice']].rename(
        columns={'finishedPrice': 'order_price'}
    )
    # Добавить ветку с таблицей товаров
    field = pd.concat([tmp1, tmp2, tmp3], axis=0, ignore_index=True).dropna(
        subset=['order_price']
    ).drop_duplicates(subset=['unique_key'], keep='first')
    main = pd.merge(main, field, how='left', on='unique_key')
    del tmp1, tmp2, tmp3, field

    # Цена покупки buy_price, комиссия (%) comission_percent, комиссия (руб) comission_rub
    # FIXME комиссии 0,01 рубля что это такое
    tmp1 = report[['unique_key', 'retail_amount', 'ppvz_for_pay']].rename(
        columns={'retail_amount': 'buy_price', 'ppvz_for_pay': 'smth_to_subtract'}
    ).loc[report.supplier_oper_name.isin(['Продажа','Корректная продажа'])]
    tmp2 = sales_14_days[['unique_key', 'priceWithDisc', 'forPay']].rename(
        columns={'priceWithDisc': 'buy_price', 'forPay': 'smth_to_subtract'}
    ).loc[sales.saleID.str.startswith('S')]
    fields = pd.concat([tmp1, tmp2], axis=0, ignore_index=True).dropna(
        subset=['buy_price']
    ).drop_duplicates(subset=['unique_key'], keep='first')
    fields['comission_rub'] = fields.buy_price - fields.smth_to_subtract
    fields['comission_percent'] = fields.comission_rub * 100 / fields.buy_price
    fields.drop(columns=['smth_to_subtract'], inplace=True)
    main = pd.merge(main, fields, how='left', on='unique_key')
    del tmp1, tmp2, fields

    # Цена возврата return price
    tmp1 = report[['unique_key', 'retail_amount']].rename(
        columns={'retail_amount': 'return_price'}
    ).loc[report.supplier_oper_name == 'Возврат']
    tmp2 = sales_14_days[['unique_key', 'priceWithDisc']].rename(
        columns={'priceWithDisc': 'return_price'}
    ).loc[sales.saleID.str.startswith('R')]
    field = pd.concat([tmp1, tmp2], axis=0, ignore_index=True).dropna(
        subset=['return_price']
    ).drop_duplicates(subset=['unique_key'], keep='first')
    main = pd.merge(main, field, how='left', on='unique_key')
    del tmp1, tmp2, field

    # Логистика return price
    field = report[['unique_key', 'delivery_rub']].loc[report.supplier_oper_name == 'Логистика']
    field = field.groupby('unique_key', as_index=False).agg('sum')
    main = pd.merge(main, field, how='left', on='unique_key')
    del field

    # Бренд, предмет, артикул поставщика, артикул ВБ, размер
    # FIXME добавить из таблицы клиента
    tmp1 = report[[
        'barcode', 'brand_name', 'subject_name', 'sa_name', 'nm_id', 'ts_name'
    ]].rename(columns={
        'brand_name': 'brand', 'subject_name': 'subject',
        'sa_name': 'supplier_article', 'nm_id': 'wb_article',
        'ts_name': 'size'
    })
    tmp2 = orders_14_days[[
        'barcode', 'brand', 'subject', 'supplierArticle', 'nmId', 'techSize'
    ]].rename(columns={
        'supplierArticle': 'supplier_article', 'nmId': 'wb_article',
        'techSize': 'size'
    })
    tmp3 = sales_14_days[[
        'barcode', 'brand', 'subject', 'supplierArticle', 'nmId', 'techSize'
    ]].rename(columns={
        'supplierArticle': 'supplier_article', 'nmId': 'wb_article',
        'techSize': 'size'
    })
    fields = pd.concat([tmp1, tmp2, tmp3], axis=0, ignore_index=True)
    fields = fields.groupby('barcode', as_index=False).first()
    main = pd.merge(main, fields, how='left', on='barcode')
    del fields, tmp1, tmp2, tmp3

    # Регион, страна, округ
    tmp1 = sales[['unique_key', 'regionName', 'countryName', 'oblastOkrugName']].rename(
        columns={'regionName': 'region', 'countryName': 'country', 'oblastOkrugName': 'oblast'}
    )
    tmp2 = orders[['unique_key', 'oblast']]
    fields = pd.concat([tmp1, tmp2], axis=0, ignore_index=True).drop_duplicates(subset=['unique_key'], keep='first')
    main = pd.merge(main, fields, how='left', on='unique_key')
    del fields, tmp1, tmp2

    # Точное время заказа
    field = orders[['unique_key', 'lastChangeDate']].rename(
        columns={'lastChangeDate': 'accurate_time'}
    ).dropna(subset=['accurate_time'])
    main = pd.merge(main, field, how='left', on='unique_key')
    del field

    # Доплаты, штрафы
    fields = report[[
        'unique_key', 'penalty', 'additional_payment'
    ]].groupby('unique_key', as_index=False).agg('sum')
    main = pd.merge(main, fields, how='left', on='unique_key')
    del fields

    # Статус
    fields = main[['unique_key', 'buy_or_cancel_dt', 'buy_price', 'return_price']]
    fields['status'] = 'В пути'
    fields['status'].loc[fields.buy_or_cancel_dt.notna()] = 'Отказ'
    fields['status'].loc[fields.buy_price.notna()] = 'Продажа'
    fields['status'].loc[fields.return_price.notna()] = 'Возврат'
    fields.drop(columns=['buy_or_cancel_dt', 'buy_price', 'return_price'], inplace=True)
    main = pd.merge(main, fields, how='left', on='unique_key')
    main[['comission_rub', 'comission_percent']].loc[main['status'] == 'Возврат'] = 0
    del fields

    main = fix_types_bq(main, 'main')
    end_len = len(main)
    logger.debug(f'MAIN LENGTH ENGING: {end_len}')
    if init_len != end_len:
        # Это проверка правильности построения позаказного учета. В случае ошибок в коде,
        # количество строк в течение формирования увеличится
        raise Exception('Количество строк в сформированной таблице main отличается \
от количества строк = сразу после отбора уникальных заказов. Возможно, при построении появились дубликаты unique_key')
    logger.success(f'c_{client_id} | Таблица main сформирована')
    return main


# Если нужно перестроить позаказный учет полностью, то данные грузим из GBQ, потому что
# Wildberries не хранит данные старше 3 месяцев (на деле иногда бывает есть за 6 месяцев,
# а иногда и за 2 не хватает)
def bq_load_report_orders_sales_by_u_keys(client_id, u_keys_str):
    global bq_service, bq_dataset_id

    table_id_part = f'{bq_dataset_id}.c_{client_id}_'
    tables = []
    for table_name in ['report', 'orders', 'sales']:
        query = f"SELECT * FROM `{table_id_part + table_name}` WHERE unique_key IN ({u_keys_str})"
        df = bq_service.query(query).to_dataframe()
        df = fix_types_bq(df, table_name)
        tables.append(df)
    logger.success(f'c_{client_id} | Получены таблицы, образующие main')
    return tables


# Загрузка таблицы реализации из GBQ
def load_report_from_bq(client_data):
    global bq_service, bq_dataset_id
    table_id = f'{bq_dataset_id}.c_{client_data.id}_report'
    if 'u_keys' not in client_data.index:
        # Ветка отвечает за случай, когда таблица реализации НЕ выгружалась из WB (уже обновлялась на неделе),
        # то есть с первого числа прошлого месяца запрашиваем уникальные ключи, и по ним запрашиваем из BQ строки
        # из реализации
        query = (f"SELECT DISTINCT(unique_key) FROM `{table_id}` WHERE rr_dt >= '{client_data.start_date}'")
        u_keys = bq_service.query(query)
        client_data['u_keys'] = list(u_keys.to_dataframe().squeeze())
        load_until_start_date = False
    else:
        # Ветка отвечает за случай, когда актуальная часть таблицы реализации выгрузилась из WB только что
        # и необходимо дополнить реализацию строками по полученным уникальным ключам с датой меньше актуальной
        load_until_start_date = True
    u_keys = client_data['u_keys']
    part_size = 30000
    queries_amount = len(u_keys) // part_size + 1
    queries_done = 0
    df = pd.DataFrame()
    for i in range(0, len(u_keys), part_size):
        u_keys_part = u_keys[i:i+part_size]
        u_keys_part = str(u_keys_part)[1:-1]
        query = f"SELECT * FROM `{table_id}` WHERE unique_key IN ({u_keys_part})"
        if load_until_start_date:
            query += f" AND rr_dt < '{client_data.start_date}'"
        df_part = bq_service.query(query).to_dataframe()
        df = pd.concat([df, df_part], axis=0, ignore_index=True)
        queries_done += 1
        logger.info(f'c_{client_data.id} | Получение таблицы report из BQ | Выполнено запросов: {queries_done} из {queries_amount}')
    df = fix_types_bq(df, 'report')
    logger.success(f'c_{client_data.id} | Таблица report получена из BQ')
    return df


# Сценарий обработки любой таблицы, кроме позаказного учета
def load_and_save_table_WB(table_name, client_data):
    if 'start_date' not in client_data.index:
        client_data['start_date'] = '2022-01-01 00:00:00'
    df = load_from_wb(table_name, client_data.id, client_data.api_key, client_data.start_date)
    if table_name == 'stocks':
        df.drop_duplicates(subset=['unique_key'], keep='last', inplace=True)
        delete_actual_part_from_bq('stocks', client_data.id, id_list=df.unique_key)
    else:
        delete_actual_part_from_bq(table_name, client_data.id, client_data.start_date)
    load_dataframe_to_bq(df, client_data.id, table_name)
    if table_name == 'report' and not client_data['first_load_flag']:
        logger.info(f'c_{client_data.id} | Выгружается дополнение к таблице {table_name} из BQ')
        client_data['u_keys'] = list(df.unique_key.unique())
        df_bq_part = load_report_from_bq(client_data)
        df = pd.concat([df, df_bq_part], axis=0, ignore_index=True)
    return df


# Выгрузка таблицы из API WB. Параметры запросов отличаются в зависимости от таблицы
def load_from_wb(table, client_id, api_key, date_from):
    global bq_service, bq_dataset_id
    date_from = str(date_from) + 'Z' # Z в конце значит, что время передаем UTC
    logger.info(f'c_{client_id} | Началась загрузка таблицы {table} из API WB начиная с {date_from}')
    base_url = "https://suppliers-stats.wildberries.ru/api/v1/supplier/"
    url_part = table if table != 'report' else 'reportDetailByPeriod'
    params = {'key': api_key, 'dateFrom': date_from}
    if table == 'report':
        params = {**params, **{'dateTo': str(datetime.datetime.today()) + 'Z', 'rrdid': None}}
        df = pd.DataFrame()
        rrd_id = 0
        while True:
            params['rrdid'] = rrd_id
            ans = requests.get(base_url + url_part, params=params).json()
            # logger.debug(ans.text)
            if ans is None:
                break
            df_part = pd.DataFrame(ans)
            rrd_id = df_part.rrd_id.iloc[-1]
            df = pd.concat([df, df_part], axis=0, ignore_index=True)
    else:
        if table in ['orders', 'sales', ]:
            params = {**params, **{'flag': 0}}
        ans = requests.get(base_url + url_part, params=params).json()
        df = pd.DataFrame(ans)
    if df.empty:
        raise Exception('API WB ответил пустым массивом')
    # logger.debug(df)
    df = fix_types_bq(df, table)
    logger.success(f'c_{client_id} | Загружена таблица {table} из API WB начиная с {date_from}')
    return df


# Cохранение в GBQ
def load_dataframe_to_bq(df, client_id, table_name):
    global bq_service, bq_dataset_id
    project_id, dataset_id = bq_dataset_id.split('.')
    destination_table = f'{dataset_id}.c_{client_id}_{table_name}'
    job_config=bigquery.LoadJobConfig()
    df.to_gbq(destination_table, project_id,
        if_exists='append')
    logger.success(f'c_{client_id} | Таблица {table_name} записана в BQ')


# Удаление части данных в GBQ
def delete_actual_part_from_bq(table_name, client_id, start_date=None, id_list=None):
    global bq_service, bq_dataset_id
    table_id = f'{bq_dataset_id}.c_{client_id}_{table_name}'
    if table_name in ['report', 'sales', 'orders']:
        delete_fields = {
            'sales': 'lastChangeDate',
            'orders': 'lastChangeDate',
            'report': 'rr_dt',
        }
        query_delete = f"DELETE FROM `{table_id}` WHERE {delete_fields[table_name]} >= '{start_date}'"
        job = bq_service.query(query_delete)
        job.result()
        msg = f'c_{client_id} | Удалены записи из таблицы {table_id} начиная с {start_date}'
    elif table_name in ['main', 'stocks']:
        id_list = list(id_list.unique())
        part_size = 30000
        queries_amount = len(id_list) // part_size + 1
        queries_done = 0
        # Удаляем по 30 000 исходя из того что максимальная длина запроса в BQ – 1 млн. символов, а длина srid 32 символа
        logger.info(f'c_{client_id} | Началось удаление строк из таблицы {table_name} | Выполнено запросов: 0 из {queries_amount}')
        for i in range(0, len(id_list), part_size):
            id_list_part = id_list[i:i+part_size]
            id_list_part = str(id_list_part)[1:-1]
            query_delete = f"DELETE FROM `{table_id}` WHERE unique_key IN ({id_list_part})"
            job = bq_service.query(query_delete)
            job.result()
            queries_done += 1
            logger.info(f'c_{client_id} | Удаление строк из таблицы {table_name} | Выполнено запросов: {queries_done} из {queries_amount}')
        msg = f'c_{client_id} | Удалены записи из таблицы {table_id} по id, которые будут заменены новыми данными'
    logger.success(msg)



def create_bq_tables_if_needed(client_id):
    global bq_service, bq_dataset_id
    for table_name in ['report', 'orders', 'sales', 'main', 'stocks']:
        table_id = f'{bq_dataset_id}.c_{client_id}_{table_name}'
        try:
            bq_service.get_table(table_id)
        except NotFound:
            create_table(table_id)
    logger.success(f'c_{client_id} | Все таблицы созданы в BQ')


def create_table(table_id):
    global bq_service
    table_name = table_id.split('_')[-1]
    if table_name == 'main':
        schema = [
            bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("srid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("odid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("barcode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("gi_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("order_dt", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("buy_or_cancel_dt", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("return_dt", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("order_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("buy_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("comission_rub", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("comission_percent", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("return_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("delivery_rub", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("subject", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("supplier_article", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("wb_article", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("size", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("oblast", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("accurate_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("penalty", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("additional_payment", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        ]
    elif table_name == 'report':
        schema = [
            bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("srid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("realizationreport_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("suppliercontract_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rrd_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("gi_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subject_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("nm_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("brand_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sa_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ts_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("barcode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("doc_type_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("retail_price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("retail_amount", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("sale_percent", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("commission_percent", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("office_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("supplier_oper_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("order_dt", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("sale_dt", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("rr_dt", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("shk_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("retail_price_withdisc_rub", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("delivery_amount", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("return_amount", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("delivery_rub", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("gi_box_type_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product_discount_for_report", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("supplier_promo", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_spp_prc", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_kvw_prc_base", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_kvw_prc", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_sales_commission", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_for_pay", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_reward", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_vw", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_vw_nds", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_office_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_office_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_supplier_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_supplier_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ppvz_inn", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("declaration_number", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sticker_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("site_country", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("penalty", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("additional_payment", "FLOAT", mode="REQUIRED"),
        ]
    elif table_name == 'orders':
        schema = [
            bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("srid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("odid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("lastChangeDate", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("supplierArticle", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("techSize", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("barcode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("totalPrice", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("discountPercent", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("warehouseName", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("oblast", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("incomeID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("nmId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subject", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("brand", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("isCancel", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("cancel_dt", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("gNumber", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sticker", "STRING", mode="REQUIRED"),
        ]
    elif table_name == 'sales':
        schema = [
            bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("srid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("odid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("lastChangeDate", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("supplierArticle", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("techSize", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("barcode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("totalPrice", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("discountPercent", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("isSupply", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("isRealization", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("promoCodeDiscount", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("warehouseName", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("countryName", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("oblastOkrugName", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("regionName", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("incomeID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("saleID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("spp", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("forPay", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("finishedPrice", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("priceWithDisc", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("nmId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subject", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("brand", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("IsStorno", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("gNumber", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sticker", "STRING", mode="REQUIRED"),
        ]
    elif table_name == 'stocks':
        schema = [
            bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("lastChangeDate", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("supplierArticle", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("techSize", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("barcode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("isSupply", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("isRealization", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("quantityFull", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("quantityNotInOrders", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("warehouse", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("warehouseName", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("inWayToClient", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("inWayFromClient", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("nmId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subject", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("daysOnSite", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("brand", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SCCode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Discount", "FLOAT", mode="REQUIRED"),
        ]
    else:
        raise Exception('Неизвестный тип таблицы')
    bq_service.create_table(bigquery.Table(table_id, schema=schema))  # Make an API request.
    logger.success(f"Таблица {table_id} создана")


# Проверка необходимости загрузки из WB таблиц, которые не должны загружаться каждый час
# в зависимости от отметки времени последней загрузки
def check_if_tables_must_be_updated(client_id, report_ts, stocks_ts):
    now = datetime.datetime.now()
    if pd.isnull(report_ts):
        load_report = True
    else:
        # Логика заключается в том, что таблица должна обновляться каждый вторник в 00:00 по UTC
        report_ts = report_ts.replace(minute=0, second=0, microsecond=0)
        days_to_add = 1 - report_ts.weekday() # 1 - это вторник (дни недели от 0 до 6)
        if days_to_add <= 0:
            days_to_add += 7
        report_ts_next_tue = report_ts + datetime.timedelta(days=days_to_add)
        load_report = now >= report_ts_next_tue
        logger.info(f"c_{client_id} | Сценарий | Необходима выгрузка реализации из WB: {load_report}")
    if pd.isnull(stocks_ts):
        load_stocks = True
    else:
        # Логика заключается в том, что таблица должна обновляться день в 00:00 по UTC
        stocks_next_ts = stocks_ts.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
        load_stocks = now >= stocks_next_ts
        logger.info(f"c_{client_id} | Сценарий | Необходима выгрузка склада из WB: {load_stocks}")
    return load_report, load_stocks


# Загрузка клиентов
def get_clients():
    global gs_service
    ssId = 'some_ssId'
    ranges = [
        {
            'range': 'Клиенты!A1:AG',
            'rename_cols': {
                'ИД': 'id',
                'Ключ API': 'api_key',
                # 'Создать таблицу': 'create_GS',
                # 'Делаем выгрузку в GBQ': 'load_to_GBQ',
                # 'Делаем выгрузку в GS': 'load_to_clients_GS',
                'Перезаписать MAIN полностью': 'rewrite_main',
                'Cсылка на файл': 'GS_link',
                'Report ts': 'Report_ts',
                'Orders ts': 'Orders_ts',
                'Sales ts': 'Sales_ts',
                'Main ts': 'Main_ts',
                'Stocks ts': 'Stocks_ts',
            },
            'main_required_col': 'ИД'
        }
    ]
    clients = get_gs_tables(ranges, ssId, gs_service)[0]
    clients['row_num'] = clients.index + 2
    clients = clients.loc[clients.api_key.notna()]
    clients.id = clients.id.astype(int)
    for col in ['Orders_ts', 'Sales_ts', 'Stocks_ts', 'Report_ts', 'Main_ts']:
        clients[col] = (
            pd.Timestamp('1899-12-30') + pd.to_timedelta(
                clients[col], unit='D'
        ))
    return clients


# Исправление типов данных
def fix_types_bq(df, data_type):
    # remain only needed columns
    if data_type == 'orders':
        list_of_columns = [
            'srid', 'odid',
            'date', 'lastChangeDate', 'supplierArticle', 'techSize',
            'barcode', 'totalPrice', 'discountPercent', 'warehouseName',
            'oblast', 'incomeID', 'nmId', 'subject', 'category',
            'brand', 'isCancel', 'cancel_dt', 'gNumber', 'sticker',
        ]
    elif data_type == 'sales':
        list_of_columns = [
            'srid', 'odid',
            'date', 'lastChangeDate', 'supplierArticle', 'techSize',
            'barcode', 'totalPrice', 'discountPercent', 'isSupply',
            'isRealization', 'promoCodeDiscount', 'warehouseName',
            'countryName', 'oblastOkrugName', 'regionName', 'incomeID',
            'saleID', 'spp', 'forPay', 'finishedPrice',
            'priceWithDisc', 'nmId', 'subject', 'category', 'brand',
            'IsStorno', 'gNumber', 'sticker',
        ]
    elif data_type == 'stocks':
        list_of_columns = [
            # 'srid',
            'lastChangeDate', 'supplierArticle', 'techSize', 'barcode',
            'quantity', 'isSupply', 'isRealization', 'quantityFull',
            'quantityNotInOrders', 'warehouse', 'warehouseName',
            'inWayToClient', 'inWayFromClient', 'nmId', 'subject',
            'category', 'daysOnSite', 'brand', 'SCCode', 'Price', 'Discount'
        ]
    elif data_type == 'report':
        list_of_columns = [
            'srid', 'rid',
            'realizationreport_id', 'suppliercontract_code', 'rrd_id',
            'gi_id', 'subject_name', 'nm_id', 'brand_name', 'sa_name',
            'ts_name', 'barcode', 'doc_type_name', 'quantity', 'retail_price',
            'retail_amount', 'sale_percent', 'commission_percent', 'office_name',
            'supplier_oper_name', 'order_dt', 'sale_dt', 'rr_dt', 'shk_id',
            'retail_price_withdisc_rub', 'delivery_amount', 'return_amount',
            'delivery_rub', 'gi_box_type_name', 'product_discount_for_report',
            'supplier_promo', 'ppvz_spp_prc', 'ppvz_kvw_prc_base',
            'ppvz_kvw_prc', 'ppvz_sales_commission', 'ppvz_for_pay', 'ppvz_reward',
            'ppvz_vw', 'ppvz_vw_nds', 'ppvz_office_id', 'ppvz_office_name',
            'ppvz_supplier_id', 'ppvz_supplier_name', 'ppvz_inn',
            'declaration_number', 'sticker_id', 'site_country', 'penalty', 'additional_payment',
        ]
    else:
        list_of_columns=df.columns # иррациональный код, придумать замену позже
    df = df[list_of_columns]

    # Формирование уникального ключа
    if data_type in ['sales', 'orders']:
        df['unique_key'] = df.srid
        df['unique_key'].loc[(df['unique_key'] == '') | (df['unique_key'].isna())] = df.odid
    elif data_type == 'report':
        df['unique_key'] = df.srid
        df['unique_key'].loc[(df['unique_key'] == '') | (df['unique_key'].isna())] = df.rid
    elif data_type == 'stocks':
        df['unique_key'] = df.barcode.astype(str) + '_' + df.warehouse.astype(str) + '_' + df.isSupply.astype(str) + '_' + df.isRealization.astype(str)

    # int
    if data_type == 'main':
        list_of_columns = []
    elif data_type == 'orders':
        list_of_columns = ['discountPercent', ]
    elif data_type == 'sales':
        list_of_columns = ['IsStorno', ]
    elif data_type == 'stocks':
        list_of_columns = ['quantity', 'quantityFull', 'quantityNotInOrders',
                          'inWayToClient', 'inWayFromClient', 'daysOnSite', ]
    elif data_type == 'report':
        list_of_columns = ['quantity', 'delivery_amount', 'return_amount', 'supplier_promo', 'rrd_id', ]
    for col in list_of_columns:
        df[col] = df[col].fillna(0).astype(int)

    # float
    if data_type == 'main':
        list_of_columns = [
            'additional_payment', 'penalty', 'delivery_rub', 'return_price',
            'comission_percent', 'comission_rub', 'buy_price', 'order_price'
        ]
    elif data_type == 'orders':
        list_of_columns = ['totalPrice', ]
    elif data_type == 'sales':
        list_of_columns = ['totalPrice', 'discountPercent', 'promoCodeDiscount', 'spp',
                          'forPay', 'finishedPrice', 'priceWithDisc', ]
    elif data_type == 'stocks':
        list_of_columns = ['Price', 'Discount', ]
    elif data_type == 'report':
        list_of_columns = ['retail_price', 'retail_amount', 'sale_percent', 'commission_percent',
                          'retail_price_withdisc_rub', 'delivery_rub', 'product_discount_for_report',
                          'ppvz_spp_prc', 'ppvz_kvw_prc_base', 'ppvz_kvw_prc', 'ppvz_sales_commission',
                          'ppvz_for_pay', 'ppvz_reward', 'ppvz_vw', 'ppvz_vw_nds', 'penalty', 'additional_payment', ]
    for col in list_of_columns:
        df[col] = df[col].astype(str).str.replace(',', '.', regex=False).astype(float).round(2)
    # str
    if data_type == 'main':
        list_of_columns = [
            'unique_key', 'srid', 'rid', 'barcode', 'gi_id', 'brand', 'subject', 'supplier_article', 'wb_article',
            'size', 'region', 'country', 'oblast', 'status'
        ]
        # list_of_columns = []
    elif data_type == 'orders':
        list_of_columns = [

            'supplierArticle', 'techSize', 'barcode', 'warehouseName', 'oblast',
            'subject', 'category', 'brand', 'gNumber', 'sticker',
            'incomeID', 'odid', 'nmId', 'unique_key'
        ]
    elif data_type == 'sales':
        list_of_columns = [
            'unique_key', 'srid', 'odid',
            'supplierArticle', 'techSize', 'barcode', 'warehouseName', 'countryName',
            'oblastOkrugName', 'regionName', 'saleID', 'subject', 'category', 'brand',
            'gNumber', 'sticker', 'odid', 'nmId', 'incomeID', 'unique_key'
        ]
    elif data_type == 'stocks':
        list_of_columns = [
            'supplierArticle', 'techSize', 'barcode', 'warehouseName', 'subject',
            'category', 'brand', 'SCCode', 'warehouse', 'nmId',
        ]
    elif data_type == 'report':
        list_of_columns = [
            'unique_key', 'srid', 'rid',
            'suppliercontract_code', 'subject_name', 'brand_name', 'sa_name', 'ts_name',
            'barcode', 'doc_type_name', 'office_name', 'supplier_oper_name', 'gi_box_type_name',
            'ppvz_office_name', 'ppvz_supplier_name', 'ppvz_inn', 'declaration_number',
            'sticker_id', 'site_country', 'sticker_id', 'site_country',
            'realizationreport_id', 'gi_id', 'nm_id',
            'shk_id', 'ppvz_office_id', 'ppvz_supplier_id',
        ]
    for col in list_of_columns:
        df[col] = df[col].astype(str)

    # timestamp
    if data_type == 'main':
        list_of_columns = [
            'order_dt', 'buy_or_cancel_dt', 'return_dt',
            'accurate_time'
        ]
    elif data_type == 'orders':
        list_of_columns = ['cancel_dt', 'date', 'lastChangeDate', ]
    elif data_type == 'sales':
        list_of_columns = ['date', 'lastChangeDate', ]
    elif data_type == 'stocks':
        list_of_columns = ['lastChangeDate', ]
    elif data_type == 'report':
        list_of_columns = ['order_dt', 'sale_dt', 'rr_dt']
    for col in list_of_columns:
        # df[col] = df[col].astype(str).str.strip('+00:00')
        df[col] = pd.to_datetime(df[col], errors = 'coerce')
        df[col] = df[col].apply(lambda t: t.tz_convert(None) if t.tzinfo is not None else t)
    # bool
    if data_type == 'main':
        list_of_columns = []
    elif data_type == 'orders':
        list_of_columns = ['isCancel', ]
    elif data_type == 'sales':
        list_of_columns = ['isSupply', 'isRealization', ]
    elif data_type == 'stocks':
        list_of_columns = ['isSupply', 'isRealization', ]
    elif data_type == 'report':
        list_of_columns = []
    for col in list_of_columns:
        df[col] = df[col].astype(bool)
    return df


# Проверка, была ли раньше хотя бы одна выгрузка у клиента
def clients_first_load(client_id):
    global bq_service, bq_dataset_id
    try:
        report_table_id = bq_dataset_id + '.c_' + str(client_id) + '_report'
        bq_service.get_table(report_table_id)
        query = f"SELECT COUNT(*) FROM `{report_table_id}`"
        total_rows = [row[0] for row in bq_service.query(query).result()][0]
        if total_rows > 0:
            ans = False
        else:
            ans = True
    except NotFound:
        ans = True
    finally:
        logger.info(f"c_{client_id} | Сценарий | Первичная выгрузка: {ans}")
        return ans


# В зависимости от отметок времени последних выгрузок таблиц и текущей выгрузки
# таблицы реализации, вычисляется дата начала загрузки из WB
def get_actual_part_start_date(report_load_flag, client_id):
    global bq_service, bq_dataset_id
    today = datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0)
    # Первое число предыдущего месяца
    start_date = (today - datetime.timedelta(days=31)).replace(day=1)
    if report_load_flag:
        # если грузим репорт то дата начала - минимум из первого числа прошлого месяца
        # и макс. даты репорта
        report_table_id = bq_dataset_id + '.c_' + str(client_id) + '_report'
        query = (f"SELECT MAX(rr_dt) FROM `{report_table_id}`")
        report_max_date = [row[0] for row in bq_service.query(query).result()][0].replace(tzinfo=None)
        start_date = min([start_date, report_max_date])
    logger.info(f'c_{client_id} | Начинаем выгрузку с даты: {start_date}')
    return start_date


if __name__ == '__main__':
    main_load()







