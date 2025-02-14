import numpy as np
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
class NewData():
    def __init__(self):
        """Инициализируем переменные ссылки на API, путь к документу NPS, столбцах, получаемых из API, а также
        пути сохранения для файлов(в будущем должно заливаться в бд.
        Итог_ITOG_Проверка содержит все столбцы и используется для сверки
        Итог_ITOG итоговый файл, где будут находиться все столбцы с баллами, датой и номером магазина"""
        self.url = 'http://10.32.2.81:2530/api/v1/alert/group_data'
        self.path_NPS = 'https://docs.google.com/spreadsheets/d/1KhHSsAqLHVYwvFjnNianFZcK580zB8Z5HnHBlx8zwW8/export?exportFormat=csv'
        self.list_columns_data = ['proceedsYoYPercent', 'checkYoYPercent',
                                  'avgCheckYoYPercent', 'writeOffPercent',
                                  'appLoyalPercent', 'onlineStoreSharePercent']
        self.path_save = 'Итог_ITOG_Проверка'
        self.name_2 = 'Итог_ITOG'


    def first_run(self, start_data):
        """Ф-ия запускается только при первом использовании, когда нет ещё анализа, баллов и данных за всё время"""
        #Получаем данные за нынешний неполный месяц. Тут не сильно важен. Пригодится при ежедневном и т.д. запуске
        self.now_month = self._get_data_for_last_month()
        #Получаем все старые данные до нынешнего месяца
        self._take_past_data(start_data, self.date_now_month)
        #Соединяем и старые и новые денные
        self.all_data = self._get_all_data()
        #Добавляем столбцы баллов _NPS
        self.full_data = self._NPS(self.all_data)
        #Запускаем получение баллов за все столбцы
        self.itog = self._take_point(self.full_data)
        #Сохраняем данные. Позже должна быть база данных
        print(self.itog)
        print(self.itog[self.itog['idStore'] == '42036'])
        quit()
        load_dotenv()
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.database = os.getenv('DATABASE_NAME')
        self.user = os.getenv('LOGIN')
        self.password = os.getenv('PASS')

        # Выполняем SQL-запрос для получения данных
        # Создаем строку подключения
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        # Запись DataFrame в таблицу
        list = ['id_store', 'order_date', 'nps_points','checkyoy_points','proceedsyoy_points', 'avgcheckyoy_point',  'apployal_points',
                'writeoff_points',  'onlinestoreshare_points']
        self.itog.columns = self.itog.columns.str.lower()


        self.itog.rename(columns={'idstore': 'id_store'}, inplace=True)
        self.itog = self.itog[list]
        self.itog['id_store'] = self.itog['id_store'].astype(int)
        print(self.itog.dtypes)
        self.itog.to_sql('rating_emp', con=connection_string, if_exists='append', index=False)
        # Создаем engine
        print("ВСЁ!")

    def not_first_run(self):
        df = self._get_data_for_last_month()
        df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')
        now_month = pd.Timestamp.now()
        now_month = now_month.replace(day=1)

        # Фильтрация данных



        self.now_month = self._get_data_for_last_month()
        self.now_month = self._NPS(self.now_month)
        self.now_month = self._take_point(self.now_month)
        print("Аза-за")
        print(self.now_month)
        print("Азазель")
        self.now_month['idStore'] = self.now_month['idStore'].astype(int)

        print(self.now_month[self.now_month['idStore'] == 42036])
        #self.all_data = pd.concat([self.now_month, past_df], axis=0)
        self.all_data = self.now_month
        print(self.all_data[self.all_data['idStore'] == 42036])
        self.all_data['order_date'] = pd.to_datetime(self.all_data['order_date'], format='%Y-%m-%d')
        print(self.all_data[self.all_data['idStore'] == 42036])
        load_dotenv()
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.database = os.getenv('DATABASE_NAME')
        self.user = os.getenv('LOGIN')
        self.password = os.getenv('PASS')

        # Выполняем SQL-запрос для получения данных
        # Создаем строку подключения
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        engine = create_engine(connection_string)
        if now_month == 1:
            self.itog.to_sql('rating_emp', con=connection_string, if_exists='append', index=False)
            quit()
        # Список нужных столбцов
        list = ['id_store', 'order_date', 'nps_points', 'checkyoy_points', 'proceedsyoy_points', 'avgcheckyoy_point',
                'apployal_points',
                'writeoff_points', 'onlinestoreshare_points']
        # В базе данных столбцы с нижним регистром, делаем и здесь
        self.all_data.columns = self.all_data.columns.str.lower()
        # Переименовываем к виду, как в базе данных
        self.all_data.rename(columns={'idstore': 'id_store'}, inplace=True)
        # Оставляем только нужные столбцы
        self.all_data = self.all_data[list]
        # Делаем столбец целочисленным
        self.all_data['id_store'] = self.all_data['id_store'].astype(int)
        with engine.begin() as connection:
            for index, row in self.all_data.iterrows():
                # Обновление данных в базе данных
                sql = text("""
                    UPDATE rating_emp
                    SET nps_points = :nps_points,
                        checkyoy_points = :checkyoy_points,
                        proceedsyoy_points = :proceedsyoy_points,
                        avgcheckyoy_point = :avgcheckyoy_point,
                        apployal_points = :apployal_points,
                        writeoff_points = :writeoff_points,
                        onlinestoreshare_points = :onlinestoreshare_points
                    WHERE id_store = :id_store AND order_date = :order_date
                """)
                connection.execute(sql, {
                    'nps_points': row['nps_points'],
                    'checkyoy_points': row['checkyoy_points'],
                    'proceedsyoy_points': row['proceedsyoy_points'],
                    'avgcheckyoy_point': row['avgcheckyoy_point'],
                    'apployal_points': row['apployal_points'],
                    'writeoff_points': row['writeoff_points'],
                    'onlinestoreshare_points': row['onlinestoreshare_points'],
                    'id_store': row['id_store'],
                    'order_date': row['order_date']
                })
        print("ВСЁ!")

    def _get_data_for_last_month(self):
        """Ф-ия для получения данных за последний месяц. Возможно для обновления"""

        # Получаем текущую дату
        today = datetime.today()


        # Определяем начало текущего месяца
        start_of_month = today.replace(day=1)
        # Меняем тип данных даты к строке
        end_date = today.strftime('%Y-%m-%d')
        #Запоминаем дату в переменную(также строкой)
        self.date_now_month = start_of_month.strftime('%Y-%m-%d')
        #Создаём пустой датафрейм
        df_res = pd.DataFrame()
        #Готовим запрос
        payload = {
            "indicators": [self.list_columns_data[0]],
            "storeCondition": [],
            "ageGroup": [],
            "idManager": [],
            "channel": [],
            "idRegion": [],
            "idStore": [],
            "idCity": [],
            "lfl": [],
            "dateStart": self.date_now_month,
            "dateEnd": end_date
        }

        # Отправляем POST-запрос
        response = requests.post(self.url, json=payload)

        if response.status_code == 200:
            #Получили ответ и пополняем им наш датафрейм и даём ему столбец, равный дате
            data = response.json()
            df_res = pd.DataFrame(data)
            df_res['order_date'] = self.date_now_month
        #До этого мы инициализировали df_res, тут мы пополняем его оставшимися данными
        for columns in self.list_columns_data[1:]:
            #Запрос
            payload = {
                "indicators": [columns],
                "storeCondition": [],
                "ageGroup": [],
                "idManager": [],
                "channel": [],
                "idRegion": [],
                "idStore": [],
                "idCity": [],
                "lfl": [],
                "dateStart": self.date_now_month,
                "dateEnd": end_date
            }

            # Отправляем POST-запрос
            response = requests.post(self.url, json=payload)

            if response.status_code == 200:
                data = response.json()
                monthly_df = pd.DataFrame(data)
                df_res = pd.merge(df_res, monthly_df, on=['idStore', 'storename'],
                                  how='left')

            else:
                print("Ошибка:", response.status_code)
                return None

        return df_res

    def get_data_for_month(self, start_date, end_date):
        """Ф-ия получает датафрейм со всеми данными, которые можно получить от API"""
        df_res = pd.DataFrame()
        payload = {
            "indicators": [self.list_columns_data[0]],
            "storeCondition": [],
            "ageGroup": [],
            "idManager": [],
            "channel": [],
            "idRegion": [],
            "idStore": [],
            "idCity": [],
            "lfl": [],
            "dateStart": start_date,
            "dateEnd": end_date
        }

        # Отправляем POST-запрос
        response = requests.post(self.url, json=payload)

        if response.status_code == 200:
            data = response.json()
            df_res = pd.DataFrame(data)
            df_res['order_date'] = start_date
        for columns in self.list_columns_data[1:]:

            payload = {
                "indicators": [columns],
                "storeCondition": [],
                "ageGroup": [],
                "idManager": [],
                "channel": [],
                "idRegion": [],
                "idStore": [],
                "idCity": [],
                "lfl": [],
                "dateStart": start_date,
                "dateEnd": end_date
            }

            # Отправляем POST-запрос
            response = requests.post(self.url, json=payload)

            if response.status_code == 200:
                data = response.json()
                monthly_df = pd.DataFrame(data)
                df_res = pd.merge(df_res, monthly_df, on=['idStore', 'storename'],
                                     how='left')  # Используйте 'inner', 'left', 'right' для других типов соединений

            else:
                print("Ошибка:", response.status_code)
                return None
        return df_res
    def _take_past_data(self, start_data, end_data):
        """Ф-ия получает начало и конец, после чего запрашивает данные по месяцам из API"""
        #Получаем численные значения наших дат из вида 2025-01-01
        start_year, start_month, start_day = map(int, start_data.split('-'))
        end_year, end_month, end_day = map(int, end_data.split('-'))
        #Создаём из полученных данных экземпляры datetime
        start = datetime(start_year, start_month, start_day)
        end = datetime(end_year, end_month, end_day)

        # Создаем пустой DataFrame для хранения данных
        all_data = pd.DataFrame()

        # Перебираем каждый месяц в заданном диапазоне
        current_date = start
        while current_date < end:
            # Определяем первый и последний день месяца
            first_day = current_date.replace(day=1)
            if current_date.month == 12:
                last_day = current_date.replace(year=current_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                last_day = current_date.replace(day=1) + timedelta(days=32)
                last_day = last_day.replace(day=1) - timedelta(days=1)  # Последний день текущего месяца

            # Получаем данные за месяц
            data = self.get_data_for_month(first_day.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d'))

            # Соединяем датасеты
            all_data = pd.concat([all_data, data], ignore_index=True)

            # Переходим к следующему месяцу
            current_date = last_day + timedelta(days=1)

        # Выводим итоговый DataFrame
        self.past_month = all_data
    def _get_all_data(self):
        """Соединяем датасет со старыми данными и новыми"""
        all_data = pd.concat([self.now_month, self.past_month], axis=0)
        all_data['order_date'] = pd.to_datetime(all_data['order_date'])
        return all_data

    def _NPS(self, df_data):
            # Загрузка данных из CSV-файла
            df = df_data
            df2 = pd.read_csv(self.path_NPS)

            # Оставляем только нужные столбцы
            df2 = df2[['id_store', 'Уровень Кауфмана', 'Начало месяца']]
            df2.rename(columns={'Начало месяца': 'order_date', 'id_store':'idStore'}, inplace=True)
            df2['order_date'] = pd.to_datetime(df2['order_date'], format="%Y-%m-%d %H:%M:%S")

            df['order_date'] = pd.to_datetime(df['order_date'])
            # Преобразуем 'id_store' в обоих DataFrame в строковый тип

            df['idStore'] = df['idStore'].astype(str)
            df2['idStore'] = df2['idStore'].astype(str)
            # Замена значений в столбце 'Уровень Кауфмана' на баллы
            points_mapping = {
                'Криминальный': 0,
                'Базовый': 0,
                'Ожидаемый': 15,
                'Доверительный': 15,
                'Удивляющий': 30,
                'Невероятный': 30
            }

            # Присвоение баллов
            df2['NPS_points'] = df2['Уровень Кауфмана'].map(points_mapping)


            combined_df = pd.merge(df, df2, on=['order_date', 'idStore'], how='left')
            combined_df['NPS_points'] = combined_df['NPS_points'].fillna(30)

            del combined_df['Уровень Кауфмана']
            return combined_df

    def _take_point(self, df_data):
        """Начисляем баллы за счёт формул"""
        result = df_data

        # От года к году чеки
        result['checkYoY_points'] = result.apply(lambda row: 10 if pd.isna(row['checkYoYPercent']) else (
            10 if row['checkYoYPercent'] >= 8 else
            0 if 5 >= row['checkYoYPercent'] else
            (row['checkYoYPercent'] - 5) / (8 - 5) * (10 - 0) + 0), axis=1)

        # От года к году выручка
        result['proceedsYoY_points'] = result['proceedsYoYPercent'].apply(lambda x: 10 if pd.isna(x) else
        10 if x >= 20 else
        0 if x <= 15 else
        (x - 10) / (20 - 10) * (10 - 0) + 0)

        # Средний чек
        result['avgCheckYoY_point'] = result['avgCheckYoYPercent'].apply(lambda x: 10 if pd.isna(x) else (
            10 if x >= 7 else
            0 if 4 > x else
            (x - 4) / (7 - 4) * (10 - 0) + 0))

        # Лояльность
        result['appLoyal_points'] = result['appLoyalPercent'].apply(lambda x:  0 if pd.isna(x) else (
            0 if x <= 55 else
            10 if 80 < x else
            (x - 55) / (80 - 55) * (10 - 0) + 0))
        result['appLoyal_points'] = result['appLoyal_points'].fillna(0)
        # Списания
        result['writeOff_points'] = result['writeOffPercent'].apply(
            lambda x:
                0 if pd.isna(x) else (
                20 if x <= 3.8 else
                0 if 5 < x else
                (x - 5) / (3.8 - 5) * (20 - 10) + 0))
        result['writeOff_points'] = result['writeOff_points'].fillna(0)
        # Онлайн магазины
        result['onlineStoreShare_points'] = result['onlineStoreSharePercent'].apply(
            lambda x:
            0 if pd.isna(x) else (
                10 if x >= 5 else
                0 if 5 > x else
                (x - 1.99) / (4.99 - 1.99) * (10 - 0) + 0))
        result['onlineStoreShare_points'] = result['onlineStoreShare_points'].fillna(0)
        result['all_point'] = result[['NPS_points', 'checkYoY_points', 'proceedsYoY_points',
                                      'avgCheckYoY_point', 'appLoyal_points',
                                      'writeOff_points', 'onlineStoreShare_points']].sum(axis=1)
        return result

