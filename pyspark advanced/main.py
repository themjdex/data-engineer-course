import numpy as np
import csv
import pyspark
import argparse
import logging
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from datetime import datetime, timedelta
from typing import List, Dict
from math import floor
from cryptography.fernet import Fernet

from utils.translit_schema import translit_dict
from utils.crypto import hash_keys

# заводим логгер для отсылки сообщений в терминал
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# указываем константы путей файлов с именами и городами и директорию для дампа csv
OUTPUT_PATH = 'output/'
NAME_DATABASE_PATH = 'utils/names.txt'
CITIES_DATABASE_PATH = 'utils/cities.txt'

def start_session_and_prepare_data() -> Dict[
                                        pyspark.sql.session.SparkSession, 
                                        pyspark.rdd.RDD
                                        ]:
    '''
    Функция создает спарк-сессию и подготавливает все массивы данных в формате RDD
    
    Returns:
        Dict[pyspark.sql.session.SparkSession, pyspark.rdd.RDD]: Словарь с сессией и RDD
    '''
    # создаем сессию
    spark = SparkSession.builder.appName('generator_synt_data').config('spark.master', 'local[*]').getOrCreate()

    # читаем файлы существующих имен, городов, домены почт и словарь транслитерации и сохраняем в RDD
    with open(NAME_DATABASE_PATH, mode='+r', encoding='utf-8') as file:
        names = file.readlines()

    names_rdd = spark.sparkContext.parallelize([line.strip() for line in names]) \
                                  .persist(pyspark.StorageLevel.MEMORY_ONLY)


    with open(CITIES_DATABASE_PATH, mode='+r', encoding='utf-8') as file:
        cities = file.readlines()

    cities_rdd = spark.sparkContext.parallelize([line.strip() for line in cities]) \
                                   .persist(pyspark.StorageLevel.MEMORY_ONLY)

    emails = spark.sparkContext.parallelize(['@yandex.ru', '@gmail.com', '@yahoo.com', '@mail.ru']) \
                               .persist(pyspark.StorageLevel.MEMORY_ONLY)

    translit_schema = spark.sparkContext.parallelize([translit_dict]) \
                                    .persist(pyspark.StorageLevel.MEMORY_ONLY)

    sc_crypt = spark.sparkContext.parallelize([hash_keys]) \
                                 .persist(pyspark.StorageLevel.MEMORY_ONLY)
    
    crypt_hash_table = broadcast(spark.createDataFrame(sc_crypt))


    return {'spark': spark, 
            'names': names_rdd, 
            'cities': cities_rdd, 
            'emails': emails, 
            'translit_schema': translit_schema,
            'crypt_hash_table': crypt_hash_table}

def finish_session(session: pyspark.sql.session.SparkSession) -> None:
    '''
    Функция завершает спарк-сессию

    Args:
        session (pyspark.sql.session.SparkSession): ссылка на спакр-сессию
    '''
    session.stop()


class SynteticDataGenerator:
    def __init__(self, 
                 row_count: int = 10, 
                 nullable: bool = False, 
                 crypt_type: str = 'fernet', 
                 need_check: str = 'no',
                 **kwargs) -> None:
        '''
        Инициализация всех необходимых параметров и RDD
        Args:
            row_count (int): кол-во генерируемых строк
            nullable (bool): должны ли быть null значения
            spark (pyspark.sql.session.SparkSession): ссылка на спарк-сессию
            names (pyspark.rdd.RDD): RDD с именами
            cities (pyspark.rdd.RDD): RDD с городами
            emails (pyspark.rdd.RDD): RDD с доменами эд. почт
            translit_schema (pyspark.rdd.RDD): RDD со словарем для транслитерации
            crypt_hash_table (pyspark.rdd.RDD): RDD с ключами шифрования символов
            crypt_type (str): тип выбранного шифрования для данных 'fernet' | 'dummy'
        '''
        self.row_count = row_count
        self.nullable = nullable
        self.crypt_type = crypt_type
        self.need_check = need_check
        self.spark = kwargs['spark']
        self.names = kwargs['names']
        self.cities = kwargs['cities']
        self.emails = kwargs['emails']
        self.translit_schema = kwargs['translit_schema']
        self.crypt_hash_table = kwargs['crypt_hash_table']
        self.all_data = []
        

    def _get_random_elem(self, elems: pyspark.rdd.RDD) -> str:
        '''
        Функция возвращает случайный элемент из RDD

        Args:
            elems (pyspark.rdd.RDD): RDD, из которого требуется случайный элемент

        Returns:
            str: случайная строка
        '''
        return elems.takeSample(False, 1)[0]


    def _transliterate(self, text: str) -> str:
        '''
        Функция транслитерирует русский текст в английский

        Args:
            text (str): текст, который нужно транслитерировать

        Returns:
            str: текст английскими символами
        '''
        return ''.join([self.translit_schema.collect()[0].get(char, char) for char in text])


    def collect_data(self) -> None:
        '''
        Функция генерирует и собирает воедино синтетические данные
        '''
        # генерируем айди юзеров
        ids = [i for i in range(self.row_count)]

        # генерируем случайные имена
        names = [self._get_random_elem(self.names) for _ in range(self.row_count)]

        # генерируем случайные города
        cities = [self._get_random_elem(self.cities) for _ in range(self.row_count)]

        # генерируем случайные емейлы
        emails = []
        for i, elem in enumerate(names):
            emails.append(self._transliterate(elem) + str(i) + self._get_random_elem(self.emails))
        
        # генерируем случайный возраст
        ages = [np.random.randint(18, 96) for _ in range(self.row_count)]

        # генерируем случайную зарплату
        salaries = [np.random.randint(10000, 150001) for _ in range(self.row_count)]

        # генерируем случайные даты регистрации не позже года назад
        registration_dates = []
        current_date = datetime.now()

        for elem in ages:
            registration_dates.append((current_date - timedelta(days=np.random.randint(1, 366))).strftime('%Y-%m-%d'))

        # добавим тестовую первую строку для второй задачи
        self.all_data.append({'id': 9999,
                            'name': 'Marat',
                            'email': 'test@test.ru',
                            'city': 'Ufa',
                            'age': 99,
                            'salary': 99,
                            'registration_date': '2024-08-01'})
        
        # сшиваем всё в JSON-подобную структуру и помещаем в общие данные
        for i in range(len(ids)):
            row = {'id': ids[i],
                   'name': names[i],
                   'email': emails[i],
                   'city': cities[i],
                   'age': ages[i],
                   'salary': salaries[i],
                   'registration_date': registration_dates[i]
                   }
            self.all_data.append(row)

        # если было указано, что пропуски в данных нужны, то удаляем случайные данные в не более 5% строк
        if self.nullable:
            trs_5_percent = floor(len(self.all_data) * .05)
            if trs_5_percent >= 1:
                for elem in range(trs_5_percent):
                    random_row = np.random.randint(1, len(self.all_data))
                    random_column = np.random.choice(['name', 'email', 'city', 'age', 'salary', 'registration_date'])
                    self.all_data[random_row][random_column] = None

        # зашифровываем все данные, если это нужно
        if self.crypt_type != 'no':
            self._encrypt_data()
        
        # проверяем файлы за нечетные дни на наличие шифровки, если это нужно
        if self.need_check != 'no':
            self.check_last_csv()



    def _encrypt_data(self) -> None:
        '''
        Функция шифрует каждый символ сгенерированных данных 
        согласно спец. таблице или Fernet
        '''
        if self.crypt_type == 'dummy':
            fields = ['id', 'name', 'email', 'city', 'age', 'salary', 'registration_date']
            char_table = str.maketrans(self.crypt_hash_table.toPandas().to_dict()['_1'][0])

            for row in self.all_data:
                for key in fields:
                    row[key] = str(row[key]).lower().translate(char_table)

        elif self.crypt_type == 'fernet':
            key = Fernet.generate_key()
            cipher = Fernet(key)
            encrypted_data = []

            for row in self.all_data:
                encrypt_rows = {}
                for k, v in row.items():
                    encrypted_value = cipher.encrypt(str(v).encode())
                    encrypt_rows[k] = encrypted_value
                encrypted_data.append(encrypt_rows)

            self.all_data = encrypted_data

        else:
            return

    def check_last_csv(self):
        '''
        Функция проверяет генерации за последние 2 месяца по нечетным дням и
        производит их шифрование, если этого не было сделано
        '''
        # проверяем, что нужная директория в целом на месте
        if not os.path.isdir(OUTPUT_PATH):
            raise ValueError(f'{OUTPUT_PATH} не существует или не найден')
        
        # получаем список всех файлов
        list_files = os.listdir(OUTPUT_PATH)

        # если список файлов не пустой, то начинаем проверки 
        if len(list_files) > 0:
            current_date = datetime.now().date()
            diff_date = current_date - timedelta(days=60)
            dates = []
            for file in list_files:
                if datetime.strptime(file[:10], '%Y-%m-%d').date() > diff_date and \
                        int(file[8:10]) % 2 != 0 and \
                        'safe' not in file:
                    dates.append(file)

        # если найдены файлы, удовлетворяющие всем условиям, то шифруем их, а прежние удаляем от греха подальше
        if len(dates) > 0:
            key = Fernet.generate_key()
            cipher = Fernet(key)
            for file in dates:
                with open(f'{OUTPUT_PATH}{file}', mode='r', newline='') as old_file, open(f'{OUTPUT_PATH}{file[:-4]}_safe.csv', mode='w', newline='') as new_file:
                    reader = csv.reader(old_file)
                    writer = csv.writer(new_file)

                    header = next(reader)
                    writer.writerow(header)

                    for row in reader:
                        encrypted_row = [cipher.encrypt(value.encode()) for value in row]
                        writer.writerow(encrypted_row)
                os.remove(f'{OUTPUT_PATH}{file}')
                
            


    def get_data(self) -> List[Dict]:
        '''
        Функция возвращает все сгенерированные значения

        Returns:
            List[Dict]: список словарей со данными по фейковым юзерам
        '''
        return self.all_data
    
    def to_csv(self) -> None:
        '''
        Функция сохраняет сгенерированные данные в CSV-файл
        '''
        # задаем верный порядок полей
        fieldnames = ['id','name', 'email', 'city', 'age', 'salary', 'registration_date']

        # задаем нужное название файла
        if self.crypt_type == 'no':
            path = f'{OUTPUT_PATH}{datetime.now().strftime('%Y-%m-%d')}-dev.csv'
        else:
            path = f'{OUTPUT_PATH}{datetime.now().strftime('%Y-%m-%d')}-dev_safe.csv'

        # записываем данные в файл
        if self.all_data:
            with open(path, mode='a', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',')
                writer.writeheader()

                for row in self.all_data:
                    writer.writerow(row)
        else:
            raise Exception('Сначала нужно запустить collect_data()')


if __name__ == '__main__':
    # считываем аргументы командной строки
    '''
    примеры команд:
    1. python main.py --rows 50 - будет создан файл с 50 юзерами без пропусков

    2. python main.py --rows 50 --null yes (или любой текст) - будет создан файл с 50 юзерами 
    с пропусками, если это возможно

    3. python main.py --rows 50 --crypt dummy --check yes - будет создан файл с 50 юзерами 
    с "тупым" шифрованием и проверкой файлов за последние 60 дней
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, help='Нужное кол-во фейковых строк в CSV')
    parser.add_argument("--null", type=str, help='указать "yes" или что угодно, если требуется разбавлять файл null значениями')
    parser.add_argument("--crypt", default='fernet', type=str, help='какое нужно шифрование: fernet | dummy | no')
    parser.add_argument("--check", default='no', type=str, help='нужно ли проверить файлы за последние 60 дней и зашифровать их')
    args = parser.parse_args()
    logger.info(f'Start generating {args.rows} syntetic rows {'with' if args.null else 'without'} null values...')

    if args.null: 
        nullable = True
    else:
        nullable = False

    # подгружаем спарк-сессию и RDD
    rdds = start_session_and_prepare_data()

    # инициализуем класс, собираем и дампим в файл
    generator_synt_data = SynteticDataGenerator(row_count=args.rows, 
                                                nullable=nullable, 
                                                crypt_type=args.crypt,
                                                need_check=args.check, 
                                                **rdds)

    logger.info('Generating and collecting data...')
    generator_synt_data.collect_data()

    logger.info('Dump to csv file...')
    generator_synt_data.to_csv()

    logger.info('Gereration is successfully. Work is finished!')
    # завершаем сессию
    finish_session(rdds['spark'])

