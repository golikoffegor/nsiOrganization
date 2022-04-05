import ftplib
import re
import zipfile
import os
import datetime
import xmltodict
from settings import *

'''Этот скрипт предназначен для загрузки данных с FTP сервера ЕИСа 
по организациям с возможностью инкрементально обновлять данные
в структурированном виде для MongoDB.
В файле settings находится информация:
Информация для подключения к FTP:
- ftp_eis_host; 
- ftp_eis_login; 
- ftp_eis_password.
Местоположение файла для логов:
- log_file_place.
Путь к из которого необходимо скачать данные:
- dir_nsi_organization.
Путь куда будут выгружены данные:
- saved_files_dir.
Название основных и инкрементальных файлов для загрузки:
- all_files_name;
- inc_files_name.
Строка соединения к MongoDB
- client.
Указание конкретной БД и коллекция для загрузки данных:
- db;
-coll_nsiorg.'''


# Модуль для загрузки данных в MongoDB
def insert_document(collection, data):
    return collection.insert(data, check_keys=False)


# Модуль для логирования действий
def logger(message, log_file_place):
    with open(log_file_place, "a") as write_file:
        message_time = datetime.datetime.now()
        write_file.write(f'{message} {message_time.strftime("%m/%d/%Y, %H:%M:%S")}\n')


# Модуль для удаления файлов в директории
def deleted_files(dir_to, log_file_place):
    os.chdir(dir_to)
    for filename in os.listdir(dir_to):
        os.remove(filename)
        logger(f'File {filename} remove successfully', log_file_place)


# Модуль для скачивания файлов с FTP
def get_doc_from_ftp(host, login, password, dir_from, dir_to):
    ftp = ftplib.FTP(host, login, password, dir_from)
    ftp.encoding = 'utf-8'
    ftp.cwd(dir_nsi_organization)
    os.chdir(dir_to)
    for filename in ftp.nlst():
        local_filename = os.path.join(saved_files_dir, filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR ' + filename, file.write)
        if filename in ftp.nlst():
            logger(f'File {filename} successfully download from FTP', log_file_place)
        else:
            logger(f'File {filename} failure download from FTP', log_file_place)


# Модуль для загрузки в MongoDB данных по организациям из архивов
def firms_to_mongo(dir_to, coll_nsiorg, files_name, log_file_place):
    os.chdir(dir_to)
    for filename in os.listdir(dir_to):
        if re.compile(files_name).search(filename):
            zip_file = zipfile.ZipFile(filename)
            with zip_file as zf:
                for z_names in zf.namelist():
                    data = xmltodict.parse(zip_file.read(z_names))
                    for organization in data['export']['nsiOrganizationList']['nsiOrganization']:
                        data = organization
                        id_entity = organization['oos:regNumber']
                        data.update({'_id': id_entity})
                        finding_info = coll_nsiorg.find_one({'_id': organization['oos:regNumber']})
                        if finding_info:
                            result_update = coll_nsiorg.update_one({'_id': organization['oos:regNumber']},
                                                                   {'$set': organization})
                            if result_update.matched_count > 0:
                                logger(f'Succes update {id_entity} organization', log_file_place)
                            else:
                                logger(f'Failure update {id_entity} organization', log_file_place)
                        else:
                            insert_document(coll_nsiorg, data)
                            logger(f'Success insert {id_entity} organization', log_file_place)

# Скачивает архивы
get_doc_from_ftp(ftp_eis_host, ftp_eis_login, ftp_eis_password, dir_nsi_organization, saved_files_dir)

# Загружает в MongoDB данные по недельным архивам
# firms_to_mongo(saved_files_dir, coll_nsiorg, all_files_name, log_file_place)

# Загружает в MongoDB данные по ежедневным архивам
firms_to_mongo(saved_files_dir, coll_nsiorg, inc_files_name, log_file_place)

# Удаляет загруженные файлы
deleted_files(saved_files_dir, log_file_place)
