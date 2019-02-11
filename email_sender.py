# Imports
import sqlite3
import smtplib
import json
import sys
import ssl
import time
import random
import logging
import pandas as pd

from os.path import basename, join
from os import listdir
from logging.handlers import RotatingFileHandler
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class PersonalEmailSender(object):
    """
    Class for sending multiple mails from excel files 
    using desired smtp configurations
    """
    def __init__(self,conf_file_location):
        self.table_name = "EmailList"
        self.log_file_name = "Email_List_Log"
        self.invalid_email_id_list = []

        self.conf_file_location = conf_file_location[0]
        self.read_conf()
        self.database_setup()
        self.read_from_excel()

    def init_logger(self,conf_file_data):
        self.file_size = conf_file_data.get('SIZE',52428800)
        self.back_up_count = conf_file_data.get('BACKUPCOUNT',10)
        self.logger_name = conf_file_data.get('LOGGERNAME',"Rotating Log")
        self.log_mode = conf_file_data.get('MODE',"DEBUG")
        self.logger = logging.getLogger(self.logger_name)
        if self.log_mode == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        elif self.log_mode == 'ERROR':
            self.logger.setLevel(logging.ERROR)
        elif self.log_mode == 'INFO':
            self.logger.setLevel(logging.INFO)
        elif self.log_mode == 'CRITICAL':
            self.logger.setLevel(logging.CRITICAL)
        elif self.log_mode == 'WARNING':
            self.logger.setLevel(logging.WARNING)
        elif self.log_mode == 'NOTSET':
            self.logger.setLevel(logging.NOTSET)
        else:
            raise Exception('Logging Mode is not Set')
        self.handler = RotatingFileHandler(self.log_file_name,maxBytes=self.file_size, \
        backupCount=self.back_up_count)
        self.formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s",\
        "%Y-%m-%d %H:%M:%S")
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

    def read_conf(self):
        # No logging here since no logger is set up
        with open(self.conf_file_location) as conf_file:
            conf_file_data = json.load(conf_file)
            self.sender_email_id_list = conf_file_data.get('Sender_Email_ID',[])
            # TOOD: Not encrypted - Totally wrong way storing password - Change it
            self.sender_email_password_list = conf_file_data.get('Sender_Email_Password',[])
            self.content_file_location = conf_file_data.get('Email_Content_File_Location', None)
            self.email_subtitle = conf_file_data.get('Email_Topic', '')
            self.smtp_port = conf_file_data.get('SMTP_Port',587)
            self.smtp_service = conf_file_data.get('SMTP_Service','smtp.gmail.com')
            self.email_content_location = conf_file_data.get('Email_Content_File_Location','')
            self.email_attachments_location = conf_file_data.get('Email_Attachments_Location','')
            self.email_excel_list_location = conf_file_data.get('Excel_Excel_List_Location','')
            self.db_location = conf_file_data.get('Database_Location','TempDataBase')
            self.sleep_time = conf_file_data.get('Delay_Limit',50)
            self.daily_email_limit = conf_file_data.get('Daily_Email_Limit',400)
            self.init_logger(conf_file_data)
        
    
    def database_setup(self):
        try:
            self.create_database_connection()
            self.create_table()
        except Exception as e:
            self.logger.exception('Error inside database_setup. ERROR : {}'.format(str(e)))  

    def random_sender_emailid(self):
        try:
            self.check_emailid_exhausted()
            email_id_list_length = len(self.sender_email_id_list) - 1
            random_number = random.randint(0,email_id_list_length)
            self.sender_email_id = self.sender_email_id_list[random_number]
            self.check_daily_limit()
            self.sender_email_password = self.sender_email_password_list[random_number]
        except Exception as e:
            self.logger.exception('Error inside random_sender_emailid. ERROR : {}'.format(str(e)))


    def set_up_email_message(self):
        try:
            self.msg = MIMEMultipart() 
            self.msg['From'] = self.sender_email_id
            self.msg['Subject'] = self.email_subtitle
            with open(self.email_content_location, "r") as email_content:
                self.email_body = email_content.read()
            self.msg.attach(MIMEText(self.email_body, 'html'))
            
            for email_attachment_location in self.email_attachments_location:
                with open(email_attachment_location, "rb") as file_content:
                    part = MIMEApplication(
                        file_content.read(),
                        Name=basename(email_attachment_location)
                    )
                part['Content-Disposition'] = 'attachment; filename="{}"'.\
                    format(basename(email_attachment_location))
                self.msg.attach(part)
        except Exception as e:
            self.logger.exception('Error inside set_up_email_message. ERROR : {}'.format(str(e)))
    

    def read_from_excel(self):
        try:
            excel_files = listdir(self.email_excel_list_location)
            for excel_file_name in excel_files:
                excel_file_path = join(self.email_excel_list_location,excel_file_name)
                opened_excel_file =  pd.read_excel(excel_file_path)
                for index, row in opened_excel_file.iterrows():
                    if(not pd.isnull(row['EmailAddress']) ):
                        self.random_sender_emailid()
                        self.check_daily_limit()
                        if (self.check_email_exists((row['EmailAddress']))):
                            print('Sending mail to {} from {}'.format(row['EmailAddress'],self.sender_email_id))
                            self.logger.info('Sending mail to {} from {}'.format(row['EmailAddress'],self.sender_email_id))
                            self.set_up_email_message()
                            
                            # self.msg['To'] = row['EmailAddress']
                            # random_sleep_time = random.randint(1,self.sleep_time)
                            # time.sleep(random_sleep_time)
                            # self.send_mail()
                            self.insert_database_record(index,row['EmailAddress'],excel_file_name)
        except Exception as e:
            self.logger.exception('Error inside read_from_excel. ERROR : {}'.format(str(e)))

    def check_emailid_exhausted(self):
        try:
            if(len(self.invalid_email_id_list) == len(self.sender_email_id_list)):
                print('All sender email id daily limit has been reached. Program exiting. Try next day')
                self.logger.info('All sender email id daily limit has been reached. Program exiting. Try next day')
                exit()

        except Exception as e:
            self.logger.exception('Error inside check_emailid_exhausted. ERROR : {}'.format(str(e)))


    def check_daily_limit(self):
        try:
            sql_command = 'select count(*) from ' + self.table_name + ' where created_at_date=(?) and sender_email_id=(?)' 
            values = (datetime.today().date(),self.sender_email_id)
            result =  self.database_cursor.execute(sql_command,values)
            data = result.fetchall()
            count = data[0][0]
            if(count>=self.daily_email_limit):
                if self.sender_email_id not in self.invalid_email_id_list:
                    self.invalid_email_id_list.append(self.sender_email_id)
                # Find another sender email id not in invaid list
                while(self.sender_email_id in self.invalid_email_id_list):
                    self.random_sender_emailid()

        except Exception as e:
            self.logger.exception('Error inside check_daily_limit. ERROR : {}'.format(str(e)))    

    def check_email_exists(self,email_id):
        try:
            sql_command = 'select email_id,sender_email_id from ' + self.table_name + ' where email_id=(?)' 
            values = (email_id,)
            result =  self.database_cursor.execute(sql_command,values)
            data = result.fetchall()
            if(not len(data)):
                return True
            else:
                print('Email addreses {} already exists in database. Email as already been sent to {} by {}'.\
                    format(email_id,email_id,self.sender_email_id))
                self.logger.info('Email {} addreses already exists in database. Email as already been sent to {} by {}'.\
                    format(email_id,email_id,self.sender_email_id))
                return False

        except Exception as e:
            self.logger.exception('Error inside check_email_exists. ERROR : {}'.format(str(e)))

    def insert_database_record(self,row,email,file_name):
        try:
            sql = 'insert into ' + self.table_name + ' (created_at_date,email_id,file_row,file_name,sender_email_id) values(?,?,?,?,?)' 
            params = (datetime.today().date(),email,row,file_name,self.sender_email_id)
            self.database_cursor.execute(sql,params)
        except Exception as e:
            self.logger.exception('Error inside insert_database_record. ERROR : {}'.format(str(e)))

    def clear_table(self):
        try:
            sql_command = 'truncate table' + self.table_name
            self.database_cursor.execute(sql_command)
        except Exception as e:
            self.logger.exception('Error inside clear_table. ERROR : {}'.format(str(e)))

    def show_table(self):
        try:
            sql_command = 'select * from ' + self.table_name
            values = self.database_cursor.execute(sql_command)
            for row in values:
                print(row)
        except Exception as e:
            self.logger.exception('Error inside show_table. ERROR : {}'.format(str(e)))

    def create_database_connection(self):
        try:
            self.database_connection = sqlite3.connect(self.db_location)
            self.database_cursor = self.database_connection.cursor()
        except Exception as e:
            self.logger.exception('Error inside create_database_connection. ERROR : {}'.format(str(e)))

    def create_table(self):
        try:
            sql_query = 'create table if not exists ' + self.table_name + \
                '(created_at_date date ,email_id text, file_row integer, file_name text, sender_email_id text)'
            self.database_cursor.execute(sql_query)
        except Exception as e:
            self.logger.exception('Error inside create_table. ERROR : {}'.format(str(e)))

    def send_mail(self):
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_service, self.smtp_port, context=context) as server:
                server.login(self.sender_email_id, self.sender_email_password)
                server.ehlo()
                text = self.msg.as_string()
                server.sendmail(self.sender_email_id, self.msg['To'], text)
        except Exception as e:
            self.logger.exception('Error inside send_mail. ERROR : {}'.format(str(e)))

    def show_exhausted_email_id_list(self):
        try:
            for email_id in self.invalid_email_id_list:
                print('Email id {} has reached it\'s daily limit'.format(email_id))
        except Exception as e:
            self.logger.exception('Error inside show_exhausted_email_id_list. ERROR : {}'.format(str(e)))   

    def __del__(self):
        self.show_table()
        self.database_connection.commit()
        self.database_connection.close()
        self.show_exhausted_email_id_list()

if __name__=='__main__':
    conf_file_location = sys.argv[1:2]
    personal_emailer = PersonalEmailSender(conf_file_location)
