from typing import List

from app import db_conn,db
from app.business.common import configcenter
from validator import Length, Required, In, validate, InstanceOf
from app.business.checksqlformat_biz import check_table_exists
from app.business.backup_biz import dbrequest
import json
import requests
from app.models.response.recovery_response import RecoveryResponse
from app.business.common import not_empty
from app.business.checksqlformat_biz import CheckFormat
import re,random
import datetime,time
import xmltodict
from dateutil.parser import parse
import threading
import os,base64
from app import db_dict
from .backup_biz import BCPExe
from app.business.execsql_biz import Execsql_biz
from deepdiff import DeepDiff

class RevocerList():
    def __init__(self, recovery_entity):
        self.recovery_entity = recovery_entity
        self.RevocerServer = recovery_entity.get('RevocerServer',None)
        self.logs = []

    def validator_recover_list_request(self):
        self.logs.append(
            "%s   validator_recover_list_request" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        recover_list_request_model = {
            "userId": [Required, Length(1, maximum=50), InstanceOf(str)],
            "execId": [Required, InstanceOf(int)],
            "taskType": [In(["BackupRecover"])]
        }
        result = validate(recover_list_request_model, self.recovery_entity)
        if not result.valid:
            raise Exception(result[1])

    def validator_request_contain_server(self):
        if "Server" in self.recovery_entity:
            if (self.recovery_entity["Server"] == None) or (self.recovery_entity["Server"] == "") or \
                    ("Port" not in self.recovery_entity) or (self.recovery_entity["Port"] == None) or (self.recovery_entity["Port"] == "") or \
                    ("User" not in self.recovery_entity) or (self.recovery_entity["User"] == None) or (self.recovery_entity["User"] == "") or \
                    ("Password" not in self.recovery_entity) or (self.recovery_entity["Password"] == None) or (self.recovery_entity["Password"] == ""):
                raise Exception(
                    "Please provide [Server] or [BackupENV] at least one in request body. If provide [Server] "
                    "then server, port, user and password must have value.")

    def get_exec_records_by_execid(self):
        self.logs.append(
            "%s   get_exec_records_by_execid" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        query_exec_records_by_execid = configcenter.recoveryconfig.get("get_exec_records_by_execid")
        exec_id = self.recovery_entity["execId"]
        parameters = {"transaction_number": exec_id}
        exec_info = db_conn.execute(query_exec_records_by_execid, parameters).fetchone()
        if not exec_info:  # 查询不到Exec Info
            raise Exception("Can not find exec records by execid.")
        elif not exec_info[8]:  # BackupIds无值，查询不到BackupIds
            raise Exception("Can not find backup id by execid.")
        return exec_info

    def update_exec_records_by_execid(self):
        self.logs.append(
            "%s   update_exec_records_by_execid" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        update_exec_records_by_execid = configcenter.recoveryconfig.get("update_execinfo")
        parameters = {"transaction_number": self.recovery_entity["execId"],
                      "last_edit_user": self.recovery_entity["userId"]
                      }
        db_conn.execute(update_exec_records_by_execid, parameters)

    def get_table_column_info(self, table_attributes, table_name, db_evn):
        self.logs.append(
            "%s   get_table_column_info" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        table_info_list = table_name.split('.')
        self.logs.append(
            "%s   check_table_exists start" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        print(self.recovery_entity)
        Server = None
        Port = None
        User = None
        Password = None
        if "Server" in self.recovery_entity:
            Server = self.recovery_entity["Server"]
            Port = self.recovery_entity["Port"]
            User = self.recovery_entity["User"]
            Password = self.recovery_entity["Password"]
        table_column_info = check_table_exists(table_attributes, table_info_list, db_evn, Server, Port, User, Password)
        self.logs.append(
            "%s   check_table_exists end" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        column_pk_list = []
        location=0
        for data in table_column_info["data"]:
            if data["ColumnPK"]:
                column_pk = {"pk_colomn_name": data["ColumnName"].upper(), "pk_type": data["ColumnType"],
                             "column_identity": data["ColumnIdentity"],"pk_localtion":location}
                column_pk_list.append(column_pk)
            if data["ColumnIdentity"] == 1 or data["ColumnIdentity"] == "1":
                column_pk = {"pk_colomn_name": data["ColumnName"].upper(), "pk_type": data["ColumnType"],
                             "column_identity": data["ColumnIdentity"],"pk_localtion":location}
                column_pk_list.append(column_pk)
            location+=1
        table_column_info = {"columns": table_column_info["data"], "pk_list": column_pk_list}
        table_column_info['pk_list']=[dict(t) for t in set([tuple(d.items()) for d in table_column_info['pk_list']])]
        return (table_column_info)


    def insert_data_from_clouddata_to_db_sql(self, backup_cloud_data, table_name, pk_list):
        self.logs.append(
            "%s   insert_data_from_clouddata_to_db_sql" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        backup_cloud_data_values = list(backup_cloud_data.values())

        i = 0
        while i < len(backup_cloud_data_values):
            if backup_cloud_data_values[i] is None:
                backup_cloud_data_values[i] = "null"
            else:
                backup_cloud_data_values[i] = str(backup_cloud_data_values[i])
            i += 1

        back_cloud_column_values = ','.join(backup_cloud_data_values)

        back_cloud_data_key = list(backup_cloud_data.keys())
        back_cloud_columns = ['[{}]'.format(column) for column in back_cloud_data_key]
        back_cloud_columns = ','.join(back_cloud_columns)
        column_identity = 0
        for pk in pk_list:
            if pk["column_identity"] == 1 or  pk["column_identity"] == "1":
                column_identity = 1
        if column_identity == 1:
            recover_sql = '''SET IDENTITY_INSERT %s ON; INSERT INTO %s (%s) SELECT %s; SET IDENTITY_INSERT  %s OFF; ''' % (
                table_name, table_name, back_cloud_columns, back_cloud_column_values, table_name)
        else:
            recover_sql = '''INSERT INTO %s (%s) SELECT %s;''' % (
                table_name, back_cloud_columns, back_cloud_column_values)
        return (recover_sql)

    def where_condition_list(self, backup_sql, table_column_info, recovery_db_evn, db_server, table_name):
        self.logs.append(
            "%s   where_condition_list" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))

        # 根据备份的原始SQL，拼接where条件
        backup_sql_condition_list = []
        backup_where_list = []
        backup_sql = backup_sql.split(';')
        backup_sql_list = list(filter(not_empty, backup_sql))
        check_result = {}
        for backup_sql in backup_sql_list:
            backup_sql = backup_sql.replace('\n', ' ')
            Server = None
            Port = None
            User = None
            Password = None
            if "Server" in self.recovery_entity:
                Server = self.recovery_entity["Server"]
                Port = self.recovery_entity["Port"]
                User = self.recovery_entity["User"]
                Password = self.recovery_entity["Password"]
            check_body = {"SqlStr": backup_sql, "ConnectionStr": db_server, "ExecuteEnv": recovery_db_evn}
            check_result = CheckFormat.check_select_sql_format(check_body, Server, Port, User, Password, is_check_table = False)
            if type(check_result) == list:
                if check_result[0]["IsSuccess"] == False:
                    raise Exception(check_result[0]["Reason"])

        # print(check_result)
        table_list = check_result["Data"][0]["TableList"]
        condition_list = check_result["Data"][0]["ConditionList"]
        for table in table_list:
            if table["Name"] == table_name:
                table_name_alias = table["Alias"]
        for condition in condition_list:
            if condition["Alias"] == None:
                backup_sql_condition_list.append(condition["Name"])
            elif condition["Alias"] == table_name_alias:
                backup_sql_condition_list.append(condition["Name"])
        if backup_sql_condition_list:
            for column in backup_sql_condition_list:
                where_condition = " %s=  %s " % (column, "{%s}" % column)
                backup_where_list.append(where_condition)
            backup_where_list = " and ".join(backup_where_list)

        # 根据主键拼接Where条件
        pk_where_list = []
        pk_list = table_column_info["pk_list"]
        if pk_list:
            for pk in pk_list:
                pk_colomn_name = pk["pk_colomn_name"].upper()
                where_condition = " %s=  %s " % (pk_colomn_name, "{%s}" % pk_colomn_name)
                pk_where_list.append(where_condition)
            pk_where_list = " and ".join(pk_where_list)

        # 最终的Where条件，包含备份的原始SQL拼接和主键拼接的Where条件
        final_where_condition_list = []
        if backup_where_list:
            final_where_condition_list.append(backup_where_list)
        if pk_where_list:
            final_where_condition_list.append(pk_where_list)
        return final_where_condition_list

    def query_backup_info(self, backup_id):
        self.logs.append(
            "%s   query_backup_info" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        backup_cloud_data_url = configcenter.urlconfig.get('clouddataurl')
        get_backup_info_by_backupid_cloud_data_url = backup_cloud_data_url + backup_id
        backup_info = requests.get(get_backup_info_by_backupid_cloud_data_url)
        if backup_info.status_code == 204:  # 根据BackupId在CloudData中找不到备份记录
            raise Exception("[BackupId: %s] Can not find backup info by backup id in clouddata." % backup_id)
        backup_info = json.loads(backup_info.text)
        return backup_info

    def query_db_sql(self, table_name, backup_sql, table_column_info, recovery_db_evn,
                     db_server):
        self.logs.append(
            "%s   query_db_sql" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        where_condition_list = self.where_condition_list(backup_sql, table_column_info, recovery_db_evn, db_server,
                                                         table_name)
        db_content_at_recovery_time_sql_list = []
        if where_condition_list:
            for where_condition in where_condition_list:
                db_content_at_recovery_time_sql = "select * from %s where %s" % (table_name, where_condition)
                db_content_at_recovery_time_sql_list.append(db_content_at_recovery_time_sql)
        else:
            db_content_at_recovery_time_sql = "select * from %s" % (table_name)
            db_content_at_recovery_time_sql_list.append(db_content_at_recovery_time_sql)
        return db_content_at_recovery_time_sql_list

    def query_db(self, recovery_db_evn, db_server, db_data_at_recovery_time_sql_list, backup_data):
        self.logs.append(
            "%s   query_db" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        result = []
        distinct_result = []
        for db_data_at_recovery_time_sql in db_data_at_recovery_time_sql_list:
            p1 = re.compile(r'[{](.*?)[}]', re.S)
            where_fileds = re.findall(p1, db_data_at_recovery_time_sql)  # 找到where条件中需要替换的字段值
            for filed in where_fileds:
                db_data_at_recovery_time_sql = db_data_at_recovery_time_sql.replace("{%s}" % filed,
                                                                                    str(backup_data[filed]))
            db_result = dbrequest(db_data_at_recovery_time_sql, recovery_db_evn, db_server, None, True)  # 查询当前数据库中的数据
            result += db_result
        [distinct_result.append(r) for r in result if r not in distinct_result]  # 查询结果去掉重复数据
        i = 0
        while i < len(distinct_result):
            distinct_result[i] = {k.upper(): v for k, v in
                                  distinct_result[i].items()}  # 将列全部修改为大写
            i += 1
        return distinct_result

    def convert_backup_value_format(self, backup_value):
        if backup_value is None:
            convert_backup_value = json.dumps(backup_value, ensure_ascii=False)
        elif type(backup_value) == str:
            try:
                time = parse(backup_value[2:-1])
                convert_backup_value = "'%s'" % str(time.replace(tzinfo=None).isoformat(" ","milliseconds"))
                print("---%Y-%m-%dT%H:%M:%S.%f%z---")
                print(backup_value)
                print(convert_backup_value)
            except ValueError:
                convert_backup_value = backup_value
        else:
            convert_backup_value = backup_value

        return convert_backup_value

    def delete_db_sql(self, table_name, backup_sql, table_column_info, recovery_db_evn, db_server):
        self.logs.append(
            "%s   delete_db_sql" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        # where_condition_list = self.where_condition_list(backup_sql, table_column_info, recovery_db_evn, db_server,
        #                                                  table_name)
        delete_sql_list = []
        # if where_condition_list:
        #     for where_condition in where_condition_list:
        #         delete_sql = "delete from %s where %s" % (table_name, where_condition)
        #         delete_sql_list.append(delete_sql)
        # else:
        #     delete_sql = "delete from %s" % (table_name)
        #     delete_sql_list.append(delete_sql)
        delete_sql_by_backup_sql = ("DELETE " + backup_sql[backup_sql.upper().find("FROM"):].upper()).replace(" WITH", " ").replace("(NOLOCK) "," ").replace("NOLOCK "," ")
        delete_sql_by_backup_sql=" ".join(delete_sql_by_backup_sql.split())
        if 'JOIN' in delete_sql_by_backup_sql:
            start_index = delete_sql_by_backup_sql.find(table_name)
            # start_index
            end_index = delete_sql_by_backup_sql[start_index:].find(" INNER JOIN ")
            if end_index < 0:
                end_index = delete_sql_by_backup_sql[start_index:].find(" ON ") + start_index
            else:
                end_index = end_index + start_index
            if end_index > 0:
                table = delete_sql_by_backup_sql[start_index:end_index]
                alias = table.replace(table_name, '')
                if len(alias.strip())==0:
                    alias=table_name.split(".")[-1].strip()
                delete_sql_by_backup_sql = "DELETE " + alias + " FROM " + delete_sql_by_backup_sql[11:]
        print("-----delete_sql_by_backup_sql-----")
        print(delete_sql_by_backup_sql)
        delete_sql_list.append(delete_sql_by_backup_sql)
        return delete_sql_list

    def delete_db(self, backup_cloud_data_content, recovery_db_evn, db_server, delete_db_sql_list):
        self.logs.append(
            "%s   delete_db" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        final_delete_sql_list = []
        for backup_data in backup_cloud_data_content:
            for delete_sql in delete_db_sql_list:
                p1 = re.compile(r'[{](.*?)[}]', re.S)
                where_fileds = re.findall(p1, delete_sql)  # 找到where条件中需要替换的字段值
                for filed in where_fileds:
                    delete_sql = delete_sql.replace("{%s}" % filed, str(backup_data[filed]))
                    final_delete_sql_list.append(delete_sql)
        final_delete_sql = ";".join(final_delete_sql_list)
        dbrequest(final_delete_sql, recovery_db_evn, db_server, None, False)

    def delete_db_with_values(self, backup_cloud_data_content, recovery_db_evn, db_server, delete_db_sql_list):
        self.logs.append(
            "%s   delete_db" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        final_delete_sql_list = []
        for backup_data in backup_cloud_data_content:
            for delete_sql in delete_db_sql_list:
                p1 = re.compile(r'[{](.*?)[}]', re.S)
                where_fileds = re.findall(p1, delete_sql)  # 找到where条件中需要替换的字段值
                for filed in where_fileds:
                    delete_sql = delete_sql.replace("{%s}" % filed, str(backup_data[filed]))
                final_delete_sql_list.append(delete_sql)
        return final_delete_sql_list

    def is_delete_first_true(self, backup_sql, recovery_db_evn, db_server, table_name,
                             table_column_info):
        self.logs.append(
            "%s   is_delete_first_true" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        pk_list = table_column_info["pk_list"]
        # if pk_list:  # 数据库表有主键，则删除，然后新增
            # 删除数据库中存在的备份数据
        delete_sql_list = self.delete_db_sql(table_name, backup_sql, table_column_info, recovery_db_evn, db_server)
            # delete_sql_list_final = self.delete_db_with_values(backup_cloud_data_content, recovery_db_evn, db_server,
            #                                                    delete_sql_list)
            # exe_sql_list = []
            # delete_sql_list_final_distinct = list(set(delete_sql_list_final))
            # if delete_sql_list_final_distinct:
            #     exe_sql_list += delete_sql_list_final_distinct
            # print("------------------------------------")
            # print(exe_sql_list)
            # 将备份的数据，新增到数据库
            # insert_sql_list = []
            # for backup_data in backup_cloud_data_content:
            #     for column in table_column_info["columns"]:
            #         if column["ColumnComputed"] == 1:
            #             if column["ColumnName"].upper() in backup_data:
            #                 backup_data.pop(column["ColumnName"].upper())
            #     insert_sql = self.insert_data_from_clouddata_to_db_sql(backup_data, table_name, pk_list)
            #     insert_sql_list.append(insert_sql)
            # exe_sql_list += insert_sql_list
        return delete_sql_list
        # else:  # 数据库表无主键。
        #     delete_sql_list = self.delete_db_sql(table_name, backup_sql, table_column_info, recovery_db_evn, db_server)
        #     self.delete_db(backup_cloud_data_content, recovery_db_evn, db_server, delete_sql_list)
        #     self.recovery_by_backup_count(table_name, backup_sql, table_column_info, recovery_db_evn, db_server,
        #                                   backup_cloud_data_content, pk_list)

    def is_delete_first_true_old(self, backup_sql, recovery_db_evn, db_server, table_name, backup_cloud_data_content,
                             table_column_info):
        self.logs.append(
            "%s   is_delete_first_true" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        pk_list = table_column_info["pk_list"]
        if pk_list:  # 数据库表有主键，则删除，然后新增
            # 删除数据库中存在的备份数据
            delete_sql_list = self.delete_db_sql(table_name, backup_sql, table_column_info, recovery_db_evn, db_server)
            delete_sql_list_final = self.delete_db_with_values(backup_cloud_data_content, recovery_db_evn, db_server,
                                                               delete_sql_list)
            exe_sql_list = []
            delete_sql_list_final_distinct = list(set(delete_sql_list_final))
            if delete_sql_list_final_distinct:
                exe_sql_list += delete_sql_list_final_distinct
            print("------------------------------------")
            print(exe_sql_list)
            # 将备份的数据，新增到数据库
            insert_sql_list = []
            for backup_data in backup_cloud_data_content:
                for column in table_column_info["columns"]:
                    if column["ColumnComputed"] == 1:
                        if column["ColumnName"].upper() in backup_data:
                            backup_data.pop(column["ColumnName"].upper())
                insert_sql = self.insert_data_from_clouddata_to_db_sql(backup_data, table_name, pk_list)
                insert_sql_list.append(insert_sql)
            exe_sql_list += insert_sql_list
            return exe_sql_list
        else:  # 数据库表无主键。
            delete_sql_list = self.delete_db_sql(table_name, backup_sql, table_column_info, recovery_db_evn, db_server)
            self.delete_db(backup_cloud_data_content, recovery_db_evn, db_server, delete_sql_list)
            self.recovery_by_backup_count(table_name, backup_sql, table_column_info, recovery_db_evn, db_server,
                                          backup_cloud_data_content, pk_list)
    # 将备份的数据与数据库的数据对比，直到他们的条数一致
    def recovery_by_backup_count(self, table_name, backup_sql, table_column_info, recovery_db_evn, db_server,
                                 backup_cloud_data_content, pk_list):
        self.logs.append(
            "%s   recovery_by_backup_count" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        db_data_at_recovery_time_sql_list = self.query_db_sql(table_name, backup_sql, table_column_info,
                                                              recovery_db_evn, db_server)
        for backup_data in backup_cloud_data_content:
            backup_data_count_in_clouddata = backup_cloud_data_content.count(backup_data)
            db_data_at_recovery_time = self.query_db(recovery_db_evn, db_server, db_data_at_recovery_time_sql_list,
                                                     backup_data)  # 查询当前数据库中的数据
            backup_data_count_in_db = db_data_at_recovery_time.count(backup_data)
            insert_sql_list = []
            while backup_data_count_in_db < backup_data_count_in_clouddata:
                insert_sql = self.insert_data_from_clouddata_to_db_sql(backup_data, table_name, pk_list)
                insert_sql_list.append(insert_sql)
                backup_data_count_in_db += 1
            insert_sql_final = ";".join(insert_sql_list)
            dbrequest(insert_sql_final, recovery_db_evn, db_server, None, False)

    def generate_temp_table_sql(self, backup_cloud_data_content, table_column_info):
        column_info_list = table_column_info["columns"]
        temp_table_columns =[]
        insert_clouddata_to_db_list = []
        for column in column_info_list:
            column_type = column["ColumnType"]
            column_name = column["ColumnName"]
            column_length = column["ColumnPrec"]
            if column_name == "SOMemo":
                print(column["ColumnType"])
                print(column['ColumnLength'])
            if "char" in column["ColumnType"]:
                if str(column["ColumnPrec"]) == "-1":
                    column_length = 'max'
                temp_table_column = "[%s] %s (%s)" % (column_name, column_type,column_length)
            elif "text" in column["ColumnType"]:
                temp_table_column = "[%s] %s (%s)" % (column_name, "nvarchar", 4000)
            elif "decimal" == column["ColumnType"]:
                temp_table_column = "[%s] %s(%s,%s)" % (column_name, column_type, column["ColumnPrec"], column["ColumnScale"])
            elif "nvarchar" == column_type and column_length =='-1':
                temp_table_column = "[%s] nvarchar (max)" % column_name
            else:
                temp_table_column = "[%s] %s" %  (column_name, column_type)
            temp_table_columns.append(temp_table_column)

        for backup_content in backup_cloud_data_content:
            temp_table_values = []
            for column in column_info_list:
                column_name = column["ColumnName"].upper()
                if column_name in list(backup_content.keys()):
                    temp_table_value = backup_content[column_name]
                    if temp_table_value is None:
                        temp_table_value = "null"
                    if column["ColumnType"] == "datetime":
                        temp_table_value = self.convert_backup_value_format(temp_table_value)
                    temp_table_values.append(str(temp_table_value))
                else:
                    temp_table_value="null"
                    temp_table_values.append(str(temp_table_value))
            cloud_data = "select " + ','.join(temp_table_values)
            insert_clouddata_to_db = "insert into @temp_table %s" % cloud_data
            insert_clouddata_to_db_list.append(insert_clouddata_to_db)

        temp_table_columns_info = ','.join(temp_table_columns)
        declare_temp_table = "declare @temp_table Table (%s);" % temp_table_columns_info

        insert_cloud_to_db_sql = ";".join(insert_clouddata_to_db_list)+";"
        final_sql = declare_temp_table + insert_cloud_to_db_sql
        return final_sql




    def update_db_sql_with_temp_table(self, table_name, table_column_info, pk_list):
        update_columns = []
        for column in table_column_info["columns"]: # 更新的字段，排除主键
            if column["ColumnPK"] != "PK" and column["ColumnIdentity"]!=1 and column["ColumnComputed"] != 1:  # 排除主键，自增列和计算列
                update_columns.append("[%s]" % column["ColumnName"])

        update_info = ["%s=t.%s" % (column,column) for column in update_columns]
        update_info = ','.join(update_info)
        inner_join_with_pk_list = ["A.%s=t.%s" % (pk["pk_colomn_name"],pk["pk_colomn_name"]) for pk in pk_list]
        inner_join_with_pk = " and ".join(inner_join_with_pk_list)
        update_sql = "update A set %s from %s A inner join @temp_table t on %s;" % (update_info, table_name,  inner_join_with_pk)
        return update_sql

    def insert_db_sql_with_temp_table(self, table_name, pk_list, table_column_info):
        column_identity = 0
        inner_join_with_pk_list = []
        column_name_list = []
        for column in table_column_info["columns"]:
            if column["ColumnComputed"] != 1:   # 排除计算列
                column_name_list.append("[%s]" % column["ColumnName"])
        column_names = ','.join(column_name_list)

        for pk in pk_list:
            inner_join_on = "A.%s=t.%s" % (pk["pk_colomn_name"],pk["pk_colomn_name"])
            inner_join_with_pk_list.append(inner_join_on)
            if pk["column_identity"] == 1 or pk["column_identity"] == "1":
                column_identity = 1
        inner_join_with_pk = " and ".join(inner_join_with_pk_list)

        if column_identity == 1:
            insert_sql = '''SET IDENTITY_INSERT %s ON; INSERT INTO %s(%s) SELECT %s from @temp_table t  WHERE NOT EXISTS (select * from %s A with(nolock) where %s); SET IDENTITY_INSERT  %s OFF; ''' \
                          % (table_name, table_name, column_names, column_names, table_name, inner_join_with_pk, table_name)
        else:
            insert_sql = '''INSERT INTO %s(%s) SELECT %s from @temp_table t  WHERE NOT EXISTS (select * from %s A with(nolock) where %s);''' \
                          % (table_name, column_names, column_names, table_name, inner_join_with_pk)
        return insert_sql


    def is_delete_first_false(self, backup_sql, recovery_db_evn, db_server, table_name, backup_cloud_data_content, table_column_info):
        print(table_column_info)
        self.logs.append(
            "%s   is_delete_first_false" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        pk_list = table_column_info["pk_list"]


        if pk_list:
            temp_table_sql = self.generate_temp_table_sql(backup_cloud_data_content, table_column_info)
            update_sql = self.update_db_sql_with_temp_table(table_name, table_column_info,pk_list)
            insert_sql = self.insert_db_sql_with_temp_table(table_name, pk_list, table_column_info)
            recovery_sql = temp_table_sql+update_sql+insert_sql
            print("--print(recovery_sql)--")
            print(recovery_sql)
            Server = None
            Port = None
            User = None
            Password = None
            if "Server" in self.recovery_entity:
                Server = self.recovery_entity["Server"]
                Port = self.recovery_entity["Port"]
                User = self.recovery_entity["User"]
                Password = self.recovery_entity["Password"]
            dbrequest(recovery_sql, recovery_db_evn, db_server, (Server, Port, User, Password) , isSelect=False)
        else:  # 数据库表无主键。
            delete_sql_list = self.delete_db_sql(table_name, backup_sql, table_column_info, recovery_db_evn, db_server)
            self.delete_db(backup_cloud_data_content, recovery_db_evn, db_server, delete_sql_list)
            self.recovery_by_backup_count(table_name, backup_sql, table_column_info, recovery_db_evn, db_server,
                                          backup_cloud_data_content, pk_list)

    def get_backup_info_detail(self, backup_info):
        self.logs.append(
            "%s   get_backup_info_detail" % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
        backup_info_detail = {}
        if "fileContent" in list(backup_info.keys()):  # 旧数据
            backup_info_detail["is_new_data"] = False
            xml_info = str(backup_info["fileContent"])
            xml_parse = xmltodict.parse(xml_info)
            json_str = json.loads(json.dumps(xml_parse, indent=1))
            print(json_str)
            backup_info_detail["backup_sql"] = json_str["Backup"]["BackupInfo"]["BackupSql"]
            backup_info_detail["db_server"] = json_str["Backup"]["BackupInfo"]["DBServer"]
            recovery_db_database = list(json_str["Backup"].keys())[0]
            recovery_db_table_name = list(json_str["Backup"][recovery_db_database].keys())[0]
            backup_info_detail["table_name"] = recovery_db_database + "." + recovery_db_table_name
            backup_cloud_data_content = json_str["Backup"][recovery_db_database][recovery_db_table_name]
            if backup_cloud_data_content:
                if type(backup_cloud_data_content["row"]) == dict:
                    backup_info_detail["backup_cloud_data_content"] = [backup_cloud_data_content["row"]]  # 备份的数据是单条数据
                else:
                    backup_info_detail["backup_cloud_data_content"] = backup_cloud_data_content["row"]  # 备份的数据是个list
            else:
                backup_info_detail["backup_cloud_data_content"] = []  # 无备份数据
            backup_cloud_data_content= backup_info_detail["backup_cloud_data_content"]
            if backup_cloud_data_content:
                for backup_info in backup_cloud_data_content:
                    for (k,v) in backup_info.items():
                        if v is None:
                            backup_info[k] = ""
                        else:
                            backup_info[k] = v
            backup_info_detail["backup_cloud_data_content"] = backup_cloud_data_content
        else:  # 新数据
            backup_info_detail["is_new_data"] = True
            backup_info_detail["backup_sql"] = backup_info["orginalsql"]
            backup_info_detail["db_server"] = backup_info["database"]
            backup_info_detail["table_name"] = backup_info["TableName"]
            backup_info_detail["backup_cloud_data_content"] = backup_info["dbcontent"]
        return backup_info_detail

    def delete_by_original_sql(self, backup_sql, db_server, recovery_db_evn):
        backup_sql = backup_sql.replace('\n', ' ')
        check_body = {"SqlStr": backup_sql, "ConnectionStr": db_server, "ExecuteEnv": recovery_db_evn}
        Server = None
        Port = None
        User = None
        Password = None
        if "Server" in self.recovery_entity:
            Server = self.recovery_entity["Server"]
            Port = self.recovery_entity["Port"]
            User = self.recovery_entity["User"]
            Password = self.recovery_entity["Password"]
        check_result = CheckFormat.check_select_sql_format(check_body, Server, Port, User, Password, is_check_table=False)
        if type(check_result) == list:
            if check_result[0]["IsSuccess"] == False:
                raise Exception(check_result[0]["Reason"])
        table_list = check_result["Data"][0]["TableList"]
        alias_list = []
        for table in table_list:
            if table["Alias"]:
                alias_list.append(table["Alias"])
        backup_sql_upper = backup_sql.upper()
        backup_sql_remove_withnolock = re.sub("WITH(\s*)[(](\s*)NOLOCK(\s*)[)]", " ", backup_sql_upper)
        backup_sql_final = re.sub("[(](\s*)NOLOCK(\s*)[)]", " ", backup_sql_remove_withnolock)
        if alias_list:
            delete_sql = "delete %s " % alias_list[0] + backup_sql_final[backup_sql_final.find("FROM"):]
        else:
            delete_sql = "delete " + backup_sql_final[backup_sql_final.find("FROM"):]
        # if 'JOIN' in delete_sql:
        #     delete_sql_by_backup_sql_copy = delete_sql
        #     start_index = delete_sql_by_backup_sql_copy.find("DELETE FROM")
        #     end_index = delete_sql_by_backup_sql_copy.find("INNER JOIN")
        #     start_index += 11
        #     print(delete_sql_by_backup_sql_copy)
        #     print(type(delete_sql_by_backup_sql_copy))
        #     table = delete_sql_by_backup_sql_copy[start_index:end_index]
        #     alias = table.replace(table_name, '')
        #     delete_sql_by_backup_sql = "DELETE "+ alias + "FROM " + delete_sql_by_backup_sql[start_index:]----
        dbrequest(delete_sql, recovery_db_evn, db_server, (Server, Port, User, Password), isSelect=False)
    def recovery_by_backup_id(self,backup_id, db_evn, backup_evn, is_delete_first):
        try:
            self.logs.append("%s -----------------backupid: %s-----------------" % (
                str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')), backup_id))
            backup_info = self.query_backup_info(backup_id)  # 根据Backupid查询Backup Cloud Data中的信息
            recovery_db_evn = db_evn
            backup_info_detail = self.get_backup_info_detail(backup_info)

            backup_sql = backup_info_detail["backup_sql"]
            db_server = self.RevocerServer if self.RevocerServer is not None else backup_info_detail["db_server"]
            table_name = backup_info_detail["table_name"]
            backup_cloud_data_content = backup_info_detail["backup_cloud_data_content"]

            for backup_entity in backup_cloud_data_content:
                for backup in backup_entity.items():
                    if type(backup[1]) == bool:  # 将cloud data中的bool类型的值转换成0或者1，从而操作数据库的表字段。
                        backup_entity[backup[0]] = int(backup[1])
                    if type(backup[1]) == str:
                        backup_entity[backup[0]] = "N'" + backup[1].replace("'",
                                                                            "''") + "'"  # 支持中文以及# 将cloud data中包含'的值，替换为''，以便符合SQL的语法规则。

            if backup_cloud_data_content:
                i = 0
                while i < len(backup_cloud_data_content):
                    backup_cloud_data_content[i] = {k.upper(): v for k, v in
                                                    backup_cloud_data_content[
                                                        i].items()}  # 将Cloud Data中，备份表的字段名的列全部修改为大写
                    i += 1

                table_attributes = {"ConnectionStr": db_server, "Name": table_name}

                Server = None
                Port = None
                User = None
                Password = None
                if "Server" in self.recovery_entity:
                    Server = self.recovery_entity["Server"]
                    Port = self.recovery_entity["Port"]
                    User = self.recovery_entity["User"]
                    Password = self.recovery_entity["Password"]
                if Server is None:
                    table_column_info = self.get_table_column_info(table_attributes, table_name, backup_evn)  # 查询表的主键
                else:
                    table_column_info = self.get_table_column_info(table_attributes, table_name, self.recovery_entity["dbEnv"])  # 查询表的主键
                if is_delete_first == 1 or is_delete_first == True:  # IsDeleteFirst值为1，删除目前表中的数据，将Backup CloudData中的数据新增到数据库
                    exec_sql_list = self.is_delete_first_true_old(backup_sql, recovery_db_evn, db_server, table_name,
                                                              backup_cloud_data_content, table_column_info)
                    if exec_sql_list:
                        exec_sql = ";".join(exec_sql_list)
                        self.logs.append(
                            "%s ---------is_delete_first_true-------final_exec_sql start------------------" % str(
                                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                        print("---------is_delete_first_true-------final_exec_sql------------------")
                        print(exec_sql)

                        dbrequest(exec_sql, recovery_db_evn, db_server, (Server, Port, User, Password), isSelect=False)
                        self.logs.append(
                            "%s ---------is_delete_first_true-------final_exec_sql end------------------" % str(
                                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                else:
                    self.is_delete_first_false(backup_sql, recovery_db_evn, db_server, table_name,
                                               backup_cloud_data_content, table_column_info)
            else:
                self.delete_by_original_sql(backup_sql, db_server, recovery_db_evn)
        except Exception as e:
            # print(e)
            # raise Exception(e)
            return RecoveryResponse.recovery_failed(self.recovery_entity["execId"], e.args)
    def recovery_by_backup_id_new(self,backup_id, db_evn, backup_evn, is_delete_first,backup_info_list):
        try:
            self.logs.append("%s -----------------backupid: %s-----------------" % (
                str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')), backup_id))
            for i in backup_info_list:
                if list(i.keys())[0]==backup_id:
                    backup_info=i[backup_id];
            # backup_info = backup_info_list[backup_id]  # 根据Backupid查询Backup Cloud Data中的信息
            #####################new type by bcp#################################################
            filepath=backup_info["dbcontent"]
            print("filepath:",filepath)
            fileinfo=requests.get(filepath)
            filename=str(time.time()).replace(".","")+str(random.randint(10000000,99999999))+".txt"
            if fileinfo.status_code==200:
                with open(filename,"wb") as file:
                    file.write(fileinfo.content)
                file.close()


                tablename=backup_info["TableName"].replace("[","").replace("]","")
                database=backup_info["database"]
                # serverinfo = configcenter.dbconnconfig.get(db_evn).get(database)
                serverinfo = db_dict[db_evn.lower()][database]
                if os.getcwd().__contains__("/opt"):

                    q = "/opt/mssql-tools/bin/bcp \"" + tablename + "\" in  "+filename+"  -N -q -k -t -c -E -S " + serverinfo["server"] + "," + str(
                        serverinfo["port"]) + " -U \"" + serverinfo["user"] + "\" -P \"" + base64.b64decode(serverinfo["password"]).decode() + "\""

                else:
                    if serverinfo.get("port",None)!=None:
                        q = "bcp \"" + tablename + "\" in  "+filename+"  -N -q -k -t -c -E -S " + serverinfo["server"] + "," + str(
                            serverinfo["port"]) + " -U \"" + serverinfo["user"] + "\" -P \"" + base64.b64decode(serverinfo["password"]).decode() + "\""
                    else:
                        q = "bcp \"" + tablename + "\" in  " + filename + "  -N -q -k -t -c -E -S \'" + serverinfo[
                            "server"] + "\' -U \"" + serverinfo["user"] + "\" -P \"" + base64.b64decode(
                            serverinfo["password"]).decode() + "\""


                recovery_db_evn = db_evn
            # backup_info_detail = self.get_backup_info_detail(backup_info)
            #
            backup_sql = backup_info["orginalsql"]

            db_server = self.RevocerServer if self.RevocerServer is not None else backup_info["database"]
            table_name = backup_info["TableName"]

            table_attributes = {"ConnectionStr": db_server, "Name": table_name}
            #
            Server = None
            Port = None
            User = None
            Password = None
            if "Server" in self.recovery_entity:
                    Server = self.recovery_entity["Server"]
                    Port = self.recovery_entity["Port"]
                    User = self.recovery_entity["User"]
                    Password = self.recovery_entity["Password"]
            delete_sql=backup_info["DeleteSql"]
            print("%s   start delete sql " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            self.logs.append(
                "%s   start delete sql " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            if delete_sql is not None and delete_sql!="":
                for sql in delete_sql.split(";"):
                    dbrequest(sql, db_evn, db_server, (Server, Port, User, Password), isSelect=False)
            self.logs.append(
                "%s   end delete sql " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            print("%s   start recovery data " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            #还原数据
            checkresult=BCPExe(q)
            if checkresult==False:
                raise Exception("Recovery data failed[2]")

            print("%s   end recovery data " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            os.remove(filename)
            print("Delete file: " + os.path.join(filename))
            # else:
            #     self.delete_by_original_sql(backup_sql, db_server, recovery_db_evn)
        except Exception as e:
            # print(e)
            raise Exception(e)
        # finally:
        #     os.remove(filename)
            # return RecoveryResponse.recovery_failed(self.recovery_entity["execId"], e.args)

    def recovery_by_backup_id_mysql(self,backup_id, db_evn, backup_evn, is_delete_first,backup_info_list):
        try:
            self.logs.append("%s -----------------backupid: %s-----------------" % (
                str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')), backup_id))
            for i in backup_info_list:
                if list(i.keys())[0] == backup_id:
                    backup_info = i[backup_id];
            # backup_info = backup_info_list[backup_id]  # 根据Backupid查询Backup Cloud Data中的信息
            #####################new type by bcp#################################################
            filepath = backup_info["dbcontent"]
            print("filepath:", filepath)
            # filepath="https://dfis.newegg.org/cdmistest/datacenter/venlertest"
            fileinfo = requests.get(filepath)
            # filename = str(time.time()).replace(".", "") + str(random.randint(10000000, 99999999)) + ".txt"
            if fileinfo.status_code == 200:
                json_info=json.loads(fileinfo.content)

            else:
                raise Exception("Get backupinfo fail.")
                # with open(filename, "wb") as file:
                #     file.write(fileinfo.content)
                # file.close()

            tablename = backup_info["TableName"].replace("[", "").replace("]", "")
            database = backup_info["database"]
                # serverinfo = configcenter.dbconnconfig.get(db_evn).get(database)
            serverinfo = db_dict[db_evn.lower()][database]
           #todo
            backup_sql = backup_info["orginalsql"]

            db_server = self.RevocerServer if self.RevocerServer is not None else backup_info["database"]
            table_name = backup_info["TableName"]

            table_attributes = {"ConnectionStr": db_server, "Name": table_name}
            #
            Server = None
            Port = None
            User = None
            Password = None
            if "Server" in self.recovery_entity:
                Server = self.recovery_entity["Server"]
                Port = self.recovery_entity["Port"]
                User = self.recovery_entity["User"]
                Password = self.recovery_entity["Password"]
            delete_sql = backup_info["DeleteSql"]
            print("%s   start delete sql " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            self.logs.append(
                "%s   start delete sql " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            if delete_sql is not None and delete_sql != "":
                for sql in delete_sql.split(";"):
                    if len(sql)>0:
                        dbrequest(sql, db_evn, db_server, (Server, Port, User, Password), isSelect=False)
            self.logs.append(
                "%s   end delete sql " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            print("%s   start recovery data " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            # 还原数据
            if len(json_info)>0:
                insert_sql=self.get_recovery_mysql_sql(tablename,json_info)
                dbrequest(insert_sql, db_evn, db_server, (Server, Port, User, Password), isSelect=False)

            print("%s   end recovery data " % str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            # else:
            #     self.delete_by_original_sql(backup_sql, db_server, recovery_db_evn)
        except Exception as e:
            # print(e)
            raise Exception(e)
    def get_recovery_mysql_sql(self,tablename,backupinfo):
        count=0
        valus_list=[]
        for data in backupinfo:
            if count==0:
                columns=[]
                # values=[]
                keys=[columns.append(key) for key,value in data.items()]
                # keys="("+str([key for key,value in data.items()])[1:-1]+")".replace("\'","")
                valus="("+str([value for key,value in data.items()])[1:-1]+")"
                valus_list.append(valus)
            else:
                valus = "(" + str([value for key, value in data.items()])[1:-1] + ")"
                valus_list.append(valus)
            count+=1
        columns_str="("+",".join(columns)+")"
        valus_list_to_string=",".join(valus_list).replace("None","null").replace("False","0").replace("True","1")
        sql="INSERT INTO %s %s values %s"%(tablename.lower(),columns_str,valus_list_to_string)
        return sql


    def recovery(self, exec_info):
        try:
            backup_id_list = exec_info[8].split(',')
            db_evn =self.recovery_entity.get('dbEnv')  if 'dbEnv' in self.recovery_entity else exec_info[5]
            if db_evn=="SH_LDEV":
                db_evn="LDEV"
            backup_evn = self.recovery_entity.get('BackupENV')  if 'BackupENV' in self.recovery_entity else exec_info[6]
            is_delete_first = exec_info[9]
            back_id_count = len(backup_id_list)
            backup_info_list=[]
            for backup_id in backup_id_list:
                backup_info = self.query_backup_info(backup_id)
                backup_info_list.append({backup_id: backup_info})
                version = backup_info["version"]
            if (version=='V3.0'):
                for backup_id in backup_id_list:
                    # self.recovery_by_backup_id_new(backup_id, db_evn, backup_evn, is_delete_first,backup_info_list)
                    t = threading.Thread(target=self.recovery_by_backup_id_new, args=(backup_id, db_evn, backup_evn, is_delete_first,backup_info_list))
                    t.start()
                t.join()
            elif(version=='V4.0'):
                for backup_id in backup_id_list:
                    t = threading.Thread(target=self.recovery_by_backup_id_mysql,
                                         args=(backup_id, db_evn, backup_evn, is_delete_first, backup_info_list))
                    t.start()
            else:
                for backup_id in backup_id_list:
                    t = threading.Thread(target=self.recovery_by_backup_id, args=(backup_id, db_evn, backup_evn, is_delete_first))
                    t.start()
                t.join()
            self.update_exec_records_by_execid()
            print(self.logs)
            return RecoveryResponse.recovery_successful(self.recovery_entity["execId"], back_id_count)
        except Exception as e:
            raise Exception(RecoveryResponse.recovery_failed(self.recovery_entity["execId"], e.args[0]))
    def insert_recovery_log(self,exec_info,reusltinfo):
        if reusltinfo.get('successed',None)==True:
            result="Succeed"
        else:
            result = "Failed"
        par_recovery_request = {"ExecID": exec_info['execId'], "ExecEnv": exec_info['dbEnv'],
                            "Inuser": exec_info['userId'],"Status":result}
        insert_reconvery_log_sql=configcenter.recoveryconfig.get("insert_recovery_log")
        try:
            db.session.execute(insert_reconvery_log_sql, par_recovery_request)
        except Exception as e:
            raise Exception("Insert Recovery log failed!")
