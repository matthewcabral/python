from .Controller import Controller, psycopg2, pd, errors, mysql, json, IntegrityError, string

class DataController(Controller):

    # Class constructor
    def __init__(self):
        super().__init__()

    # Function to open database connection
    def open_db_connection(self):
        try:
            if super().get_DB_DRIVER().upper() == "ORACLE":
                conn = psycopg2.connect(database=super().get_DB_NAME(), user=super().get_DB_USER(), password=super().get_DB_PASSWORD(), host=super().get_DB_HOST(), port=super().get_DB_PORT())
            elif super().get_DB_DRIVER().upper() == "MYSQL":
                conn = mysql.connector.connect(user=super().get_DB_USER(), password=super().get_DB_PASSWORD(), host=super().get_DB_HOST(), port=super().get_DB_PORT(), database=super().get_DB_NAME())
            else:
                conn = psycopg2.connect(database=super().get_DB_NAME(), user=super().get_DB_USER(), password=super().get_DB_PASSWORD(), host=super().get_DB_HOST(), port=super().get_DB_PORT())
            return conn
        except Exception as e:
            print(f"[ERROR] Error while trying to connect to the database. - {e}")
            return False

    # Function to close database connection
    def close_db_connection(self, conn):
        try:
            conn.close()
            return True
        except Exception as e:
            print(f"[ERROR] Error closing the database connection. - {e}")
            return False

    # Function to insert/update/delete from tables on the database
    def exec_db_cmd(self, command):
        conn = self.open_db_connection()
        if conn != False:
            rowcount = 0
            try:
                with conn.cursor() as cursor:
                    # print(f"[INFO] Executing database command...\t-\t[COMMAND] {command}")
                    cursor.execute(command)
                    rowcount = cursor.rowcount
                    cursor.close()
                    conn.commit()
            except (psycopg2.errors.Error, IntegrityError, Exception) as e:
                # print(e)
                self.close_db_connection(conn)
                return False, 0, str(e)
            finally:
                self.close_db_connection(conn)
        else:
            print(f"[ERROR] Error connecting to the database")
            return False, 0, ""
        return True, rowcount, "Success"

    # Return the SYSTEM DATE format according to the database
    def get_sysdate(self):
        sysdate = "CURRENT_TIMESTAMP(6)"
        db_driver_upper = super().get_DB_DRIVER().upper()

        if "ORACLE" in db_driver_upper:
            sysdate = "SYSDATE"
        elif "MYSQL" in db_driver_upper:
            sysdate = "CURRENT_TIMESTAMP(6)"  # Assuming MySQL syntax for current timestamp
        else:
            sysdate = "CURRENT_TIMESTAMP(6)"  # Default for unknown databases

        return sysdate

    # Date conversion
    def convert_Date(self, date):
        date_converted = ""
        db_driver_upper = super().get_DB_DRIVER().upper()

        if "ORACLE" in db_driver_upper:
            date_converted = f"TO_DATE(TO_CHAR('{date}', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')"
        elif "MYSQL" in db_driver_upper:
            date_converted = f"DATE_FORMAT(STR_TO_DATE('{date}', '%m/%d/%Y %H:%i:%s'), '%Y-%m-%d %H:%i:%s')"
        else:
            date_converted = f"TO_DATE(TO_CHAR('{date}', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')"

        return date_converted

    # function to query in the database tables
    def query_record(self, table, columns, condition):
        array_columns = ""
        sql_header = "SELECT"
        sql_column = ""
        sql_table = f"\nFROM {super().get_DB_OWNER()}.{table}"
        sql_condition = "\nWHERE "
        sql_final_cmd = ""
        found_asterisk = False
        count_columns = 0
        count_columns = len(columns)
        return_array = []

        if table == None or table == "":
            return "[ERROR] Table is Mandatory!!!"
        elif count_columns < 1:
            return "[ERROR] No columns provided for table QUERY."
        else:
            for i in range(count_columns):
                if i > 0:
                    array_columns += f",\n\t" + columns[i]
                else:
                    array_columns += f"\n\t" + columns[i]

                if columns[i] == '*':
                    found_asterisk = True

            # if by any chance the user add the asterisk to the array, I overwrite the array_columns with the asterisk
            if found_asterisk:
                array_columns = " *"

            sql_column += array_columns
            sql_condition += condition if condition is not None else "1=1"
            sql_final_cmd += sql_header + sql_column + sql_table + sql_condition
            # print(f"[INFO] Querying record from database...\t-\t[COMMAND] {sql_final_cmd}")
            conn = self.open_db_connection()
            if conn != False:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_final_cmd)
                        columns = [desc[0].upper() for desc in cursor.description]
                        rows = cursor.fetchall()
                        cursor.close()
                        if columns and rows:
                            for row in rows:
                                row_dict = dict(zip(columns, row))
                                return_array.append(row_dict)
                        return True, return_array
                except (psycopg2.errors.Error, Exception) as e:
                    print(f"[ERROR] Error executing the query on the database: {e}")
                    return False, None
                finally:
                    self.close_db_connection(conn)
            else:
                return False, None

    # Insert records into database tables
    def insert_record(self, table, columns, values):
        row_Inserted_Flag = False
        array_columns = ""
        array_values = ""
        sql_header = ""
        sql_column = ""
        sql_value = ""
        sql_final_cmd = ""
        count_columns = 0
        count_values = 0

        return_string = "Success"

        count_columns = len(columns)
        count_values = len(values)

        if table == None or table == "":
            return row_Inserted_Flag, 0, "[ERROR] Table is Mandatory!!!"
        elif count_columns < 1:
            return row_Inserted_Flag, 0, "[ERROR] No columns provided for table INSERT."
        elif count_values < 1:
            return row_Inserted_Flag, 0, "[ERROR] No values provided for table INSERT."
        elif count_columns != count_values:
            return row_Inserted_Flag, 0, f"[ERROR] Some of the columns or values are missing. Total: Columns = {count_columns} - Values = {count_values}"
        else:
            for i in range(len(columns)):
                array_columns += ", " + str(columns[i])

            for j in range(len(values)):
                if type(values[j]) == str:
                    if values[j] == 'CURRENT_TIMESTAMP(6)' or values[j] == 'SYSDATE' or values[j] == 'SYSDATE()' or values[j] == 'NULL' or values[j] == 'null':
                        array_values += ", " + str(values[j])
                    else:
                        array_values += ", '" + str(values[j]) + "'"
                else:
                    array_values += ", " + str(values[j])

            sql_header += f"""INSERT INTO {super().get_DB_OWNER()}.{table}"""
            sql_column += f""" (ROW_ID, CREATED, CREATED_BY, LAST_UPD, LAST_UPD_BY, MODIFICATION_NUM, DB_LAST_UPD{array_columns})"""
            sql_value += f""" \nVALUES ((SELECT NEXT_ID FROM {super().get_DB_OWNER()}.{super().get_tbl_ID()} WHERE ROW_ID = '0-1'), {self.get_sysdate()}, '0-1', {self.get_sysdate()}, '0-1', 0, {self.get_sysdate()}{array_values})"""

            sql_final_cmd += sql_header + sql_column + sql_value
            row_Inserted_Flag, rowcount, return_string = self.exec_db_cmd(sql_final_cmd)

            if row_Inserted_Flag == True:
                #self.generate_next_id()
                self.generate_next_id()

        return row_Inserted_Flag, rowcount, return_string

    # Update records into database tables
    def update_record(self, table, columns, values, condition):
        return_string = "Success"
        row_updated_flag = False
        array_columns_values = ""
        sql_header = f"""UPDATE {super().get_DB_OWNER()}.{table}\n"""
        sql_column_values = "SET "
        sql_condition = f"\nWHERE {condition}"
        sql_final_cmd = ""
        count_columns = 0
        count_values = 0

        count_columns = len(columns)
        count_values = len(values)

        if table == None or table == "":
            return row_updated_flag, 0, "[ERROR] Table is Mandatory!!!"
        elif condition == None or condition == "":
            return row_updated_flag, 0, "[ERROR] Condition is Mandatory!!!"
        elif count_columns < 1:
            return row_updated_flag, 0, "[ERROR] No columns provided for table UPDATE."
        elif count_values < 1:
            return row_updated_flag, 0, "[ERROR] No values provided for table UPDATE."
        elif count_columns != count_values:
            return row_updated_flag, 0, f"[ERROR] Some of the columns or values are missing. Total: Columns = {count_columns} - Values = {count_values}"
        else:
            for i in range(len(columns)):
                if i > 0:
                    array_columns_values += f",\n\t"
                else:
                    array_columns_values += f"\t"

                if type(values[i]) == str:
                    if values[i] == 'CURRENT_TIMESTAMP(6)' or values[i] == 'SYSDATE' or values[i] == 'SYSDATE()' or values[i] == 'NULL' or values[i] == 'null':
                        array_columns_values += str(columns[i]).upper() +  " = " + str(values[i])
                    else:
                        array_columns_values += str(columns[i]).upper() +  " = '" + str(values[i]) + "'"
                else:
                    array_columns_values += str(columns[i]).upper() +  " = " + str(values[i])

            if table is not super().get_tbl_ID():
                array_columns_values += ",\n\tMODIFICATION_NUM = MODIFICATION_NUM"
                array_columns_values += ",\n\tLAST_UPD = " + self.get_sysdate()
                array_columns_values += ",\n\tLAST_UPD_BY = '0-1'"
                array_columns_values += ",\n\tDB_LAST_UPD = " + self.get_sysdate()

            sql_column_values += array_columns_values
            sql_final_cmd += sql_header + sql_column_values + sql_condition
            row_updated_flag, rowcount, return_string = self.exec_db_cmd(sql_final_cmd)

        return row_updated_flag, rowcount, return_string

    # Delete records into database tables
    def delete_record(self, table, condition):
        return_string = "Success"

        row_deleted_flag = False
        sql_header = f"""DELETE\nFROM {super().get_DB_OWNER()}.{table}"""
        sql_condition = f"\nWHERE {condition}"
        sql_final_cmd = ""

        if table == None or table == "":
            return False, 0, "[ERROR] Table is Mandatory!!!"
        elif condition == None or condition == "":
            return False, 0, "[ERROR] Condition is Mandatory!!!"
        else:
            sql_final_cmd += sql_header + sql_condition
            #print(f"[INFO] Deleting record from database...\t-\t[COMMAND] {sql_final_cmd}")
            row_deleted_flag, rowcount, return_string = self.exec_db_cmd(sql_final_cmd)

        return row_deleted_flag, rowcount, return_string

    # Function to generate ROW ID (PLEASE, DO NOT CHANGE THIS TO AVOID ERRORS!)
    def generate_next_id(self):
        prefix = '0'
        suffix = '0'
        counter = 0
        modification_num = 0
        prefix_length = 5  # Set the desired length for the prefix
        next_id = ""
        array_columns = ['LAST_UPD', 'LAST_UPD_BY', 'DB_LAST_UPD', 'MODIFICATION_NUM', 'NEXT_ID', 'PREFIX', 'SUFFIX', 'COUNTER']
        array_values = [self.get_sysdate(), '0-1', self.get_sysdate()]
        condition = "ROW_ID = '0-1'"

        result_flag, result_query = self.query_record(super().get_tbl_ID(), ['*'], condition)

        if result_flag:
            for row in result_query:
                counter = row["COUNTER"]
                prefix = row["PREFIX"]
                suffix = row["SUFFIX"]
                modification_num = row["MODIFICATION_NUM"]
                next_id = row["NEXT_ID"]

            characters = string.digits + string.ascii_uppercase
            #for i in range(100000000):
            suffix_length = len(str(suffix))
            counter += 1
            keep_checking = True

            if counter >= 36:
                counter = 0
                if suffix == 'ZZZZZZZZZ':
                    for i in range(6):
                        if keep_checking:
                            if str(prefix)[len(prefix) - 1 - i] == 'Z':
                                prefix = str(prefix)[0:len(prefix) - 1 - i] + str(characters[0]) + str(prefix)[len(prefix) - i:]
                                keep_checking = True
                            else:
                                if i == len(prefix) + 1:
                                    prefix = str(prefix)[0:len(prefix) - 1 - i] + str(characters[characters.find(str(prefix)[len(prefix) - 1 - i]) + 1]) + str(prefix)[len(prefix) - i:]
                                else:
                                    if len(prefix) - 1 - i < 0:
                                        prefix = str(characters[characters.find(str(prefix)[len(prefix) - 1 - i]) + 1]) + str(prefix)[len(prefix) - i:]
                                    else:
                                        prefix = str(prefix)[0:len(prefix) - 1 - i] + str(characters[characters.find(str(prefix)[len(prefix) - 1 - i]) + 1]) + str(prefix)[len(prefix) - i:]

                                keep_checking = False
                    suffix = '0'
                else:
                    for i in range(suffix_length + 1):
                        if keep_checking:
                            if str(suffix)[suffix_length - 1 - i] == 'Z':
                                suffix = str(suffix)[0:suffix_length - 1 - i] + str(characters[0]) + str(suffix)[suffix_length - i:]
                                keep_checking = True
                            else:
                                if i == suffix_length + 1:
                                    suffix = str(suffix)[0:suffix_length - 1 - i] + str(characters[characters.find(str(suffix)[suffix_length - 1 - i]) + 1]) + str(suffix)[suffix_length - i:]
                                else:
                                    if suffix_length - 1 - i < 0:
                                        suffix = str(characters[characters.find(str(suffix)[suffix_length - 1 - i]) + 1]) + str(suffix)[suffix_length - i:]
                                    else:
                                        suffix = str(suffix)[0:suffix_length - 1 - i] + str(characters[characters.find(str(suffix)[suffix_length - 1 - i]) + 1]) + str(suffix)[suffix_length - i:]

                                keep_checking = False
                        else:
                            break
            else:
                suffix = str(suffix)[0:suffix_length - 1] + str(characters[counter])

            suffix_length = len(str(suffix))
            zeros_to_add_suffix = max(15 - (prefix_length + 1 + suffix_length), 0)
            zeros_string_suffix = ''

            for i in range(zeros_to_add_suffix):
                zeros_string_suffix += '0'

            next_id = f"{prefix}-{zeros_string_suffix}{suffix:0{suffix_length}}"

            array_values.append(modification_num + 1)
            array_values.append(next_id)
            array_values.append(prefix)
            array_values.append(suffix)
            array_values.append(counter)

            return self.update_record(super().get_tbl_ID(), array_columns, array_values, condition)

        else:
            return "[ERROR] No columns or rows found."
