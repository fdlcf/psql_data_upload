
import func
import path
import users

conn = func.connect_to_psql(users.dwh_param_dic_lyzin)
df = func.get_df(path.filepath, path.sheet_name)
func.execute_values_with_brakets(conn, df, path.table1)
