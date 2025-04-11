from airflow.models import Variable
from typing import Any, Dict

def get_variables(
        name,
        deserialize_json=True,
        default_var={}
) -> Dict[str, Any]:
    variables = Variable.get(
        key=name,
        deserialize_json=deserialize_json,
        default_var=default_var
    )
    return variables

def generate_create_table_sql(iceberg_db, iceberg_table, iceberg_columns_properties, location):
    with open("../sql/common/create_table_template.sql", 'r') as sql_file:
        template = sql_file.read()
    sql = template.format(
        iceberg_db=iceberg_db,
        iceberg_table=iceberg_table,
        iceberg_columns_properties=iceberg_columns_properties,
        location=location
    )
    return sql

