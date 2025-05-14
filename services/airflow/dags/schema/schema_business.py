from schema.Table import Table

class ItemReport(Table):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.table_name = table_name
        self.iceberg_db = "sales_business"
        self.COLUMNS = [
            {"name": "date", "type": "DATE", "comment": "Date"},
            {"name": "item_nbr", "type": "INT", "comment": "Item Number"},
            {"name": "family", "type": "STRING", "comment": "Family"},
            {"name": "class", "type": "STRING", "comment": "Class"},
            {"name": "perishable", "type": "INT", "comment": "Perishable"},
            {"name": "city", "type": "STRING", "comment": "City"},
            {"name": "state", "type": "STRING", "comment": "State"},
            {"name": "unit_sales", "type": "INT", "comment": "Unit Sales"}
        ]
        self.SQL = "sql/business/load_agg_item_report.sql"

class StoreReport(Table):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.table_name = table_name
        self.iceberg_db = "sales_business"
        self.COLUMNS = [
            {"name": "date", "type": "DATE", "comment": "Date"},
            {"name": "store_nbr", "type": "INT", "comment": "Store Number"},
            {"name": "city", "type": "STRING", "comment": "City"},
            {"name": "state", "type": "STRING", "comment": "State"},
            {"name": "dcoilwtico", "type": "DOUBLE", "comment": "DCOILWTICO"},
            {"name": "unit_sales", "type": "INT", "comment": "Unit Sales"},
            {"name": "transactions", "type": "INT", "comment": "Transactions"}
        ]
        self.SQL = "sql/business/load_agg_store_report.sql"

item_report = ItemReport("item_report")
store_report = StoreReport("store_report")

ALL_TABLES = [item_report, store_report]
