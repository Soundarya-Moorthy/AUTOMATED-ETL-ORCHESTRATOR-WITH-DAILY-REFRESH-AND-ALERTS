
from load.load_dimensions import load_dim_date, load_dim_city, load_dim_stock
from load.load_facts import load_fact_weather, load_fact_finance

def run_warehouse_load():
    print("Loading Dimension Tables...")
    load_dim_date()
    load_dim_city()
    load_dim_stock()

    print("Loading Fact Tables...")
    load_fact_weather()
    load_fact_finance()

    print("Warehouse Load Completed Successfully")

if __name__ == "__main__":
    run_warehouse_load()
