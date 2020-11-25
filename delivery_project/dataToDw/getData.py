import pandas as pd 
import pyodbc 

from os import getenv

print('Script iniciado!')

def main():
    try:
        server = 'localhost' 
        database = 'meal_dw' 
        username = 'sa' 
        password = '********' 
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
        cursor = cnxn.cursor()

        meal_info = pd.read_csv('/Projects/meal_info.csv')
        center_info = pd.read_csv('/Projects/fulfilment_center_info.csv')
        train = pd.read_csv('/Projects/train.csv')

        df_meal = pd.DataFrame(meal_info, columns=['meal_id', 'category', 'cuisine'])
        df_center = pd.DataFrame(center_info, columns=['center_id', 'city_code', 'region_code', 'center_type', 'op_area'])
        df_train = pd.DataFrame(train, columns=['id','week','center_id','meal_id','checkout_price','base_price','emailer_for_promotion','homepage_featured','num_orders'])

        for row in df_meal.itertuples():
            cursor.execute("""
                        INSERT INTO meal_info (meal_id, category, cuisine)
                        VALUES (?,?,?)
                        """,
                        (row.meal_id,
                        row.category,
                        row.cuisine))

        for row in df_center.itertuples():
            cursor.execute("""
                        INSERT INTO center_info (center_id, city_code, region_code, center_type, op_area)
                        VALUES
                        (?,?,?,?,?)
                        """,
                        (row.center_id,
                        row.city_code,
                        row.region_code,
                        row.center_type,
                        row.op_area
                        ))  

        for row in df_train.itertuples():
            cursor.execute("""
                        INSERT INTO weekly_demand (id, week, center_id, meal_id, checkout_price, base_price, emailer_for_promotion, homepage_featured, num_orders)
                        VALUES                     
                        (?,?,?,?,?,?,?,?,?)
                        """,
                        (row.id,
                        row.week,
                        row.center_id,
                        row.meal_id,
                        row.checkout_price, 
                        row.base_price,
                        row.emailer_for_promotion,
                        row.homepage_featured,
                        row.num_orders
                        ))

        cnxn.commit()        
        print('Script finalizado, dados inseridos!')
    
    except NameError as error:
        print('Erro: ', error)

main()
