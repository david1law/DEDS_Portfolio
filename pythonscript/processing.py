from settings import settings, logger
# %% [markdown]
# ## Setup

# %%
import pandas as pd
import sqlite3 as s3
import pyodbc
from datetime import datetime
import warnings
warnings.simplefilter('ignore')

DB = {'servername': 'NOAH\SQLEXPRESS01',
      'database': '4.3 db3'}

export_conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
export_cursor = export_conn.cursor()
export_cursor

go_sales = s3.connect('go_sales.sqlite')
go_staff = s3.connect('go_staff.sqlite')
go_crm = s3.connect('go_crm.sqlite')

product = pd.read_sql_query('SELECT * FROM product', go_sales)
product_type = pd.read_sql_query('SELECT * FROM product_type', go_sales)
sales_staff = pd.read_sql_query('SELECT * FROM sales_staff', go_sales)
sales_branch = pd.read_sql_query('SELECT * FROM sales_branch', go_sales)
staff_manager = pd.read_sql_query('SELECT * FROM sales_staff', go_staff)
order_method = pd.read_sql_query('SELECT * FROM order_method', go_sales)
return_reason = pd.read_sql_query('SELECT * FROM return_reason', go_sales)
retailer_contact = pd.read_sql_query('SELECT * FROM retailer_contact', go_crm)
course = pd.read_sql_query('SELECT * FROM course', go_staff)
satisfaction_type = pd.read_sql_query('SELECT * FROM satisfaction_type', go_staff)
order_details = pd.read_sql_query('SELECT * FROM order_details', go_sales)
returned_item = pd.read_sql_query('SELECT * FROM returned_item', go_sales)
order_header = pd.read_sql_query('SELECT * FROM order_header', go_sales)
sales_target = pd.read_sql_query('SELECT * FROM sales_targetDATA', go_sales)
training = pd.read_sql_query('SELECT * FROM training', go_staff)
satisfaction = pd.read_sql_query('SELECT * FROM satisfaction', go_staff)
inventory = pd.read_csv('GO_SALES_INVENTORY_LEVELSData.csv')
product_forecast = pd.read_csv('GO_SALES_PRODUCT_FORECASTData.csv')

# %%
oude_inventory = inventory.reset_index()
nieuwste_inventory = oude_inventory.shift(axis = 1)
nieuwste_inventory

# %% [markdown]
# ### PRODUCT

# %%
merged = pd.merge(product, product_type, left_on='PRODUCT_TYPE_CODE', how='inner', right_on='PRODUCT_TYPE_CODE')
new_product = merged[['PRODUCT_NUMBER', 'PRODUCT_NAME', 'DESCRIPTION', 'INTRODUCTION_DATE', 'PRODUCT_TYPE_CODE', 'PRODUCTION_COST', 'MARGIN', 'PRODUCT_IMAGE', 'LANGUAGE']]
new_product

for index, row in new_product.iterrows():
    try:
        query = f"INSERT INTO Product VALUES ({row['PRODUCT_NUMBER']}, '{row['PRODUCT_NAME'].replace("'", "''")}', '{row['DESCRIPTION'].replace("'", "''")}', '{row['INTRODUCTION_DATE']}', {row['PRODUCT_TYPE_CODE']}, '{row['PRODUCTION_COST']}', '{row['MARGIN']}', '{row['PRODUCT_IMAGE']}', '{row['LANGUAGE']}')"
        export_cursor.execute(query)
    except pyodbc.Error as e:
        print(e)
        print(query)

export_conn.commit()

# %% [markdown]
# ### SALES_STAFF

# %%
merged = pd.merge(staff_manager, sales_branch, left_on='SALES_BRANCH_CODE', how='inner', right_on='SALES_BRANCH_CODE')
new_staff = merged[['SALES_STAFF_CODE', 'WORK_PHONE', 'FAX', 'EMAIL', 'FIRST_NAME', 'LAST_NAME', 'POSITION_EN', 'EXTENSION', 'DATE_HIRED', 'SALES_BRANCH_CODE', 'MANAGER_CODE']]
new_staff['STAFF_SK'] = range(1, len(new_staff) + 1)
new_staff.set_index('STAFF_SK', inplace = True)
new_staff.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))

for index, row in new_staff.iterrows():
    try:
        query = f"INSERT INTO Sales_staff VALUES ({row.name}, '{row['TIMESTAMP']}', {row['SALES_STAFF_CODE']}, '{row['WORK_PHONE']}', '{row['FAX']}', '{row['EMAIL']}', '{row['FIRST_NAME']}', '{row['LAST_NAME'].replace("'", "''")}', '{row['POSITION_EN']}', '{row['EXTENSION']}', '{row['DATE_HIRED']}', '{row['SALES_BRANCH_CODE']}', '{row['MANAGER_CODE']}')"
        export_cursor.execute(query)
    except pyodbc.Error as e:
        print(e)
        print(query)

export_conn.commit()

# %% [markdown]
# ### ORDER_METHOD

# %%
new_order_method = order_method[['ORDER_METHOD_CODE', 'ORDER_METHOD_EN']]
new_order_method

for index, row in new_order_method.iterrows():
    try:
        query = f"INSERT INTO Order_method VALUES ({row['ORDER_METHOD_CODE']}, '{row['ORDER_METHOD_EN']}')"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()


# %% [markdown]
# ### RETURN_REASON

# %%
new_return_reason = return_reason[['RETURN_REASON_CODE', 'RETURN_DESCRIPTION_EN']]
new_return_reason

for index, row in new_return_reason.iterrows():
    try:
        query = f"INSERT INTO Return_reason VALUES ({row['RETURN_REASON_CODE']}, '{row['RETURN_DESCRIPTION_EN']}')"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()

# %% [markdown]
# ### RETAILER_CONTACT

# %%
new_retailer_contact = retailer_contact[['RETAILER_CONTACT_CODE', 'FAX', 'E_MAIL', 'RETAILER_SITE_CODE', 'FIRST_NAME', 'LAST_NAME', 'JOB_POSITION_EN', 'EXTENSION', 'GENDER']]
new_retailer_contact

for index, row in new_retailer_contact.iterrows():
    try:
        last_name = row['LAST_NAME'].replace("'", "''")
        query = f"INSERT INTO Retailer_contact VALUES ({row['RETAILER_CONTACT_CODE']}, '{row['FAX']}', '{row['E_MAIL']}', '{row['RETAILER_SITE_CODE']}', '{row['FIRST_NAME']}', '{last_name}', '{row['JOB_POSITION_EN']}', '{row['EXTENSION']}', '{row['GENDER']}')"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()

# %% [markdown]
# ### COURSE

# %%
new_course = course[['COURSE_CODE', 'COURSE_DESCRIPTION']]
new_course

for index, row in new_course.iterrows():
    try:
        query = f"INSERT INTO Course VALUES ({row['COURSE_CODE']}, '{row['COURSE_DESCRIPTION']}')"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()

# %% [markdown]
# ### SATISFACTION_TYPE

# %%
new_satisfaction = satisfaction_type[['SATISFACTION_TYPE_CODE', 'SATISFACTION_TYPE_DESCRIPTION']]
new_satisfaction

for index, row in new_satisfaction.iterrows():
    try:
        query = f"INSERT INTO Satisfaction_type VALUES ({row['SATISFACTION_TYPE_CODE']}, '{row['SATISFACTION_TYPE_DESCRIPTION']}')"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()


# %% [markdown]
# ### ORDER_DETAILS

# %%
merged = pd.merge(order_details, order_header, left_on='ORDER_NUMBER', how='inner', right_on='ORDER_NUMBER')
merged_order = pd.merge(merged, returned_item, left_on='ORDER_DETAIL_CODE', how='inner', right_on='ORDER_DETAIL_CODE')
new_order = merged_order[['ORDER_DETAIL_CODE', 'RETURN_CODE', 'ORDER_NUMBER', 'RETURN_DATE', 'RETURN_QUANTITY', 'UNIT_COST', 'UNIT_PRICE', 'ORDER_DATE', 'RETURN_REASON_CODE', 'PRODUCT_NUMBER', 'RETAILER_CONTACT_CODE', 'RETAILER_NAME', 'RETAILER_SITE_CODE', 'SALES_STAFF_CODE', 'ORDER_METHOD_CODE']]
new_order['ORDER_DETAILS_SK'] = range(1, len(new_order) + 1)
new_order.set_index('ORDER_DETAILS_SK', inplace = True)
new_order.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))
new_order

for index, row in new_order.iterrows():
    try:
        query = f"INSERT INTO Order_details VALUES ({row.name}, '{row['TIMESTAMP']}', {row['ORDER_DETAIL_CODE']}, '{row['RETURN_CODE']}', {row['ORDER_NUMBER']}, '{row['RETURN_DATE']}', {row['RETURN_QUANTITY']}, {row['UNIT_COST']}, {row['UNIT_PRICE']}, '{row['ORDER_DATE']}', {row['RETURN_REASON_CODE']}, {row['PRODUCT_NUMBER']}, {row['RETAILER_CONTACT_CODE']}, '{row['RETAILER_NAME'].replace("'", "''")}', {row['RETAILER_SITE_CODE']}, {row['SALES_STAFF_CODE']}, {row['ORDER_METHOD_CODE']})"
        export_cursor.execute(query)
    except pyodbc.Error as e:
        print(e)
        print(query)


export_conn.commit()

# %% [markdown]
# ### PRODUCT_FORECAST

# %%
new_product_forecast = product_forecast[['PRODUCT_NUMBER', 'YEAR', 'MONTH', 'EXPECTED_VOLUME']]
new_product_forecast.insert(0, 'PROD_NUMBER_YEAR_MONTH', new_product_forecast['YEAR'].astype(str) + '-' + new_product_forecast['MONTH'].astype(str) + '-' + new_product_forecast['PRODUCT_NUMBER'].astype(str))
new_product_forecast['PRODUCT_FORECAST_SK'] = range(1, len(new_product_forecast) + 1)
new_product_forecast.set_index('PRODUCT_FORECAST_SK', inplace = True)
new_product_forecast.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))
new_product_forecast

for index, row in new_product_forecast.iterrows():
    try:
        query = f"INSERT INTO Product_forecast VALUES ({row.name}, '{row['TIMESTAMP']}', {row['PRODUCT_NUMBER']}, {row['YEAR']}, {row['MONTH']}, {row['EXPECTED_VOLUME']})"
        export_cursor.execute(query)
    except pyodbc.Error as e:
        print(e)
        print(query)

export_conn.commit()

# %% [markdown]
# ### INVENTORY_LEVELS

# %%
new_inventory = nieuwste_inventory[['INVENTORY_YEAR', 'INVENTORY_MONTH', 'PRODUCT_NUMBER', 'INVENTORY_COUNT']]
new_inventory.insert(0, 'YEAR_MONTH_PROD_NUMBER', new_inventory['INVENTORY_YEAR'].astype(str) + '-' + new_inventory['INVENTORY_MONTH'].astype(str) + '-' + new_inventory['PRODUCT_NUMBER'].astype(str))
new_inventory['INVENTORY_SK'] = range(1, len(new_inventory) + 1)
new_inventory.set_index('INVENTORY_SK', inplace = True)
new_inventory.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))
new_inventory

for index, row in new_inventory.iterrows():
    try:
        row = row.fillna(0)
        query = f"INSERT INTO INVENTORY_LEVELS VALUES ({row.name}, '{row['TIMESTAMP']}', {row['INVENTORY_YEAR']}, {row['INVENTORY_MONTH']}, {row['PRODUCT_NUMBER']}, {row['INVENTORY_COUNT']})"
        export_cursor.execute(query)
    except pyodbc.Error as e:
        print(e)
        print(query)

export_conn.commit()

# %% [markdown]
# ### SALES_TARGET_DATA

# %%
new_sales_target = sales_target[[ 'SALES_YEAR', 'SALES_PERIOD', 'SALES_STAFF_CODE', 'PRODUCT_NUMBER', 'SALES_TARGET', 'RETAILER_CODE', 'Id']]
new_sales_target = new_sales_target.rename(columns = {'Id' : 'ID'})
new_sales_target['SALES_TARGET_DATA_SK'] = range(1, len(new_sales_target) + 1)
new_sales_target.set_index('SALES_TARGET_DATA_SK', inplace = True)
new_sales_target.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))
new_sales_target

for index, row in new_sales_target.iterrows():
    try:
        query = f"INSERT INTO Sales_target_data VALUES ({row.name}, '{row['TIMESTAMP']}', {row['SALES_YEAR']}, {row['SALES_PERIOD']}, {row['SALES_STAFF_CODE']}, {row['SALES_TARGET']}, {row['RETAILER_CODE']},  {row['ID']})"
        export_cursor.execute(query)
    except pyodbc.Error as e:
        print(e)
        print(query)

export_conn.commit()

# %% [markdown]
# ### TRAINING

# %%
new_training = training[['COURSE_CODE', 'YEAR', 'SALES_STAFF_CODE']]
new_training['TRAINING_SK'] = range(1, len(new_training) + 1)
new_training.set_index('TRAINING_SK', inplace = True)
new_training.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))
new_training

for index, row in new_training.iterrows():
    try:
        query = f"INSERT INTO Training VALUES ({row.name}, '{row['TIMESTAMP']}', {row['COURSE_CODE']}, {row['YEAR']}, {row['SALES_STAFF_CODE']})"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()

# %% [markdown]
# ### SATISFACTION

# %%
new_satisfaction = satisfaction[['SATISFACTION_TYPE_CODE', 'YEAR', 'SALES_STAFF_CODE']]
new_satisfaction['SATISFACTION_SK'] = range(1, len(new_satisfaction) + 1)
new_satisfaction.set_index('SATISFACTION_SK', inplace = True)
new_satisfaction.insert(0, 'TIMESTAMP', datetime.now().replace(microsecond=0))
new_satisfaction

for index, row in new_satisfaction.iterrows():
    try:
        query = f"INSERT INTO Satisfaction VALUES ({row.name}, '{row['TIMESTAMP']}', {row['SATISFACTION_TYPE_CODE']}, {row['YEAR']}, {row['SALES_STAFF_CODE']})"
        export_cursor.execute(query)
    except pyodbc.Error:
        print(query)

export_conn.commit()



logger.info(f"Radius {type_activity}: {activity_radius}")