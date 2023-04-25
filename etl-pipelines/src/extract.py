import psycopg2
import pandas as pd


# connect to redshift

def connect_to_redshift(dbname, host, port, user, password):
    """Method that connects to redshift. This gives a warning so will look for another solution"""

    connect = psycopg2.connect(
        dbname=dbname, host=host, port=port, user=user, password=password
    )

    print("connection to redshift made")

    return connect


def extract_transactional_data(dbname, host, port, user, password):
    """This method connects to redshift and extracts customer transactions data.
     1. Removes all customers with no customer id
     2. Joins descriptions to do the online transaction table, and replaces missing values and ? with Unknown
     3. Removes all invoices that have the stock code postage, bank changes, d, m and cruk
     4. Removes all invoices where quantity is less than 0
     """

    # connect to redshift
    connect = connect_to_redshift(dbname, host, port, user, password)

    # write query that extract data

    query = """
        SELECT ot.*,
                case when sd.description = '?' or sd.description is null then 'Unknown' else sd.description end as description
        FROM bootcamp1.online_transactions ot
        LEFT JOIN bootcamp1.stock_description sd ON ot.stock_code = sd.stock_code
        WHERE ot.customer_id <> '' 
        AND ot.stock_code NOT IN ('BANK CHARGES', 'POSTAGE', 'D', 'M', 'CRUK')
        AND ot.quantity > 0 """

    online_transactions_reduced = pd.read_sql(query, connect)

    print(f"The dataframe contains {online_transactions_reduced.shape[0]} invoices")

    return online_transactions_reduced
