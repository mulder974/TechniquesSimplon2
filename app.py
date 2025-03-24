import os
import requests
import psycopg2
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

app = Flask(__name__)

def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def create_tables():
    conn = get_connection()
    cur = conn.cursor()
    
    # Create table for shops first, as it is referenced by sells
    create_shops_table = """
    CREATE TABLE IF NOT EXISTS shops (
        id SERIAL PRIMARY KEY,
        shop_id INT UNIQUE,
        town TEXT,
        employees_number INT
    );
    """
    
    # Create table for products
    create_products_table = """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        product_reference TEXT UNIQUE,
        price NUMERIC(10, 2),
        stock INT
    );
    """
    
    # Create table for sells with a foreign key reference to shops
    create_sells_table = """
    CREATE TABLE IF NOT EXISTS sells (
        id SERIAL PRIMARY KEY,
        date DATE,
        product_reference TEXT,
        quantity INT,
        shop_id INT,
        FOREIGN KEY (shop_id) REFERENCES shops(shop_id) ON DELETE CASCADE
    );
    """
    
    # Execute table creation queries
    cur.execute(create_shops_table)
    cur.execute(create_products_table)
    cur.execute(create_sells_table)
    
    conn.commit()
    cur.close()
    conn.close()

def import_data_from_csv(csv_url, table_name):
    conn = get_connection()
    cur = conn.cursor()

    # Fetch CSV data from URL
    resp = requests.get(csv_url)
    df = pd.read_csv(StringIO(resp.text), encoding='utf-8')
    print(df)
    print(df.columns)

    # Insert data into the specified table
    for _, row in df.iterrows():
        if table_name == 'sells':
            cur.execute(
                "INSERT INTO sells (date, product_reference, quantity, shop_id) "
                "VALUES (%s, %s, %s, %s)",
                (row["Date"], row["ID RÃ©fÃ©rence produit"], row["QuantitÃ©"], row["ID Magasin"])
            )
        elif table_name == 'products':
            cur.execute(
                "INSERT INTO products (name, product_reference, price, stock) "
                "VALUES (%s, %s, %s, %s)",
                (row["Nom"], row["ID RÃ©fÃ©rence produit"], row["Prix"], row["Stock"])
            )
        elif table_name == 'shops':
            cur.execute(
                "INSERT INTO shops (shop_id, town, employees_number) "
                "VALUES (%s, %s, %s)",
                (row["ID Magasin"], row["Ville"], row["Nombre de salariÃ©s"])
            )

    conn.commit()
    cur.close()
    conn.close()

@app.route('/products', methods=['GET'])
def get_products():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM products")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/sells', methods=['GET'])
def get_sells():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sells")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/sellsbyproduct', methods=['GET'])
def get_sells_by_product():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_reference, SUM(quantity) as total_quantity 
        FROM sells 
        GROUP BY product_reference
        ORDER BY total_quantity DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/sellsvaluebyproduct', methods=['GET'])
def get_sells_value_by_product():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.product_reference, SUM(s.quantity * p.price) as total_value
        FROM sells s
        JOIN products p ON s.product_reference = p.product_reference
        GROUP BY s.product_reference
        ORDER BY total_value DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/sellsbyshop', methods=['GET'])
def get_sells_by_shop():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sh.town, s.shop_id, SUM(s.quantity) as total_quantity 
        FROM sells s
        JOIN shops sh ON s.shop_id = sh.shop_id
        GROUP BY s.shop_id, sh.town
        ORDER BY total_quantity DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


@app.route('/sellsvaluebyshop', methods=['GET'])
def get_sells_value_by_shop():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sh.town, s.shop_id, SUM(s.quantity * p.price) as total_value 
        FROM sells s
        JOIN products p ON s.product_reference = p.product_reference
        JOIN shops sh ON s.shop_id = sh.shop_id
        GROUP BY s.shop_id, sh.town
        ORDER BY total_value DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/sellsbyshopbyproduct', methods=['GET'])
def get_sells_by_shop_by_product():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sh.town, s.shop_id, p.product_reference, SUM(s.quantity) as total_quantity 
        FROM sells s
        JOIN products p ON s.product_reference = p.product_reference
        JOIN shops sh ON s.shop_id = sh.shop_id
        GROUP BY s.shop_id, sh.town, p.product_reference
        ORDER BY total_quantity DESC
    """)
    rows = cur.fetchall()   
    cur.close()
    conn.close()
    return jsonify(rows)    


@app.route('/sellsvaluebyshopbyproduct', methods=['GET'])
def get_sells_value_by_shop_by_product():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sh.town, s.shop_id, p.product_reference, SUM(s.quantity * p.price) as total_value 
        FROM sells s
        JOIN products p ON s.product_reference = p.product_reference
        JOIN shops sh ON s.shop_id = sh.shop_id
        GROUP BY s.shop_id, sh.town, p.product_reference
        ORDER BY total_value DESC
    """)    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    try:
        # Validate request data
        if not request.json:
            return jsonify({"error": "No JSON data provided"}), 400
        
        data = request.json.get('data')
        table_name = request.json.get('table_name')
        print(data)
        print(table_name)
        
        if not data or not table_name:
            return jsonify({"error": "Missing required fields: data or table_name"}), 400

        # Validate table name
        valid_tables = {
            'sells': ["Date", "ID Référence produit", "Quantité", "ID Magasin"],
            'products': ["ID Référence produit", "Prix"],
            'shops': ["ID Magasin", "Ville"]
        }
        
        if table_name not in valid_tables:
            return jsonify({"error": f"Invalid table name: {table_name}"}), 400

        try:
            df = pd.DataFrame(data)
            print(df)
            print(df.columns)
        except Exception as e:
            return jsonify({"error": f"Error creating DataFrame: {str(e)}"}), 400


        # Validate required columns
        missing_cols = set(valid_tables[table_name]) - set(df.columns)
        if missing_cols:
            return jsonify({"error": f"Missing required columns: {list(missing_cols)}"}), 400


        try:
            conn = get_connection()
            cur = conn.cursor()

            # Insert or update data based on table
            for _, row in df.iterrows():
                print(row)
                try:
                    if table_name == 'sells':
                        # Check if record exists
                        cur.execute(
                            "SELECT id FROM sells WHERE date=%s AND product_reference=%s AND shop_id=%s",
                            (row["Date"], row["ID Référence produit"], row["ID Magasin"])
                        )
                        print("row already exists")
                        if not cur.fetchone():
                            cur.execute(
                                "INSERT INTO sells (date, product_reference, quantity, shop_id) "
                                "VALUES (%s, %s, %s, %s)",
                                (row["Date"], row["ID Référence produit"], row["Quantité"], row["ID Magasin"])
                            )
                    
                    elif table_name == 'products':
                        cur.execute(
                            "INSERT INTO products (product_reference, price) VALUES (%s, %s) "
                            "ON CONFLICT (product_reference) DO UPDATE SET price = EXCLUDED.price",
                            (row["ID Référence produit"], row["Prix"])
                        )
                    
                    elif table_name == 'shops':
                        cur.execute(
                            "INSERT INTO shops (shop_id, town) VALUES (%s, %s) "
                            "ON CONFLICT (shop_id) DO UPDATE SET town = EXCLUDED.town",
                            (row["ID Magasin"], row["Ville"])
                        )

                except Exception as e:
                    conn.rollback()
                    return jsonify({"error": f"Error inserting/updating row: {str(e)}"}), 400

            conn.commit()
            return jsonify({"message": "CSV uploaded successfully!"}), 200

        except Exception as e:
            if conn:
                conn.rollback()
            return jsonify({"error": f"Database error: {str(e)}"}), 500

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
