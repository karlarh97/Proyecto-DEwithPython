from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals()) 

class Transform():
    
    def __init__(self) -> None:
        self.process = 'Transform Process'


    def consulta_5tablas(self, customers_df, orders_df, order_items_df, products_df, categories_df):
        q = """
            SELECT
                c.customer_city,
                cat.category_name,
                COUNT(DISTINCT o.order_id) AS total_orders,
                SUM(oi.order_item_subtotal) AS total_sales
            FROM
                customers_df AS c
                INNER JOIN orders_df AS o ON c.customer_id = o.order_customer_id
                INNER JOIN order_items_df AS oi ON o.order_id = oi.order_item_order_id
                INNER JOIN products_df AS p ON oi.order_item_product_id = p.product_id
                INNER JOIN categories_df AS cat ON p.product_category_id = cat.category_id
            WHERE
                o.order_status = 'COMPLETE'
            GROUP BY
                c.customer_city,
                cat.category_name
            ORDER BY
                total_sales DESC
            LIMIT 10
            """
        result = sqldf(q)

        return result
        
    def consulta_3tablas(self, customers_df, orders_df, order_items_df):
        q = """
            SELECT
                c.customer_state,
                COUNT(DISTINCT o.order_id) AS total_orders,
                SUM(oi.order_item_quantity) AS total_items_sold
            FROM
                customers_df AS c
                INNER JOIN orders_df AS o ON c.customer_id = o.order_customer_id
                INNER JOIN order_items_df AS oi ON o.order_id = oi.order_item_order_id
            WHERE
                o.order_status = 'COMPLETE'
            GROUP BY
                c.customer_state
            ORDER BY
                total_orders DESC
            """
        result = sqldf(q)

        return result
  



