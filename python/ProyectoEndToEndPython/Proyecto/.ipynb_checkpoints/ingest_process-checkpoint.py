from process.Extract import Extract
from process.Transform import Transform
from process.Load import load
#import pandas as pd


extract = Extract()
transform = Transform()
load = load()

customers_df0 = extract.read_mysql('customers','retail_db')
categories_df0 = extract.read_mysql("categories", 'retail_db')
departments_df0 = extract.read_mysql("departments", 'retail_db')
order_items_df0 = extract.read_mysql("order_items", 'retail_db')
orders_df0 = extract.read_mysql("orders", 'retail_db')
products_df0 = extract.read_mysql("products", 'retail_db')
print("Tablas extraídas de retail db")

load.load_to_cloud_storage(customers_df0,"karladep12","landing/customers")
load.load_to_cloud_storage(categories_df0,"karladep12","landing/categories")
load.load_to_cloud_storage(departments_df0,"karladep12","landing/departments")
load.load_to_cloud_storage(order_items_df0,"karladep12","landing/order_items")
load.load_to_cloud_storage(orders_df0,"karladep12","landing/orders")
load.load_to_cloud_storage(products_df0,"karladep12","landing/products")
print("Tablas cargadas a landing")

customers_df = extract.read_cloud_storage("karladep12","landing/customers")
categories_df = extract.read_cloud_storage("karladep12","landing/categories")
departments_df = extract.read_cloud_storage("karladep12","landing/departments")
order_items_df = extract.read_cloud_storage("karladep12","landing/order_items")
orders_df = extract.read_cloud_storage("karladep12","landing/orders")
products_df = extract.read_cloud_storage("karladep12","landing/products")
print("Tablas extraídas de landing")

#Consultas realizadas 
result1 = transform.consulta_5tablas(customers_df, orders_df, order_items_df, products_df, categories_df)
print("Transformación 1")

result2 = transform.consulta_3tablas(customers_df, orders_df, order_items_df)
print("Transformación 2")

#Capa Gold
load.load_to_cloud_storage(result1,"karladep12","gold/result1")
load.load_to_cloud_storage(result2,"karladep12","gold/result2")

print("Consultas cargadas a gold")


consulta1 = extract.read_cloud_storage("karladep12","gold/result1")
consulta2 = extract.read_cloud_storage("karladep12","gold/result2")
print("Consultas extraídas de gold")

load.load_to_mysql(consulta1,"consulta1","BDFinal")
load.load_to_mysql(consulta2,"consulta2","BDFinal")
print("Consultas cargadas a MySQL")