from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, max
from pyspark.sql.window import Window
import findspark

findspark.init()


spark = SparkSession.builder.getOrCreate()


customers_columns = ['id', 'first_name', 'last_name', 'city']

orders_columns = ['cust_id', 'order_details']

customers = [("101", "John", "Doe", "San Diego"),
             ("102", "Juan", "Perez", "Salto"),
             ("103", "Jane", "Doe", "Miami"),
             ("104", "Jon", "Doe", "San Diego"),
             ("105", "John", "Do", "San Diego"),
             ]

orders = [("101", "clothes"),
          ("103", "food"),
          ("101", "food"),
          ("104", "clothes"),
          ("104", "clothes"),
          ("104", "clothes"),
          ("104", "clothes")]

customer_df = spark.createDataFrame(data=customers, schema=customers_columns)
orders_df = spark.createDataFrame(data=orders, schema=orders_columns)

# customer_df.show()
# orders_df.show()

condition_for_join = [(customer_df['id'] == orders_df['cust_id'])]

df_joined = customer_df.join(orders_df, on=condition_for_join, how='left')

# df_joined.show()

df_joined = df_joined.withColumn('RealOrder',
                                 when(col('cust_id').isNull(), 0).otherwise(1))
# df_joined.show()

agg_join = df_joined.groupBy('id').sum('RealOrder')
# agg_join.show()

top_order = agg_join.orderBy(col('sum(RealOrder)').desc())
# top_order.limit(1).show()

partitioned_orders = Window.partitionBy('id').orderBy('id')

ranked_customer = df_joined.withColumn('top_client', sum('RealOrder').over(partitioned_orders))

ranked_customer.show()

(ranked_customer.groupBy(col('id')).agg(max('top_client').alias('topClient'))
 .orderBy(col('topClient').desc()).limit(3).show())
