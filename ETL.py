from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


# minimum frequency of product appear in the basket
MIN_FREQ = 100


def main():
    # create SparkContext and SparkSession
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    # load the product csv file and create temp table
    products = spark.read.csv('products.csv', sep=',', inferSchema=True, header=True)
    products.createOrReplaceTempView('products')

    # load the order_products_prior csv file and create temp table
    order_products_prior = spark.read.csv('order_products_prior.csv', sep=',', inferSchema=True, header=True)
    order_products_prior.createOrReplaceTempView('order_products_prior')

    # join products and order_products_prior on product_id to get product id for each order (basket)
    order_products = spark.sql("""
        select collect_list(product_id) as basket
        from order_products_prior
        group by order_id
        order by order_id
    """)

    # convert df to rdd
    rdd = order_products.rdd
    # get a list of product ids for each order (basket)
    rdd = rdd.map(lambda row: row['basket'])
    # filter out basket with 1 item only
    rdd = rdd.filter(lambda x: len(x) > 1)
    # sort the basket
    sorted_rdd = rdd.map(lambda x: sorted(x))
    # combine every two items in the basket to key-value pair
    paired_key = sorted_rdd.map(lambda x: [((x[i], x[j]), 1)
                                for i in range(len(x)) for j in range(len(x)) if (i != j and i < j)])
    # combine the key together and get the sum for each key
    reduced_key = paired_key.flatMap(lambda x: x).reduceByKey(lambda x, y: x + y)
    # keep only a pair of items with frequency > 100
    reduced_key = reduced_key.filter(lambda x: x[1] > MIN_FREQ)
    # sort the key
    reduced_key = reduced_key.sortByKey()

    # convert into a tuple of 3 elements
    # first element is item 1, second element is item 2, third element is frequency of both items appear in the baskets
    rdd = reduced_key.map(lambda x: (x[0][0], x[0][1], x[1]))
    # convert into dataframe
    columns = ["item1", "item2", "frequency"]
    itemset_frequency = rdd.toDF(columns)
    itemset_frequency.createOrReplaceTempView('itemset_frequency')

    # count number of baskets containing each product
    product_in_basket_count = spark.sql("""
        select product_id, count(*) as in_basket_count
        from (select order_id, product_id from order_products_prior group by order_id, product_id)
        group by product_id
    """)
    product_in_basket_count.createOrReplaceTempView('product_in_basket_count')

    # count number of baskets
    basket_count = spark.sql("""select order_id from order_products_prior group by order_id""").count()

    # calculate support for each product
    support_product_df = spark.sql("""
        select p1.product_id, p1.product_name, p2.in_basket_count/{} as support
        from products p1
        left join product_in_basket_count p2 on p1.product_id = p2.product_id
        order by p1.product_id
    """.format(basket_count))
    # drop any rows that have null values
    support_product_df = support_product_df.dropna("any")
    support_product_df.createOrReplaceTempView('support_product')

    # calculate support for each itemset
    support_itemset_df = spark.sql("""
        select item1, item2, frequency/{} as support from itemset_frequency
    """.format(basket_count))
    support_itemset_df.createOrReplaceTempView('support_itemset')

    # calculate confidence for each itemset
    # union because we need confidence for each item in the itemset
    confidence_df = spark.sql("""
        select si.item1, si.item2, si.support, si.support/sp.support as confidence
        from support_itemset si
        join support_product sp on si.item1 = sp.product_id
        union
        select si.item2, si.item1, si.support, si.support/sp.support as confidence
        from support_itemset si
        join support_product sp on si.item2 = sp.product_id
    """)
    confidence_df.createOrReplaceTempView('confidence')

    # calculate lift for each itemset
    lift_df = spark.sql("""
        select c.*, confidence/sp.support as lift
        from confidence c
        join support_product sp on c.item2 = sp.product_id
    """)
    lift_df.createOrReplaceTempView('lift')

    # get the product name instead of product id for displaying purpose
    result = spark.sql("""
        select s.item1, p2.product_name as item2, s.support, s.confidence, s.lift
        from (select p.product_name as item1, l.item2, l.support, l.confidence, l.lift
                from lift l
                join products p on l.item1 = p.product_id) s
        join products p2 on s.item2 = p2.product_id
    """)

    # save result as csv
    result.coalesce(1).write.csv('output', mode='overwrite', header=True)

    print(result.show(truncate=False))


if __name__ == '__main__':
    main()