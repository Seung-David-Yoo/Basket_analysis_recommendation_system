from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, upper
from py4j.java_gateway import JavaGateway


MIN_CONFIDENCE = .2
CONFIDENCE_DECIMAL_PLACE = 4
LIFT_DECIMAL_PLACE = 2
TOP_N = 5


def main():
    # create SparkSession
    spark = SparkSession.builder.appName('Recommendation System').getOrCreate()
    # load association rule result dataset
    association_rule = spark.read.csv('result_full.csv', header=True)
    # change column name
    recommendation = association_rule.withColumnRenamed('item1', 'item in the basket')\
        .withColumnRenamed('item2', 'recommended item')
    # filter out association rule values with minimum confidence
    recommendation_system = recommendation.select(['item in the basket', 'recommended item', 'confidence', 'lift'])\
        .where(association_rule.confidence >= MIN_CONFIDENCE)
    # convert item in the basket column to uppercase and round confidence and lift values
    recommendation_system = recommendation_system.select(
        upper(col("item in the basket")).alias("item in the basket"), "recommended item",
        round("confidence", CONFIDENCE_DECIMAL_PLACE).alias("confidence"),
        round("lift", LIFT_DECIMAL_PLACE).alias("lift"))
    # cache the dataframe for efficiency
    recommendation_system.cache()

    # create SparkContext
    sc = SparkContext.getOrCreate()
    # create a scanner
    scanner = sc._gateway.jvm.java.util.Scanner
    # create system input to get input from user
    sys_in = getattr(sc._gateway.jvm.java.lang.System, 'in')

    # interactively prompt user and generate top n recommended items based on user input
    while True:
        print("Item in basket:")
        input_item = scanner(sys_in).nextLine().strip().upper()
        recommendation_system.where(col("item in the basket").like('% ' + input_item + ' %'))\
            .select('recommended item').distinct()\
            .orderBy(col("confidence").desc(), col("lift").desc())\
            .show(TOP_N, truncate=False)


if __name__ == '__main__':
    main()