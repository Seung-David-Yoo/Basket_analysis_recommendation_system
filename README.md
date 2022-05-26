# Basket_analysis_recommendation_system

Technology has made our lives easier. There are many things in our day-to-day lives
that enhance our overall experience, be it traveling, shopping, or online web surfing.
One such experience is online shopping when we are recommended products similar to
our cart or the products that we are looking for on the e-commerce website. In this
project, we are trying to build a recommender system using Pyspark that uses the past
data from an e-commerce grocery store ‘Instacart’ and identifies the frequent pattern of
the products sold together and then recommends the products to the new customers.
The proposed system uses the association rules such as Support, Confidence, and Lift
to finalize the products to be recommended. Since we had the dataset from one of the
largest online stores, we chose the pair of items with a frequency greater than 100. We
also chose the minimum Lift value to be greater than 1 for a good recommendation
system. The best recommendation we got was with the confidence of 0.20 and
onwards, in addition to the chosen Lift and Support values. The work done in the project
is not just limited to Instacart, and can also be expanded to the other online stores.
Keyword : Map/Reduce function, Pyspark, Apriori algorithms, Recommendation
system, Confidence, Lift

Setup requirements:
Make sure you have Python and PySpark installed on your machine before running these scripts.


The Project is divided into two file:
1. etl.py
2. recommendation.py

-------------------------------------------------------------------------------------
1. etl.py : This file needs two input .csv files: 'products.csv' & 'order_products_prior.csv' and they need to be in the same folder as etl.py, 
which can be found in the shareable link https://drive.google.com/drive/folders/1bxeq-WOjjeeWGZ91KCTmsDyfpCYrBD3M?usp=sharing

	To run etl.py on a local computer, run the following command:
---------------------------------
	spark-submit etl.py
---------------------------------

If you encounter memory errors while running the script due to not enough memory, 
please add in an option driver-memory <number of memory GB> like below:

spark-submit --driver-memory 12g etl.py


Input: 'products.csv' & 'order_products_prior.csv'. 

Output: etl.py will generate a folder called 'output' which contains a csv file called "part-00000-*****".
Please change this filename to 'result_full.csv' and move it to the directory that contains recommendation.py.
Or you can find the 'result_full.csv' file that we have generated in the shareable link https://drive.google.com/drive/folders/1bxeq-WOjjeeWGZ91KCTmsDyfpCYrBD3M usp=sharing.

------------------------------------------------------------------------
2. recommendation.py : This file needs one input .csv file: 'result_full.csv' and it has to be in the same folder at recommendation.py, which can be found in the shareable link https://drive.google.com/drive/folders/1bxeq-WOjjeeWGZ91KCTmsDyfpCYrBD3M?usp=sharing

	To run recommendation.py on a local computer, run the following command:
----------------------------------------
	spark-submit recommendation.py
----------------------------------------

Input: 'result_full.csv'.
Output: a dataframe displaying on stdout (terminal).
	
	After sucessfully running the .py file, it will prompt for input. Please enter input such as Banana, and it will provide the recommended item as the output. The input value should be from one of the items in the basket.
