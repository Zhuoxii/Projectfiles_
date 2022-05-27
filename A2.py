# -*- coding: utf-8 -*-


from pyspark.sql.functions import explode, col
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import locate
from pyspark.sql.functions import size
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import count
import argparse

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Comp5349 Assignment2_zkua9391") \
    .getOrCreate()



spark.sparkContext.setLogLevel("ERROR")
parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='a2_out')
args = parser.parse_args()
output_path = args.output





########################## Load the data and convert into DataFrame #######################3

data = "s3://comp5349-2022/test.json"
df= spark.read.option('multiline', 'true').json(data)

df_title = df.select(explode("data.title").alias("title")).withColumn("index",f.monotonically_increasing_id())
df_para = df.select(explode("data.paragraphs").alias("data")).withColumn("index",f.monotonically_increasing_id())


df = df_title.join(df_para,df_title.index==df_para.index).drop("index")\
         .select('title','data',explode("data.context").alias("context"))\
         .select('title','data', 'context', explode("data.qas").alias("qas"))\
         .select('title', 'context', 'qas', explode("qas").alias("qas2"))\
         .select('title', 'context', 'qas2', "qas2.question", "qas2.answers","qas2.is_impossible").cache()

df.show()
print("Initial DF generating!!!!!!!!!")
print("Maybe taking 3 minutes to run all code o(╥﹏╥)o")


### function for generating sequnece for a contract
def segmentToSequence(data):
  ls = []
  ix = []
  n = 0
  for i in range(len(data)):
    if i >= 4096 and i % 2048 == 0:
      res = list(data[n:i])
      res = "".join(res)
      ls.append(res)
      ix.append([n,i])
      n += 2048
  return ls


udf1=udf(segmentToSequence, ArrayType(StringType()))

####################### generate impossible negative samples   ####################
  
df_no_answer = df.filter(df.is_impossible == True)  #.select('context','question','answers' ,"is_impossible")
df_impossible_negative = df_no_answer.withColumn('list_context',udf1(df_no_answer.context))\
                       .withColumn("source",f.explode('list_context'))\
                       .withColumn('type', lit('impossible negative'))\
                       .withColumn('answer_start', lit(0))\
                       .withColumn('answer_end', lit(0))\
                       .select('title','context', 'source', 'question', 'answer_start', 'answer_end', 'type').cache()





####################### generate possible samples   ####################

df_answer = df.filter(df.is_impossible == False )\
              .withColumn('answers2', explode('answers').alias('answers2'))\
              .select('title','context','question','answers2.text', 'answers2.answer_start')
df_answer = df_answer.withColumn('list_context',udf1('context'))\
           .withColumn("source",f.explode('list_context')).select('title','context', 'source', 'question','text', 'answer_start').cache()





####################### helper function   ####################!

def is_positive(record):
  context = record[1]
  source = record[2]
  text = record[4]
  answer_start = record[5]

  source_start = context.index(source)
  source_end = source_start + len(source) 

  answer_end = answer_start + len(text)
  if answer_start <= source_start and answer_end >= source_start:
    return  record + ["positive"]
  elif answer_start >= source_start and answer_start <= source_end:
    return  record + ["positive"]
  else:
    return  record + ["possible negative"]


def positive_answer_index(record):
    context = record[1]
    source = record[2]
    text = record[4]
    answer_start = record[5]
    source_start = context.index(source)
    source_end = source_start + len(source)
    answer_end = answer_start + len(text)

    if answer_start < source_start and answer_end < source_end:
      return    [record[0], record[2], record[3], 0, len(text), record[6]]
    elif answer_start < source_start and answer_end > source_end:
      return  record + [0,len(source)]
    elif answer_start > source_start and answer_end < source_end:
      return  [record[0],  record[2], record[3], source.index(text), source.index(text) + len(text),record[6]]
    else:
      new_text = context[answer_start: source_end]
      return  [record[0], record[2], record[3], source.index(new_text), len(source),record[6]]


def negative_answer_index(record):
    record = [record[0], record[2], record[3], 0, 0, record[6]]
    return record




####################### generate positive and possible negative samples   ####################

rdd_answer = df_answer.rdd.map(list)
rdd_type = rdd_answer.map(is_positive).cache()
rdd_positive = rdd_type.filter(lambda x: x[6] == 'positive').map(positive_answer_index).cache()
rdd_possible_negative = rdd_type.filter(lambda x: x[6] == 'possible negative').map(negative_answer_index).cache()


schema1 =  ['title', 'source', 'question', 'answer_start','answer_end','type']
df_positive = rdd_positive.toDF(schema1).cache()
schema2 = ['title', 'source', 'question', 'answer_start','answer_end','type']
df_possible_negative = rdd_possible_negative.toDF(schema2).cache()



""####################### Balance negative and positive samples"  ######################


df_1 = df_positive.groupBy('question').count().withColumnRenamed('count','question_count')
df_3 = df_positive.groupBy('question').agg(f.countDistinct('title')).withColumnRenamed('count(title)','other_contract_count')
df_4 = df_1.join(df_3, 'question','inner')
df_1 = df_4.withColumn('extract_length',f.round(f.col('question_count')/f.col('other_contract_count'),0).astype('int'))
# df_1.show()

## join postive question impossible negative question 
df_2 =  df_1.join(df_impossible_negative, 'question', 'inner').orderBy('title','question','source')\
          .select('title', 'question','source', 'extract_length','type')

# df_2.show()

df_3 = df_2.groupBy('title','question','extract_length').agg(f.collect_set('source').alias('source_list')).orderBy('title','question')
df_3 = df_3.withColumn('seq_len', f.size('source_list'))

df_4 = df_3.withColumn('extract_length2',f.when(df_3.extract_length <=  df_3.seq_len ,df_3.extract_length).otherwise(df_3.seq_len))\
          .drop('extract_length')

import random
def extract(source_list, n):
  ls = list(source_list)
  # ls2 = random.shuffle(ls)
  res = ls[:n]
  return res

udf2 = udf(extract, ArrayType(StringType()))
impossible_negative = df_4.withColumn('extract_source', udf2(f.col("source_list"), f.col('extract_length2')))

impossible_negative =  impossible_negative.withColumn('source', explode(f.col('extract_source')))\
                                  .withColumn('answer_start', lit(0))\
                                  .withColumn('answer_end', lit(0))\
                                  .select('source', 'question', 'answer_start', 'answer_end').cache()
# impossible_negative.show()
# print("Impossible_negative samples generating!!!!!!!!!!!!!!! ")

"""## Balance possible negative and postive"""

df1 = df_positive.groupBy('title', 'question').count().withColumnRenamed('count','extract_length')

df2 = df_possible_negative.join(df1, ['title','question'], 'inner')
df3 = df2.groupBy('title','question','extract_length').agg(f.collect_set('source').alias('source_list')).orderBy('title','question')\
          .withColumn('seq_len', f.size('source_list'))


df4 = df3.withColumn('extract_length2',f.when(df3.extract_length <=  df3.seq_len ,df3.extract_length).otherwise(df3.seq_len))\
          .drop('extract_length')

possible_negative = df4.withColumn('extract_source', udf2(f.col("source_list"), f.col('extract_length2')))

# possible_negative = df4.withColumn('extract_source', f.slice("source_list",start= lit(1), length=f.col('extract_length2')))
possible_negative =  possible_negative.withColumn('source', explode(f.col('extract_source')))\
                                  .withColumn('answer_start', lit(0))\
                                  .withColumn('answer_end', lit(0))\
                                  .select('source', 'question', 'answer_start', 'answer_end').cache()

possible_negative.show()
print("Possible_negative samples generating!!!!!!!!!!!!!!! ")





""####################### Union three DataFrame  ######################

positive = df_positive.select('source', 'question', 'answer_start','answer_end')
df_all = positive.union(impossible_negative).union(possible_negative).cache()

import json
result = df_all.toJSON().collect()
output = json.dumps(result, indent = 2)
with open('result.json','w') as f:
  json.dump(output, f)
print("Finished all!!!!!!!!The final json file is in the same directory with .py and .sh script")

spark.stop()

