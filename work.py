# -*- coding: utf-8 -*-


# from pyspark.sql.functions import explode, col
# from pyspark.sql.functions import udf
# from pyspark.sql.functions import lit
# from pyspark.sql.functions import locate
# from pyspark.sql.functions import size
# from pyspark.sql import functions as f
# from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
# from pyspark.sql.window import Window
# from pyspark.sql.functions import count
# import argparse

# from pyspark.sql import SparkSession


# if __name__ == "__main__":
#     spark = SparkSession \
#         .builder \
#         .appName("Comp5349 Assignment2") \
#         .getOrCreate() 



#     # Configuration
#     spark.conf.set("spark.sql.shuffle.partitions", 5)


#     # Read the dataset

#     # s3://comp5349-2022/test.json
#     # s3://comp5349-2022/train_separate_questions.json
#     # s3://comp5349-2022/CUADv1.json
#     data = "s3://comp5349-2022/train_separate_questions.json"
#     df= spark.read.option('multiline', 'true').json(data)

# #     spark.sparkContext.setLogLevel("ERROR")

# #     parser = argparse.ArgumentParser()
# #     parser.add_argument("--output", help="the output path",
# #                             default='week8_out')
# #     args = parser.parse_args()
# #     output_path = args.output

#     # Phase 1: Convert json format to DataFrame.

#     df =  df.select(explode("data.paragraphs").alias("data"))\
#              .select('data',explode("data.context").alias("context"))\
#              .select('data', 'context', explode("data.qas").alias("qas"))\
#              .select('context', 'qas', explode("qas").alias("qas2"))\
#              .select('context', 'qas2', "qas2.question", "qas2.answers","qas2.is_impossible")\
#              .cache()  

#     df.show()
#     print("Phase 1: Convert json format to DataFrame")




#     # Phase 1: Divide the whole DF into three kinds of Sub-DF and add necessary fields.
#     ## Some useful udf
#     def segmentToSequence(data):
#       ls = []
#       ix = []
#       n = 0
#       for i in range(len(data)):
#         if i >= 4096 and i % 2048 == 0:
#           res = list(data[n:i])
#           res = "".join(res)
#           ls.append(res)
#           ix.append([n,i])
#           n += 2048
#       return ls


#     def is_positive(context, sequence, text, answer_start):
#         sequence_start = context.index(sequence)
#         sequence_end = sequence_start + len(sequence)
#         answer_end = answer_start + len(text)
#         if answer_start <= sequence_start and answer_end >= sequence_start:
#           return 'positive'
#         elif answer_start >= sequence_start and answer_start <= sequence_end:
#           return 'positive'
#         else:
#           return 'possible negative'


#     def answer_index(context, sequence, text, answer_start):
#         sequence_start = context.index(sequence)
#         sequence_end = sequence_start + len(sequence)

#         answer_end = answer_start + len(text)
#         if answer_start < sequence_start and answer_end < sequence_end:
#           return  [0,len(text)]
#         elif answer_start < sequence_start and answer_end > sequence_end:
#           return [0,len(sequence)]
#         elif answer_start > sequence_start and answer_end < sequence_end:
#           return [sequence.index(text), sequence.index(text)+len(text)]
#         else:
#           new_text = context[answer_start: sequence_end]
#           return [sequence.index(new_text), len(sequence)]



#     udf1=udf(segmentToSequence, ArrayType(StringType()))  
#     udf2=udf(is_positive, StringType()) 
#     udf3=udf(answer_index, ArrayType(IntegerType()))



#     ## Phase 2.1: DataFrame for impossible_negative samples
#     df_no_answer = df.filter(df.is_impossible == True).select('context','question','answers' ,"is_impossible")
#     df_impossible_negative = df_no_answer.withColumn('list_context',udf1('context'))\
#                           .withColumn("source",f.explode('list_context'))\
#                           .withColumn('type', lit('impossible negative'))\
#                           .withColumn('answer_start', lit(0))\
#                           .withColumn('answer_end', lit(0))\
#                           .select('context', 'source', 'question', 'answer_start', 'answer_end', 'type')\
#                           .orderBy('context','question','source')

#     df_impossible_negative.show()
#     print("Phase 2.1 Generate DataFrame for impossible_negative samples")




#     ## Phase 2.2: DataFrame for positive and possible_negative samples
#     df_answer = df.withColumn('answers2', explode('answers').alias('answers2'))\
#                 .select('context','question','answers2.text', 'answers2.answer_start',"is_impossible")\
#                 .withColumn('list_context',udf1('context'))\
#                 .withColumn("source",f.explode('list_context')).select( 'context', 'source', 'question','text', 'answer_start') \
#                 .withColumn('type',udf2('context', 'source', 'text', 'answer_start'))\
#                 .cache()

#     df_answer.show()
#     df_answer.write.csv(result.csv)
#     spark.stop()

    

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("COMP5349 Assignment2 Dataset")\
    .config("spark.executor.memory", "4g")\
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()

# s3://comp5349-2022/test.json
#     # s3://comp5349-2022/train_separate_questions.json
#     # s3://comp5349-2022/CUADv1.json

train_dataset = 'train_separate_questions.json'
training_df = spark.read.json('s3://comp5349-2022/train_separate_questions.json')
test_dataset = 'test.json'
testing_df = spark.read.json('s3://comp5349-2022/test.json')
# all_dataset = 'CUADv1.json'
# all_data_df = spark.read.json(train_dataset)

testing_df.show(1)

testing_df.printSchema()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window, Row
from pyspark.sql.types import IntegerType, StringType, FloatType

testing_data_df= testing_df.select((explode("data").alias('data')))
testing_paragraph_df = testing_data_df.select(explode("data.paragraphs").alias("paragraph"))

testing_paragraph_df.show(5)

paragraphs_context_qas_df = testing_paragraph_df.select(
    col("paragraph.context").alias("paragraph_context"),
    explode("paragraph.qas").alias('qas'),
    )

paragraphs_context_qas_df.show(5)

qas_answers_df = paragraphs_context_qas_df.select(
    col("paragraph_context"),
    col("qas.question").alias("qas_question"),
    col("qas.is_impossible").alias("qas_is_impossible"),
    explode_outer("qas.answers").alias('answers'),
)

qas_answers_df.show(5)

final_table_df = qas_answers_df.select(
    col("paragraph_context"),
    col("qas_question"),
    col("qas_is_impossible"),
    col("answers.answer_start").alias("answer_start"),
    col("answers.text").alias("answer_text"),
)

final_table_df.show(5)

possible_sample_df = final_table_df.where(
    col("qas_is_impossible") == False
)
possible_sample_df.show(5)

possible_sample_rdd = possible_sample_df.rdd
possible_sample_rdd.take(3)

def row_to_tuple(row):
  return tuple(row)
possible_sample_rdd_tuple = possible_sample_rdd.map(row_to_tuple)
possible_sample_rdd_tuple.take(3)

import random
def create_possible_sample(record):
  output_list = []
  poss_record = []
  neg_record = []
  subStrings = []
  window = 4096
  stride = 2048
  answer_start = record[3]
  answer_end = record[3] + len(record[4])
  length_record = len(record[0])
  for i in range(0, length_record, stride):
    if i+window <= length_record:
      temp = record[0][i:i+window]
    else:
      temp = record[0][i:]
    subStrings.append(temp)
  for j in range(len(subStrings)):
    if (j * stride <= answer_start) and (answer_start < j * stride + window) and (j * stride + window < answer_end):
      poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=answer_start - j * stride, answer_end=window))
    elif(j * stride <= answer_start) and (answer_start < j * stride + window) and (answer_end < j * stride + window):
      poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=answer_start - j * stride, answer_end=answer_end-j * stride))
    elif(answer_start <= j * stride) and (j * stride + window < answer_end):
      poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=window))
    elif(j * stride <= answer_end) and (answer_end < j * stride + window) and (answer_start < j * stride):
      poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=answer_end-j * stride))
    else:
      neg_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=0))
  if (len(poss_record)<len(neg_record)):
    random.shuffle(neg_record)
    neg_record = neg_record[:len(poss_record)]
  poss_record.extend(neg_record)
  return poss_record

poss_rdd = possible_sample_rdd_tuple.flatMap(create_possible_sample).cache()
poss_rdd.take(3)

possible_contract = possible_sample_df.groupBy("paragraph_context").count().withColumnRenamed("count","positive_contract_num")
possible_contract.show(5)

poss_dfColumns = ["data_source","qas_question","answer_start","answer_end"]
poss_df = poss_rdd.toDF(poss_dfColumns).cache()
poss_df.show(3)

positive_simple_df = poss_df.where(col("answer_start") != '0')
positive_simple_num_df = positive_simple_df.where(col("answer_end") != '0').groupBy("qas_question").count().withColumnRenamed("count","positive_simple_num")
positive_simple_num_df.show(5)

count_table = final_table_df.where(
  col("qas_is_impossible") == True
).join(positive_simple_num_df,"qas_question")
count_table = count_table.join(possible_contract,"paragraph_context").cache()
count_table.show(5)

count_table_rdd = count_table.rdd
count_table_rdd = count_table_rdd.map(row_to_tuple)
count_table_rdd.take(5)

def count_negtive_sample(record):
  output = []
  if record[5] % record[6] == 0:
    n = record[5] / record[6]
  elif (record[5] % record[6] != 0) and (float(record[5] / record[6])-int(record[5] / record[6]) >= 0.5):
    n = record[5]//record[6]+1
  elif (record[5] % record[6] != 0) and (record[5] / record[6]-record[5] // record[6] <= 0.5):
    n = record[5]//record[6]
  output.append((record[0],record[1],record[2],record[3],record[4],n))
  return output
count_netiv_num_rdd = count_table_rdd.flatMap(count_negtive_sample)
count_netiv_num_rdd.take(5)

def create_negative_sample(record):
  neg_record = []
  subStrings = []
  window = 4096
  stride = 2048
  num = int(record[5])
  length_record = len(record[0])
  for i in range(0, length_record, stride):
    if i+window <= length_record:
      temp = record[0][i:i+window]
    else:
      temp = record[0][i:]
    subStrings.append(temp)
  for j in range(len(subStrings)):
      neg_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=0))
  random.shuffle(neg_record)
  neg_record = neg_record[:num]
  return neg_record
imposs_rdd = count_netiv_num_rdd.flatMap(create_negative_sample).cache()
imposs_rdd.take(3)

output_rdd = poss_rdd.union(imposs_rdd)
output_rdd.take(5)

output_rddColumns = ["source","question","answer_start","answer_end"]
output_df = output_rdd.toDF(output_rddColumns).cache()
output_df.show()

output_df.write.json("output.json")
