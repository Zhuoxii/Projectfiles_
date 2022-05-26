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

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .config("spark.driver.memory", "6g") \
    .appName("Comp5349 Assignment2") \
    .getOrCreate()


    # s3://comp5349-2022/test.json
    # s3://comp5349-2022/train_separate_questions.json
    # s3://comp5349-2022/CUADv1.json
data = "s3://comp5349-2022/train_separate_questions.json"
df= spark.read.option('multiline', 'true').json(data)


df =  df.select(explode("data.paragraphs").alias("data"))\
         .select('data',explode("data.context").alias("context"))\
         .select('data', 'context', explode("data.qas").alias("qas"))\
         .select('context', 'qas', explode("qas").alias("qas2"))\
         .select('context', 'qas2', "qas2.question", "qas2.answers","qas2.is_impossible").cache()

df.show()


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

### impossible negative
  
df_no_answer = df.filter(df.is_impossible == True)  #.select('context','question','answers' ,"is_impossible")
df_impossible_negative = df_no_answer.withColumn('list_context',udf1(df_no_answer.context))\
                       .withColumn("source",f.explode('list_context'))\
                       .withColumn('type', lit('impossible negative'))\
                       .withColumn('answer_start', lit(0))\
                       .withColumn('answer_end', lit(0))\
                       .select('context', 'source', 'question', 'answer_start', 'answer_end', 'type')

df_impossible_negative.show()

## 筛选出没有impossible negative的sample
df_no_answer = df.filter(df.is_impossible == True).select('context','question','answers' ,"is_impossible")
df_no_answer.show()

df_answer = df.withColumn('answers2', explode('answers').alias('answers2'))\
          .select('context','question','answers2.text', 'answers2.answer_start',"is_impossible")
df_answer = df_answer.withColumn('list_context',udf1('context'))\
          .withColumn("source",f.explode('list_context')).select( 'context', 'source', 'question','text', 'answer_start') #.cache()
df_answer.show()

def is_positive(record):
  context = record[0]
  source = record[1]
  text = record[3]
  answer_start = record[4]

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
    context = record[0]
    source = record[1]
    text = record[3]
    answer_start = record[4]
    source_start = context.index(source)
    source_end = source_start + len(source)
    answer_end = answer_start + len(text)

    record = [record[0], record[1], record[2], record[3],record[5]]
    if answer_start < source_start and answer_end < source_end:
      return   record + [0,len(text)]
    elif answer_start < source_start and answer_end > source_end:
      return  record + [0,len(source)]
    elif answer_start > source_start and answer_end < source_end:
      return  record + [source.index(text), source.index(text)+len(text)]
    else:
      new_text = context[answer_start: source_end]
      return  record +[source.index(new_text), len(source)]


def negative_answer_index(record):
    record = [record[0], record[1], record[2], 0, 0, record[5]]
    return record

rdd_answer = df_answer.rdd.map(list)
rdd_type = rdd_answer.map(is_positive)
rdd_positive = rdd_type.filter(lambda x: x[5] == 'positive').map(positive_answer_index)
rdd_possible_negative = rdd_type.filter(lambda x: x[5] == 'possible negative') .map(negative_answer_index)

schema1 =  ['context', 'source', 'question','text', 'answer_start','answer_end','type']
df_positive = rdd_positive.toDF(schema1)
#df_positive  = spark.createDataFrame(rdd_positive,['context', 'source', 'question','text', 'answer_start','answer_end','type'])
schema2 = ['context', 'source', 'question', 'answer_start','answer_end','type']
df_possible_negative = rdd_possible_negative.toDF(schema2)

"""# Balance negative and positive samples

##  去掉possible negative和positive重复的部分
"""

# df_positive 去重
df_positive_distinct = df_positive.select('source').distinct()

df2_possible_negative = df_possible_negative.join(df_positive_distinct, ['source'], 'leftanti').select('context','source','question','answer_start','answer_end','type')
# df2_possible_negative.show(10)

"""## 去掉impossible negative和positive重复的部分"""

df2_impossible_negative = df_impossible_negative.join(df_positive_distinct, ['source'], 'leftanti').select('context','source','question','answer_start','answer_end','type')
# df2_impossible_negative.show()

"""## 平衡 impossible negative and positive

For an impossible question in a contract, the number of impossible negative samples to keep equals the average number of positive samples of that question in other contracts that have at least one positive sample for the same question

- 分子: 单个contract单个问题的positive samples的数量  -- groupby contract and question 计算每个contract的每个question的positive samples的数量
- 分母: 单个问题的contract总数 - 1 --groupby question 计算每个question的contract数量再减1
- 思路是: 分子和分母都通过join方式到impossible negative生成新的两列 --
"""

# df_impossible_negative_extract = df_impossible_negative.select('question').distinct()

#对于每个contract每个question有多少sample
##对于positive而言
df_1 = df_positive.groupBy('question').count().withColumnRenamed('count', 'extract_length')

## 把postive的question和impossible negative的question join
df_2 =  df_1.join(df_impossible_negative, 'question', 'inner').orderBy( 'context','question','source')

window1 = Window.partitionBy("context").orderBy('context','question')
df_3 = df_2.groupBy('context','question','extract_length').agg(f.collect_set('source').alias('source_list')).orderBy('context','question')\
          .withColumn('seq_len', f.size('source_list'))\
          .withColumn('lag_extract_length', f.lag(f.col('extract_length')).over(window1))\
          .fillna(0)\
          .withColumn('cusum_lag_extract_length', f.sum(f.col('lag_extract_length')).over(window1))\
          .withColumn('extract_start', f.col('cusum_lag_extract_length')+1)\
          .drop('lag_extract_length', 'cusum_lag_extract_length')\
          .select('context','question','source_list','extract_start','extract_length','seq_len')

def new_extract(extract_start, extract_length, seq_len):
  if extract_start <= seq_len and extract_start + extract_length <= seq_len + 1:
    extract_start2 = extract_start
    extract_length2 = extract_length
  
  elif extract_start <= seq_len and extract_start + extract_length > seq_len + 1:
    extract_start2 = extract_start
    extract_length2 = seq_len - extract_start + 1

  elif extract_start > seq_len and extract_length <= seq_len:
    extract_start2 = 1
    extract_length2 = extract_length
  
  else:
    extract_start2 = 1
    extract_length2 = seq_len
  return [extract_start2, extract_length2]

udf4 = udf(new_extract, ArrayType(IntegerType()))  
df_4 = df_3.withColumn('extract_start',udf4('extract_start', 'extract_length','seq_len')[0])\
           .withColumn('extract_length',udf4('extract_start', 'extract_length','seq_len')[1])

# df_4.show()

impossible_negative = df_4.withColumn('extract_source', f.slice("source_list",start=f.col('extract_start'), length=f.col('extract_length')))\
                                  .withColumn('source', explode('extract_source'))\
                                  .withColumn('answer_start', lit(0))\
                                  .withColumn('answer_end', lit(0))\
                                  .select('source', 'question', 'answer_start', 'answer_end')
impossible_negative.show()

"""## 平衡 possible negative and postive"""

df1 = df_positive.groupBy('context', 'question').count().withColumnRenamed('count','extract_length')

df2 = df_possible_negative.join(df1, ['context','question'], 'inner')
df3 = df2.groupBy('context','question','extract_length').agg(f.collect_set('source').alias('source_list')).orderBy('context','question')\
          .withColumn('seq_len', f.size('source_list'))\
          .withColumn('lag_extract_length', f.lag(f.col('extract_length')).over(window1))\
          .fillna(0)\
          .withColumn('cusum_lag_extract_length', f.sum(f.col('lag_extract_length')).over(window1))\
          .withColumn('extract_start', f.col('cusum_lag_extract_length')+1)\
          .drop('lag_extract_length', 'cusum_lag_extract_length')\
          .select('context','question','source_list','extract_start','extract_length','seq_len')

df4 = df3.withColumn('extract_start',udf4('extract_start', 'extract_length','seq_len')[0])\
           .withColumn('extract_length',udf4('extract_start', 'extract_length','seq_len')[1])

possible_negative = df4.withColumn('extract_source', f.slice("source_list",start=f.col('extract_start'), length=f.col('extract_length')))\
                                  .withColumn('source', explode('extract_source'))\
                                  .withColumn('answer_start', lit(0))\
                                  .withColumn('answer_end', lit(0))\
                                  .select('source', 'question', 'answer_start', 'answer_end')
possible_negative.show()

"""# 结果合并"""

positive = df_positive.select('source', 'question', 'answer_start','answer_end')
df_all = positive.union(impossible_negative).union(possible_negative)
# df_all.show()

# positive.repartition(1).write.mode('overwrite').json("output.jsonl")

import json
result = df_all.toJSON().collect()
output = json.dumps(result, indent = 2)
with open('result.json','w') as f:
  json.dump(output, f)

# testing_df = df
# testing_df.show(1)

# testing_df.printSchema()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql import Window, Row
# from pyspark.sql.types import IntegerType, StringType, FloatType

# testing_data_df= testing_df.select((explode("data").alias('data')))
# testing_paragraph_df = testing_data_df.select(explode("data.paragraphs").alias("paragraph"))

# testing_paragraph_df.show(5)

# paragraphs_context_qas_df = testing_paragraph_df.select(
#     col("paragraph.context").alias("paragraph_context"),
#     explode("paragraph.qas").alias('qas'),
#     )

# paragraphs_context_qas_df.show(5)

# qas_answers_df = paragraphs_context_qas_df.select(
#     col("paragraph_context"),
#     col("qas.question").alias("qas_question"),
#     col("qas.is_impossible").alias("qas_is_impossible"),
#     explode_outer("qas.answers").alias('answers'),
# )

# qas_answers_df.show(5)

# final_table_df = qas_answers_df.select(
#     col("paragraph_context"),
#     col("qas_question"),
#     col("qas_is_impossible"),
#     col("answers.answer_start").alias("answer_start"),
#     col("answers.text").alias("answer_text"),
# )

# final_table_df.show(5)

# possible_sample_df = final_table_df.where(
#     col("qas_is_impossible") == False
# )
# possible_sample_df.show(5)

# possible_sample_rdd = possible_sample_df.rdd
# possible_sample_rdd.take(3)

# def row_to_tuple(row):
#   return tuple(row)
# possible_sample_rdd_tuple = possible_sample_rdd.map(row_to_tuple)
# possible_sample_rdd_tuple.take(3)

# import random
# def create_possible_sample(record):
#   output_list = []
#   poss_record = []
#   neg_record = []
#   subStrings = []
#   window = 4096
#   stride = 2048
#   answer_start = record[3]
#   answer_end = record[3] + len(record[4])
#   length_record = len(record[0])
#   for i in range(0, length_record, stride):
#     if i+window <= length_record:
#       temp = record[0][i:i+window]
#     else:
#       temp = record[0][i:]
#     subStrings.append(temp)
#   for j in range(len(subStrings)):
#     if (j * stride <= answer_start) and (answer_start < j * stride + window) and (j * stride + window < answer_end):
#       poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=answer_start - j * stride, answer_end=window))
#     elif(j * stride <= answer_start) and (answer_start < j * stride + window) and (answer_end < j * stride + window):
#       poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=answer_start - j * stride, answer_end=answer_end-j * stride))
#     elif(answer_start <= j * stride) and (j * stride + window < answer_end):
#       poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=window))
#     elif(j * stride <= answer_end) and (answer_end < j * stride + window) and (answer_start < j * stride):
#       poss_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=answer_end-j * stride))
#     else:
#       neg_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=0))
#   if (len(poss_record)<len(neg_record)):
#     random.shuffle(neg_record)
#     neg_record = neg_record[:len(poss_record)]
#   poss_record.extend(neg_record)
#   return poss_record

# poss_rdd = possible_sample_rdd_tuple.flatMap(create_possible_sample).cache()
# poss_rdd.take(3)

# possible_contract = possible_sample_df.groupBy("paragraph_context").count().withColumnRenamed("count","positive_contract_num")
# possible_contract.show(5)

# poss_dfColumns = ["data_source","qas_question","answer_start","answer_end"]
# poss_df = poss_rdd.toDF(poss_dfColumns).cache()
# poss_df.show(3)

# positive_simple_df = poss_df.where(col("answer_start") != '0')
# positive_simple_num_df = positive_simple_df.where(col("answer_end") != '0').groupBy("qas_question").count().withColumnRenamed("count","positive_simple_num")
# positive_simple_num_df.show(5)

# count_table = final_table_df.where(
#   col("qas_is_impossible") == True
# ).join(positive_simple_num_df,"qas_question")
# count_table = count_table.join(possible_contract,"paragraph_context").cache()
# count_table.show(5)

# count_table_rdd = count_table.rdd
# count_table_rdd = count_table_rdd.map(row_to_tuple)
# count_table_rdd.take(5)

# def count_negtive_sample(record):
#   output = []
#   if record[5] % record[6] == 0:
#     n = record[5] / record[6]
#   elif (record[5] % record[6] != 0) and (float(record[5] / record[6])-int(record[5] / record[6]) >= 0.5):
#     n = record[5]//record[6]+1
#   elif (record[5] % record[6] != 0) and (record[5] / record[6]-record[5] // record[6] <= 0.5):
#     n = record[5]//record[6]
#   output.append((record[0],record[1],record[2],record[3],record[4],n))
#   return output
# count_netiv_num_rdd = count_table_rdd.flatMap(count_negtive_sample)
# count_netiv_num_rdd.take(5)

# def create_negative_sample(record):
#   neg_record = []
#   subStrings = []
#   window = 4096
#   stride = 2048
#   num = int(record[5])
#   length_record = len(record[0])
#   for i in range(0, length_record, stride):
#     if i+window <= length_record:
#       temp = record[0][i:i+window]
#     else:
#       temp = record[0][i:]
#     subStrings.append(temp)
#   for j in range(len(subStrings)):
#       neg_record.append(Row(source=record[0][j*stride:j*stride+window], question=record[1], answer_start=0, answer_end=0))
#   random.shuffle(neg_record)
#   neg_record = neg_record[:num]
#   return neg_record
# imposs_rdd = count_netiv_num_rdd.flatMap(create_negative_sample).cache()
# imposs_rdd.take(3)

# output_rdd = poss_rdd.union(imposs_rdd)
# output_rdd.take(5)

# output_rddColumns = ["source","question","answer_start","answer_end"]
# output_df = output_rdd.toDF(output_rddColumns).cache()
# output_df.show()

