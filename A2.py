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
    .appName("Comp5349 Assignment2") \
    .getOrCreate() 



# Configuration
spark.conf.set("spark.sql.shuffle.partitions", 5)


# Read the dataset

# s3://comp5349-2022/test.json
# s3://comp5349-2022/train_separate_questions.json
# s3://comp5349-2022/CUADv1.json
data = "s3://comp5349-2022/train_separate_questions.json"
df= spark.read.option('multiline', 'true').json(data)

spark.sparkContext.setLogLevel("ERROR")

parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='week8_out')
args = parser.parse_args()
output_path = args.output

# Phase 1: Convert json format to DataFrame.

df =  df.select(explode("data.paragraphs").alias("data"))\
         .select('data',explode("data.context").alias("context"))\
         .select('data', 'context', explode("data.qas").alias("qas"))\
         .select('context', 'qas', explode("qas").alias("qas2"))\
         .select('context', 'qas2', "qas2.question", "qas2.answers","qas2.is_impossible")\
         .cache()  

df.show()
print("Phase 1: Convert json format to DataFrame")




# Phase 1: Divide the whole DF into three kinds of Sub-DF and add necessary fields.
## Some useful udf
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

  
def is_positive(context, sequence, text, answer_start):
    sequence_start = context.index(sequence)
    sequence_end = sequence_start + len(sequence)
    answer_end = answer_start + len(text)
    if answer_start <= sequence_start and answer_end >= sequence_start:
      return 'positive'
    elif answer_start >= sequence_start and answer_start <= sequence_end:
      return 'positive'
    else:
      return 'possible negative'


def answer_index(context, sequence, text, answer_start):
    sequence_start = context.index(sequence)
    sequence_end = sequence_start + len(sequence)

    answer_end = answer_start + len(text)
    if answer_start < sequence_start and answer_end < sequence_end:
      return  [0,len(text)]
    elif answer_start < sequence_start and answer_end > sequence_end:
      return [0,len(sequence)]
    elif answer_start > sequence_start and answer_end < sequence_end:
      return [sequence.index(text), sequence.index(text)+len(text)]
    else:
      new_text = context[answer_start: sequence_end]
      return [sequence.index(new_text), len(sequence)]
    


udf1=udf(segmentToSequence, ArrayType(StringType()))  
udf2=udf(is_positive, StringType()) 
udf3=udf(answer_index, ArrayType(IntegerType()))



## Phase 2.1: DataFrame for impossible_negative samples
df_no_answer = df.filter(df.is_impossible == True).select('context','question','answers' ,"is_impossible")
df_impossible_negative = df_no_answer.withColumn('list_context',udf1('context'))\
                      .withColumn("source",f.explode('list_context'))\
                      .withColumn('type', lit('impossible negative'))\
                      .withColumn('answer_start', lit(0))\
                      .withColumn('answer_end', lit(0))\
                      .select('context', 'source', 'question', 'answer_start', 'answer_end', 'type')\
                      .orderBy('context','question','source')

df_impossible_negative.show()
print("Phase 2.1 Generate DataFrame for impossible_negative samples")




## Phase 2.2: DataFrame for positive and possible_negative samples
df_answer = df.withColumn('answers2', explode('answers').alias('answers2'))\
            .select('context','question','answers2.text', 'answers2.answer_start',"is_impossible")\
            .withColumn('list_context',udf1('context'))\
            .withColumn("source",f.explode('list_context')).select( 'context', 'source', 'question','text', 'answer_start') \
            .withColumn('type',udf2('context', 'source', 'text', 'answer_start'))\
            .cache()
            
df_answer.show()
df_answer.write.csv(output_patch)
spark.stop()
