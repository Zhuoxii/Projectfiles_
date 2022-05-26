from pyspark.sql.functions import udf, array, explode, concat, col
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import argparse

spark = SparkSession \
    .builder \
    .appName("xzha6842 COMP5349 Assignment2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='a2_out')
args = parser.parse_args()
output_path = args.output

train_data = "s3://comp5349-2022/test.json"
train_init_df = spark.read.json(train_data).cache()

schema = ArrayType(StructType([
    StructField("start", LongType(), False),
    StructField("end", LongType(), False),
    StructField("source", StringType(), False)
]))

def setType(AnswerStart,AnswerEnd,is_impossible):
  if is_impossible: # impossible negative
    return 0
  elif not is_impossible and AnswerStart==0 and AnswerEnd==0: # possible negative
    return 1
  else: # positive
    return 2

def calculateAveragePositiveInOtherContract(TotalNumber,Num_of_Contract):
  if Num_of_Contract == 1:
    return 0
  return TotalNumber//(Num_of_Contract-1)

def getElementsInList(input_data,numOfElements):
  numOfElements = int(numOfElements)
  return input_data[:numOfElements]

def getFirst(input_data):
  return input_data[0]
  
def getAnswerStart(answers):
  startIndex = answers['answer_start']
  data_input = answers['text']
  return startIndex+len(data_input)-len(data_input)

def getAnswerEnd(answers):
  startIndex = answers['answer_start']
  data_input = answers['text']
  return startIndex+len(data_input)

def sliding_windows(data):
  window_size = 4096
  sliding_size = 2048
  tail,head = 0,window_size
  result = []
  if len(data)<=4096:
    result.append(data)
    return result
  while tail<len(data):
    result.append(data[tail:head])
    head += sliding_size
    tail += sliding_size
  return result

def sliding_windows_location(data,answerStart,answerEnd):
  window_size = 4096
  sliding_size = 2048
  tail,head = 0,window_size
  result = []
  if len(data)<=4096:
    result.append((answerStart,answerEnd,data))
    return result
  while tail<len(data):
    if answerStart<=tail and tail<=answerEnd<=head:
      result.append((0,answerEnd-tail,data[tail:head]))
    elif tail<=answerStart<=head and tail<=answerEnd<=head:
      result.append((answerStart-tail,answerEnd-tail,data[tail:head]))
    elif tail<=answerStart<=head and answerEnd>=head:
      result.append((answerStart-tail,4095,data[tail:head]))
    else:
      result.append((0,0,data[tail:head]))
    head += sliding_size
    tail += sliding_size
  return result

def getAnswerStartFromTuple(data):
  return data['start']

def getAnswerEndFromTuple(data):
  return data['end']
  
def getAnswerSourceFromTuple(data):
  return data['source']

udf_segment = udf(sliding_windows,ArrayType(StringType()))
udf_sliding_windows_location = udf(sliding_windows_location,schema)
udf_answerStart = udf(getAnswerStart,LongType())
udf_answerEnd = udf(getAnswerEnd,LongType())
udf_LocStart = udf(getAnswerStartFromTuple,LongType())
udf_LocEnd = udf(getAnswerEndFromTuple,LongType())
udf_source = udf(getAnswerSourceFromTuple,StringType())
udf_type = udf(setType,LongType())
udf_calculateAveragePositiveInOtherContract = udf(calculateAveragePositiveInOtherContract,LongType())
udf_getElementsInList = udf(getElementsInList,ArrayType(StringType()))
udf_getFirst = udf(getFirst,LongType())

df_train_title = train_init_df.select(explode("data.title").alias("title")).withColumn("index",f.monotonically_increasing_id())
df_train_para = train_init_df.select(explode("data.paragraphs").alias("data")).withColumn("index",f.monotonically_increasing_id())

df_train = df_train_title.join(df_train_para,df_train_title.index==df_train_para.index).drop("index")\
    .select("title","data",explode("data.context").alias("context"))\
    .select("title","context","data",explode("data.qas").alias("qas"))\
    .select("title","context","data",explode("qas").alias("qas"))\
    .select("title","context","qas.id","qas.question","qas.answers","qas.is_impossible")

df_train_impossible = df_train.filter(df_train.is_impossible == "true")
df_train_possible = df_train.filter(df_train.is_impossible == "false")\
  .select("title","context","id","question",explode("answers").alias("answers"),"is_impossible")

# #source, question, AnswerStart, AnswerEnd
# # possible negatives + positives

# # For each contract, the number of possible negative samples to keep for each question
# # equals the number of positive samples of that question in this contract

df_train_possible_aaa = df_train_possible.withColumn('answerStart',udf_answerStart(df_train_possible.answers)).withColumn('answerEnd',udf_answerEnd(df_train_possible.answers)).drop('answers')
df_train_possible_aaa = df_train_possible_aaa.withColumn('answer_loc',udf_sliding_windows_location(df_train_possible_aaa.context,df_train_possible_aaa.answerStart,df_train_possible_aaa.answerEnd))\
    .drop('context','answerStart','answerEnd')
df_train_possible_aaa = df_train_possible_aaa.withColumn("answer_loc",f.explode("answer_loc"))
df_train_possible_aaa = df_train_possible_aaa.withColumn('source',udf_source(df_train_possible_aaa.answer_loc))\
    .withColumn('AnswerStart',udf_LocStart(df_train_possible_aaa.answer_loc))\
    .withColumn('AnswerEnd',udf_LocEnd(df_train_possible_aaa.answer_loc))\
    .drop('answer_loc')

df_train_possible_seg = df_train_possible_aaa.withColumn('seg_type',udf_type('AnswerStart','AnswerEnd',df_train_possible_aaa.is_impossible))\
    .select("title","id","source","question","AnswerStart","AnswerEnd","seg_type")

df_train_possible_negative = df_train_possible_seg.filter(df_train_possible_seg.seg_type == 1)
df_train_positive = df_train_possible_seg.filter(df_train_possible_seg.seg_type == 2)

df_train_impossible_negative = df_train_impossible.withColumn('source',udf_segment(df_train_impossible.context))\
    .withColumn("source",f.explode("source"))\
    .withColumn("AnswerStart", f.lit(0))\
    .withColumn("AnswerEnd", f.lit(0))\
    .withColumn('seg_type',udf_type('AnswerStart','AnswerEnd',df_train_impossible.is_impossible))\
    .select("title","id","source","question","AnswerStart","AnswerEnd","seg_type")\

df_train_impossible_negative = df_train_impossible_negative.join(df_train_positive,['source'],'left_anti')
df_train_possible_negative = df_train_possible_negative.join(df_train_positive,['source'],'left_anti')

############################### The Number of Impossible Negatives ###############################
df_train_count_total = df_train_positive.groupBy('question').count().withColumnRenamed("question","_question").withColumnRenamed("count","TotalNumber")
df_train_count_num_of_contract = df_train_positive.withColumn("counter",f.lit(1)).groupBy("title","question").sum('counter').withColumn("Question_Count",f.lit(1)).groupBy("question").sum('Question_Count').withColumnRenamed("sum(Question_Count)","Num_of_Contract")
df_train_count_IN = df_train_count_total.join(df_train_count_num_of_contract,df_train_count_total._question==df_train_count_num_of_contract.question).drop('question')
df_train_count_IN = df_train_count_IN.withColumn("IN_Count",udf_calculateAveragePositiveInOtherContract(df_train_count_IN.TotalNumber,df_train_count_IN.Num_of_Contract)).drop("TotalNumber","Num_of_Contract")

df_train_process_IN = df_train_impossible_negative.join(df_train_count_IN,df_train_impossible_negative.question==df_train_count_IN._question).drop("_question")
df_train_process_IN = df_train_process_IN.groupBy("title","question").agg(f.collect_list(df_train_process_IN.source),f.collect_list(df_train_process_IN.IN_Count)).withColumnRenamed("collect_list(source)","source").withColumnRenamed("collect_list(IN_Count)","IN_Count")
df_train_process_IN = df_train_process_IN.withColumn("IN_Count",udf_getFirst(df_train_process_IN.IN_Count))
df_train_process_IN = df_train_process_IN.withColumn("source",udf_getElementsInList(df_train_process_IN.source,df_train_process_IN.IN_Count)).withColumn("source",f.explode("source"))
df_train_process_IN = df_train_process_IN.drop("title","IN_Count").withColumn("AnswerStart", f.lit(0)).withColumn("AnswerEnd", f.lit(0))

############################### The Number of Possible Negatives ###############################
# calculate the number of each question which should keep for possible negatives
df_train_positive_count = df_train_positive.withColumn("count",f.lit(1))
df_train_positive_count = df_train_positive_count.groupBy("title","question").sum('count').withColumnRenamed("question","_question").withColumnRenamed("title","_title")

df_train_process_PN = df_train_possible_negative.groupBy("title","question").agg(f.collect_list(df_train_possible_negative.source)).withColumnRenamed("collect_list(source)","source")
df_train_process_PN = df_train_process_PN.join(df_train_positive_count,(df_train_process_PN.title==df_train_positive_count._title)&(df_train_process_PN.question==df_train_positive_count._question)).drop("_title","_question").withColumnRenamed("sum(count)","PN_count")
df_train_process_PN = df_train_process_PN.withColumn("source",udf_getElementsInList(df_train_process_PN.source,df_train_process_PN.PN_count)).withColumn("source",f.explode("source"))
df_train_process_PN = df_train_process_PN.drop("title","PN_count").withColumn("AnswerStart", f.lit(0)).withColumn("AnswerEnd", f.lit(0))

############################### The Number of Positives ###############################
df_train_process_positive = df_train_positive.select('question','source','AnswerStart','AnswerEnd')

############################### Final Results and Json Output ###############################
import json

df_train_result = df_train_process_positive.union(df_train_process_PN)
df_train_result = df_train_result.union(df_train_process_IN)

results = df_train_result.toJSON().collect()
json_train = json.dumps(results)
with open('test_result.json', 'w') as f:
    json.dump(json_train, f)

spark.stop()