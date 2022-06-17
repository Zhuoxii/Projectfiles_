spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 8 \
    pyspark_df.py \
    --output $1
