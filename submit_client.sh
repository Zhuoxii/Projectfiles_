spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 8 \
    A2.py \
    --output $1
