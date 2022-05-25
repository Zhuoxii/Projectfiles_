spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    A2.py \
    --output $1
