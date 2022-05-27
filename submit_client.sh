spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    A2.py \
    --output $1
