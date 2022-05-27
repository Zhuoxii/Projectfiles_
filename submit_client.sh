spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    a.py \
    --output $1
