spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    b.py \
    --output $1
