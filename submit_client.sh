spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    new.py \
    --output $1
