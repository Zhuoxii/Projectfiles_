spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    new.py \
    --output $1
