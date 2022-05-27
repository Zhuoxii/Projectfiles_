spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 7 \
    new.py \
    --output $1
