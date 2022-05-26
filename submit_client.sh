spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors  4 \
    new.py \
    --output $1
