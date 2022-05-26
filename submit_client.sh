spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors  8 \
    a.py \
    --output $1
