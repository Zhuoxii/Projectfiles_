spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors  4 \
    b.py \
    --output $1
