spark-submit \
    --master yarn \
    --deploy-mode client \
    work.py \
    --output $1
