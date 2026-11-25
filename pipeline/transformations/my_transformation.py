
from pyspark import pipelines as dp


SOURCE_PATH = "/Volumes/workspace/rp_landing/raw/landing/sales_daily"

@dp.materialized_view(
    name="m0_pipeline_test"
)
def m0_pipeline_test():

    sales_df = (
        spark.read
        .format("json")
        .load(SOURCE_PATH)
    )

    return sales_df
