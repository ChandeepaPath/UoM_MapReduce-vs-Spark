# Year wise carrier delay from 2003-2010
# Year wise NAS delay from 2003-2010
# Year wise Weather delay from 2003-2010
# Year wise late aircraft delay from 2003-2010
# Year wise security delay from 2003-2010

import argparse

from pyspark.sql import SparkSession

def year_wise_flight_carrier_delay(data_source, output_uri):
  
    with SparkSession.builder.appName("Year wise carrier delay from 2003-2010").getOrCreate() as spark:
        # Load the CSV data
        if data_source is not None:
            flightdelay_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flightdelay_df.createOrReplaceTempView("carrier_flightdelay")

        # -- Year wise carrier delay from 2003-2010
        carrier_delay_flightdelay = spark.sql("""SELECT Year, AVG((CarrierDelay/ArrDelay)*100) AS avg_carrier_delay
            FROM carrier_flightdelay
            WHERE Year BETWEEN 2003 AND 2010
            GROUP BY Year""")

        # Write the results to the specified output URI
        carrier_delay_flightdelay.write.option("header", "true").mode("overwrite").csv(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    year_wise_flight_carrier_delay(args.data_source, args.output_uri)