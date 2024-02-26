from src import data_generator as dg, spark_session as ss, source_to_target as etl
import logging
# Set up logging configuration
logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logging.info("starting data  generation.....")
    dg.generate_data()
    logging.info("Data generation completed.")
    logging.info("Trying to fire up spark cluster")

    spark = ss.start_spark_session()
    logging.info("Spark Session created.")

    etl.source_to_target(spark)
    logging.info("data loaded to successfully.")

    ss.stop_spark_session(spark)
    logging.info("Spark sesson killed.")
