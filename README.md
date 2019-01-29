# car-data-spark-stream-maprdb

Usage

Submit the job to spark with table and toppic as parameters

/spark-submit --master yarn --deploy-mode client car-data-spark-stream-maprdb-1.0-SNAPSHOT.jar -n "/user/mapr/connected-car/streams/car-data:all" -t "/user/mapr/tables/car-data"
