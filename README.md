# sd3





levantar instancias

```bash
   docker-compose up 
   ```

iniciar el scraper
```bash
  node scraper.js
   ```

realizar paso de mensajes vía kafka a spark
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.authenticate=false kafka_spark_integration.py
   ```


