databricks bundle validate -t dev -p $1 && 
databricks bundle deploy -t dev -p $1 &&
databricks bundle run coffee_weather -p $1 -t dev