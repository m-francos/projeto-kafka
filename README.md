
# Kafka and Spark Challenge: E-commerce Sales Simulation

This project aims to simulate an e-commerce sales system using Apache Kafka for real-time message transmission and PySpark for processing those messages.

## Challenge Description

The challenge involves creating a sales data flow using Kafka to transmit messages and PySpark to process them in real-time. The main steps include:

1. **Install Apache Kafka:** Set up the Kafka environment with Zookeeper and create a topic for sales messages.
2. **Create a Python Message Producer:** Develop a producer that sends simulated sales data messages to Kafka.
3. **Create a Message Consumer in PySpark:** Develop a PySpark consumer that reads messages from Kafka, processes the data, and displays the total sales grouped by product.

## Challenge Steps

1. **Install Apache Kafka**

   Ensure that Apache Kafka and Zookeeper are installed on your machine.

   Start Zookeeper:

   ```bash
   $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
   ```

   Start Kafka server:

   ```bash
   $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
   ```

   Create the `vendas_ecommerce` topic:

   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --create --topic vendas_ecommerce --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

2. **Set up the Environment**

   Clone the repository and create a virtual environment:

   ```bash
   git clone <your repo url>
   python3 -m venv venv
   source venv/bin/activate
   ```

   Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Message Producer**

   Run the Python script `producer_vendas_ecommerce.py` to generate and send sales data messages to Kafka:

   ```bash
   python3 producer_vendas_ecommerce.py
   ```

4. **Run the Consumer with PySpark**

   Run the consumer in PySpark with the Kafka connector to process the messages:

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer_pyspark_ecommerce.py
   ```

5. **Stop the Process**

   To stop any Kafka service (Zookeeper, Producer, or Consumer), press `Ctrl + C` in the terminal.

## Project Structure

- `producer_vendas_ecommerce.py`: Python script that sends simulated sales data to Kafka.
- `consumer_pyspark_ecommerce.py`: PySpark script that reads messages and processes the data.
- `requirements.txt`: File containing project dependencies.
- `README.md`: Instructions and project details.

## Conclusion

This project demonstrates the integration of Apache Kafka and PySpark to process sales data in real time. It is useful for simulating e-commerce scenarios and can be expanded to other real-time monitoring and analysis applications.


