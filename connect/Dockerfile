FROM confluentinc/cp-kafka-connect:7.6.1

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

RUN curl -fSL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar

RUN ls -l /usr/share/confluent-hub-components/
