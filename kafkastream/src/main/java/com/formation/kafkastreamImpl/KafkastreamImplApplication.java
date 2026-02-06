package com.formation.kafkastreamImpl;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class KafkastreamImplApplication {
	
public static final String INPUT_TOPIC="orders";
	
	public static final String OUTPUT_TOPIC="orders-ca-global";
	
	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
		props.put("bootstrap.servers","localhost:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName() );
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		// Topologie = Ens des transformations sur les données
		final StreamsBuilder builder = new StreamsBuilder();
		
		ObjectMapper mapper = new ObjectMapper();
		
		KStream<String, String> source = builder.stream(INPUT_TOPIC);
		

		// Extraction du total_price
		KStream<String, Double> revenueStream =
    		source.mapValues(value -> {
                try {
                    JsonNode json = mapper.readTree(value);
                    return json.get("total_price").asDouble();
                } catch (Exception e) {
                    return 0.0;
                }
            });
		
		// Regrouper sous une clé unique (CA GLOBAL)
        KGroupedStream<String, Double> grouped =
                revenueStream.groupBy(
                        (key, value) -> "GLOBAL",
                        Grouped.with(Serdes.String(), Serdes.Double()) );
		
		// Agrégation : somme cumulée
        KTable<String, Double> totalRevenue =
                grouped.reduce(
                        Double::sum,
                        Materialized.with(Serdes.String(), Serdes.Double())
                );
		
        // Envoi vers le topic de sortie
        totalRevenue
                .toStream()
                .mapValues(total -> "{ \"global_revenue\": " + total + " }")
                .to("orders-ca-global", Produced.with(Serdes.String(), Serdes.String()));
		
		
		
		// source.to(OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.String()));
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
		kafkaStreams.start();
	}

}
