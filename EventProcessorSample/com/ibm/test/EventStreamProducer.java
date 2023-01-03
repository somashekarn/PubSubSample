package com.ibm.kafka.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventStreamProducer {

	private final static String TOPIC = "sampletopic";

	private final static String BOOTSTRAP_SERVERS = "";
	private final static String PASSWORD = "<API_KEY>";
	private final static String INPUT_PAYLOAD = "<PATH>/sample_payload.dat";
	

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"token\" password=\""+PASSWORD+"\";";
		props.put("sasl.jaas.config", jaasConfig);
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.mechanism", "PLAIN");
		props.put("ssl.protocol", "TLSv1.2");
		props.put("ssl.enabled.protocols", "TLSv1.2");
		props.put("ssl.endpoint.identification.algorithm", "HTTPS");
		return new KafkaProducer<>(props);
	}

	static void runProducerFromFile() throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		File fileName = new File(INPUT_PAYLOAD);

		if (!fileName.exists()) {
			System.out.println("Invalid file:" + INPUT_PAYLOAD);
			return;
		}

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));

		try {
			long index = time;
			do {
				String line = reader.readLine();

				if (line == null) {
					// EOF
					break;
				}

				line = line.replaceAll("LUD", String.valueOf(index));

				final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index++, line);

				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("Sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);

			} while (true);
		} finally {
			producer.flush();
			producer.close();
			reader.close();
		}
	}

	public static void main(String... args) throws Exception {
		runProducerFromFile();
	}
}

