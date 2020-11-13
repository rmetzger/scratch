package de.robertmetzger.statemachine.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.util.Collector;

import de.robertmetzger.statemachine.event.Event;
import de.robertmetzger.statemachine.event.EventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NewEventDeserializer implements KafkaRecordDeserializer<Event> {
    @Override
    public void deserialize(
        ConsumerRecord<byte[], byte[]> record, Collector<Event> collector) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(record.value()).order(ByteOrder.LITTLE_ENDIAN);
        int address = buffer.getInt(0);
        int typeOrdinal = buffer.getInt(4);
        collector.collect(new Event(EventType.values()[typeOrdinal], address));
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
