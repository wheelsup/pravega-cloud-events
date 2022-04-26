package io.pravega.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.protobuf.ProtobufFormat;
import io.pravega.client.stream.Serializer;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class CloudEventSerializer<T> implements Serializer<T> {
    private final CloudEventBuilder template;
    private final Serializer<T> dataSerializer;
    private final BiFunction<T, CloudEventBuilder, CloudEventBuilder> populateEvent;
    private final Consumer<CloudEvent> deserializeHook;
    private final ProtobufFormat protobufFormat;

    /**
     * Create a new CloudEventSerializer
     * @param template a template event to copy for all events. Typically this will have common
     *                 fields filled in like source, type, etc already filled in.
     * @param dataSerializer a Pravega DataSerializer. This will convert a message into bytes so it
     *                       can be embedded in the CloudEvent.
     * @param populateEvent this lambda will be invoked for every message serialzed and gives an opportunity
     *                      to populate CloudEvent fields based on message content (e.g. ID, etc)
     * @param deserializeHook this lambda will be invoked for every message deserialized and gives an
     *                        opportunity to process CloudEvent envelope data, e.g. set up some tracing/logging,
     *                        stash the ID somewhere, etc.
     */
    public CloudEventSerializer(
            CloudEventBuilder template,
            Serializer<T> dataSerializer,
            BiFunction<T, CloudEventBuilder, CloudEventBuilder> populateEvent,
            Consumer<CloudEvent> deserializeHook) {
        this.populateEvent = populateEvent;
        this.dataSerializer = dataSerializer;
        this.deserializeHook = deserializeHook;
        this.template = template;
        this.protobufFormat = new ProtobufFormat();
    }

    @Override
    public ByteBuffer serialize(T value) {
        CloudEventBuilder b = template.newBuilder();
        b = b.withTime(OffsetDateTime.now());

        if(populateEvent != null) {
            b = populateEvent.apply(value, b);
        }

        // Serialize data.
        b = b.withData(dataSerializer.serialize(value).array());

        // Use the Protobuf serializer
        return ByteBuffer.wrap(protobufFormat.serialize(b.build()));
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        CloudEvent ev = protobufFormat.deserialize(serializedValue.array());

        // If user wants to do anything with the CloudEvent itself (e.g. store fields in local context,
        // initialize some tracing, logging, etc.
        if(deserializeHook != null) {
            deserializeHook.accept(ev);
        }

        if(ev.getData() == null) {
            return null;
        }

        // Extract value
        return dataSerializer.deserialize(ByteBuffer.wrap(ev.getData().toBytes()));
    }
}
