package io.pravega.cloudevent;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class CloudEventSerializerTest {

    @Test
    public void testRoundTrip() {
        UUID u = UUID.randomUUID();
        String source = "urn://pravega.io/cloudevent/test";
        String type = "urn://pravega.io/TestEvent";
        String testSubject = "Test Event";
        OffsetDateTime dt = OffsetDateTime.now();
        CloudEventBuilder template = CloudEventBuilder.v03();
        template = template.withId(u.toString());
        template = template.withSource(URI.create(source));
        template = template.withType(type);

        Map<String,String> outValues = new HashMap<>();

        CloudEventSerializer<String> s = new CloudEventSerializer<>(
                template,
                new UTF8StringSerializer(),
                (value, t) -> {
                    t = t.withSubject(testSubject);
                    return t;
                },
                (ev) -> {
                    outValues.put("id", ev.getId());
                    outValues.put("subject", ev.getSubject());
                    outValues.put("source", ev.getSource().toString());
                    outValues.put("type", ev.getType());
                }
        );

        String testString = "fhqwhgads";

        ByteBuffer serializedMessage = s.serialize(testString);
        assertNotNull(serializedMessage);
        assertTrue(serializedMessage.capacity() > 0);

        serializedMessage.rewind();
        String outString = s.deserialize(serializedMessage);

        assertEquals(testString, outString);
        assertEquals(u.toString(), outValues.get("id"));
        assertEquals(testSubject, outValues.get("subject"));
        assertEquals(source, outValues.get("source"));
        assertEquals(type, outValues.get("type"));
    }
}