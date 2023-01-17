package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.JacksonHandle;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class JsonPlanInvoker implements PlanInvoker {

    private DatabaseClient client;
    private Map<String, Object> parsedConfig;

    public JsonPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        this.parsedConfig = parsedConfig;
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        JacksonHandle baseHandle = new JacksonHandle();
        JacksonHandle result = client.newRowManager().resultDoc(plan, baseHandle);
        List<SourceRecord> records = new ArrayList<>();
        /**
         * Testing has shown that converting the JSON to a string and using
         * org.apache.kafka.connect.storage.StringConverter as the value converter works well. And a user could
         * still choose to use org.apache.kafka.connect.json.JsonConverter, though they would most likely want
         * to set "value.converter.schemas.enable" to "false".
         */
        JsonNode doc = result.get();
        if (doc != null && doc.has("rows")) {
            KeyGenerator keyGenerator = KeyGenerator.newKeyGenerator(this.parsedConfig, baseHandle.getServerTimestamp());
            for (JsonNode row : doc.get("rows")) {
                records.add(new SourceRecord(null, null, topic, null, keyGenerator.generateKey(), null, row.toString()));
            }
        }
        return new Results(records, baseHandle.getServerTimestamp());
    }
}
