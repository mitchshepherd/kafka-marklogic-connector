package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.RowBatchSuccessListener;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public class XmlStringRowBatcherBuilder extends AbstractRowBatchBuilder implements RowBatcherBuilder<String> {

    XmlStringRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        super(dataMovementManager, parsedConfig);
    }

    public RowBatcher<String> newRowBatcher(List<SourceRecord> newSourceRecords) {
        RowBatcher<String> rowBatcher = dataMovementManager.newRowBatcher(new StringHandle().withFormat(Format.XML));
        configureRowBatcher(parsedConfig, rowBatcher);
        rowBatcher.onSuccess(event -> onSuccessHandler(event, newSourceRecords));
        return rowBatcher;
    }

    private void onSuccessHandler(RowBatchSuccessListener.RowBatchResponseEvent<String> event, List<SourceRecord> newSourceRecords) {
        try {
            long start = System.currentTimeMillis();
            String xml = event.getRowsDoc();
            xml = xml.substring(xml.indexOf("<t:row>"));
            xml = xml.substring(0, xml.indexOf("</t:rows>"));
            String[] rows = xml.split("</t:row>" + "\\s+" + "<t:row>");
            for (String row : rows) {
                if (row == null) {
                    System.out.println("HOW IS THIS NULL??? ");
                    continue;
                }
                row = row.trim();
                if (row.startsWith("<t:row>")) {
                    row = row.replaceFirst("<t:row>", "<t:row xmlns:t=\"http://marklogic.com/table\">");
                } else {
                    row = "<t:row xmlns:t=\"http://marklogic.com/table\">\n" + row;
                }
                if (!row.endsWith("</t:row>")) {
                    row += "\n</t:row>";
                }
                newSourceRecords.add(new SourceRecord(null, null, topic, null, row));
            }
            System.out.println("XML DURATION: " + (System.currentTimeMillis() - start));
        } catch (Throwable ex) {
            logBatchError(ex);
        }
    }
}
