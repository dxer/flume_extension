package org.dxer.flume.serializer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by linghf on 2017/2/16.
 */

public class CustomHbaseEventSerializer implements HbaseEventSerializer {

    public static final String COLUMN_FAMILY = "columnFamily";
    public static final String COL_NAME = "colNames";
    public static final String COL_INDEXES = "colIndexes";
    public static final String CHARSET_CONFIG = "charset";
    public static final String CHARSET_DEFAULT = "UTF-8";
    public static final String BODY_SPLIT_SYMBOL = "bodySplit";
    public static final String BODY_SPLIT_SYMBOL_DEFAULT = "\t";

    protected byte[] columnFamily;
    private byte[] payload;
    private List<byte[]> colNames = Lists.newArrayList();
    private Map<String, String> headers;
    private boolean regexIgnoreCase;
    private boolean depositHeaders;
    private Pattern inputPattern;
    private Charset charset;
    private int rowKeyIndex;
    private String bodySplit = null;

    private boolean isGlobalCF = true;

    private Map<String, Integer> colNameIndexMap = Maps.newLinkedHashMap();

    public void configure(Context context) {
        String colNameStr = null;

        String colIndexStr = context.getString(COL_INDEXES, null);
        String[] colIndexes = null;
        if (!Strings.isNullOrEmpty(colIndexStr)) {
            colIndexes = colIndexStr.split(",");
        }

        colNameStr = context.getString(COL_NAME, null);
        String[] colNames = null;
        if (!Strings.isNullOrEmpty(colNameStr)) {
            colNames = colNameStr.split(",");
        }

        if (colNames != null && colIndexes != null) {
            int colNamesLength = colNames.length;
            int colIndexesLength = colIndexes.length;
            int length = colIndexesLength >= colIndexesLength ? colIndexesLength : colIndexesLength;
            for (int i = 0; i < length; i++) {
                try {
                    colNameIndexMap.put(colNames[i], Integer.parseInt(colIndexes[i].trim()));
                } catch (Exception e) {

                }
            }
        }


        charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));
        bodySplit = context.getString(BODY_SPLIT_SYMBOL, BODY_SPLIT_SYMBOL_DEFAULT);
    }

    public void configure(ComponentConfiguration componentConfiguration) {

    }

    public void initialize(Event event, byte[] columnFamily) {
        this.headers = event.getHeaders();
        this.payload = event.getBody();
        this.columnFamily = columnFamily;
        if (columnFamily == null || columnFamily.length <= 0) {
            isGlobalCF = false;
        }
    }


    private String[] splitColName(String colName) {
        if (!Strings.isNullOrEmpty(colName)) {
            String[] strs = colName.split(":");
            return strs;
        }
        return null;
    }

    public List<Row> getActions() {
        List<Row> actions = Lists.newArrayList();
        try {
            byte[] rowKey = null;
            String body = new String(payload, charset);

            String[] splits = body.split(bodySplit);

            Put put = new Put(rowKey);

            byte[] cf = null;
            String col = null;
            boolean isInsert = false;
            for (String colName : colNameIndexMap.keySet()) {
                Integer index = colNameIndexMap.get(colName);
                if (Strings.isNullOrEmpty(colName) || index == null) {
                    continue;
                }
                String value = splits[index];
                if (!isGlobalCF) { // 不是全局变量
                    String[] strs = splitColName(colName);
                    if (strs != null && strs.length == 2) {
                        cf = strs[0].getBytes();
                        col = strs[1];
                    }
                } else {
                    cf = this.columnFamily;
                }
                if (cf != null && !Strings.isNullOrEmpty(col) && !Strings.isNullOrEmpty(value)) {
                    put.addColumn(cf, Bytes.toBytes(col), Bytes.toBytes(value));
                    isInsert = true;
                }
            }

            if (isInsert) {
                actions.add(put);
            }
        } catch (Exception e) {
            throw new FlumeException("Could not get row key!", e);
        }
        return actions;
    }

    public List<Increment> getIncrements() {
        return null;
    }

    public void close() {

    }
}
