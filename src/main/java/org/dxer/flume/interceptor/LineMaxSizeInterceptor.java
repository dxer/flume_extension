/**
 * Copyright (c) 2017 21CN.COM . All rights reserved.
 * <p/>
 * Description: flume_plugin
 * <p/>
 * <pre>
 * Modified log:
 * ------------------------------------------------------
 * Ver.		Date		Author			Description
 * ------------------------------------------------------
 * 1.0		2017年2月16日	linghf		created.
 * </pre>
 */
package org.dxer.flume.interceptor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

/**
 * <p/>
 * # 忽略整行数据
 * a1.sources.r1.interceptors=i1
 * a1.sources.r1.interceptors.i1.type=com._21cn.common.flume.interceptor.LineMaxSizeInterceptor$Builder
 * # 最大字节数
 * a1.sources.r1.interceptors.i1.lineMaxSize=2
 * # 是否忽略整行
 * a1.sources.r1.interceptors.i1.ingoreLine=true
 * # 日志的字符集，默认utf-8
 * a1.sources.r1.interceptors.i1.charset=utf-8
 * <p/>
 * <p/>
 * # 忽略一行中某个字段数据
 * a1.sources.r1.interceptors=i1
 * a1.sources.r1.interceptors.i1.type=com._21cn.common.flume.interceptor.LineMaxSizeInterceptor$Builder
 * # 最大字节数
 * a1.sources.r1.interceptors.i1.lineMaxSize=2
 * # 是否忽略整行
 * a1.sources.r1.interceptors.i1.ingoreLine=false
 * # 要忽略字段数据的下标（从0开始，会以“-”填充）
 * a1.sources.r1.interceptors.i1.emptyColIndexes=0,1
 * # 日志分隔符
 * a1.sources.r1.interceptors.i1.lineSeparator=\t
 * <p/>
 * <p/>
 * 行最大长度拦截器
 */
public class LineMaxSizeInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(LineMaxSizeInterceptor.class);

    /**
     * 一行数据的最大长度，小于等于0的话，不考虑该条件
     */
    private long lineMaxSize = -1l;

    private Charset charset;

    private boolean ignoreLine = false;

    private String separator = "\t";

    private List<Integer> emptyColIndexes = Lists.newArrayList();

    public LineMaxSizeInterceptor(
            long lineMaxSize, boolean ignoreLine, List<Integer> emptyColIndexes,
            String separator, Charset charset) {
        this.lineMaxSize = lineMaxSize;
        this.ignoreLine = ignoreLine;
        this.charset = charset;
        this.emptyColIndexes = emptyColIndexes;
        this.separator = separator;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void initialize() {
        // TODO Auto-generated method stub
    }

    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return event;
        }

        byte[] body = event.getBody();
        long length = body.length;
        logger.info("bodyLenght: " + length);
        if (length > lineMaxSize) { // 长度大于最大值
            if (ignoreLine) { // 忽略整行
                return null;
            } else {
                String bodyStr = new String(body, charset);
                String[] bodyArray = bodyStr.split(separator);

                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < bodyArray.length; i++) {
                    if (emptyColIndexes.contains(i)) {
                        bodyArray[i] = "-";
                    }
                    builder.append(bodyArray[i]);
                    if (i != bodyArray.length - 1) {
                        builder.append(separator);
                    }
                }
                byte[] newBody = builder.toString().getBytes(charset);
                if (newBody.length > lineMaxSize) { // 如果处理之后的newBody还是太长，就直接忽略这行数据
                    return null;
                }
                event.setBody(newBody); // 替换新的内容
            }
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    public static class Builder implements Interceptor.Builder {

        private static final String LINE_MAX_SIZE = "lineMaxSize";

        private static final String CHARSET_KEY = "charset";

        private static final String INGORE_LINE = "ignoreLine";

        private static final String LINE_SEPARATOR = "lineSeparator";

        private static final String EMPTY_COL_INDEXES = "emptyColIndexes";

        private Long lineMaxSize = null;

        private Charset charset = Charset.defaultCharset();

        /**
         * 是否忽略整条数据
         */
        private boolean ignoreLine = false;

        /**
         * 数据分隔符
         */
        private String separator = "\t";

        /**
         * 需要设置为空的字段编号
         */
        private String emptyColIndexStr;

        private List<Integer> emptyColIndexes = Lists.newArrayList();

        @Override
        public void configure(Context context) {
            lineMaxSize = context.getLong(LINE_MAX_SIZE, null);
            Preconditions.checkNotNull(lineMaxSize, "lineMaxSize required");
            ignoreLine = context.getBoolean(INGORE_LINE, false);
            separator = context.getString(LINE_SEPARATOR, "\t");
            emptyColIndexStr = context.getString(EMPTY_COL_INDEXES, null);
            if (context.containsKey(CHARSET_KEY)) {
                charset = Charset.forName(context.getString(CHARSET_KEY));
            } else {
                charset = Charset.defaultCharset();
            }

            if (Strings.isNullOrEmpty(emptyColIndexStr)) {
                this.ignoreLine = true;
            } else {
                this.ignoreLine = false;
                String[] strs = emptyColIndexStr.split(",");
                for (String str : strs) {
                    try {
                        emptyColIndexes.add(Integer.parseInt(str.trim()));
                    } catch (Exception e) {
                    }
                }
            }

            Preconditions.checkArgument(
                    (ignoreLine) || !ignoreLine && emptyColIndexes != null && emptyColIndexes.size() > 0,
                    "ignoreLine is true or emptyColIndexes not empty");
        }

        @Override
        public Interceptor build() {
            return new LineMaxSizeInterceptor(lineMaxSize, ignoreLine, emptyColIndexes, separator, charset);
        }

    }

}
