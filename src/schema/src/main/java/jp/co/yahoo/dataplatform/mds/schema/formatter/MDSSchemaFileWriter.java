package jp.co.yahoo.dataplatform.mds.schema.formatter;

import java.io.*;
import java.util.List;
import java.util.Map;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;


public class MDSSchemaFileWriter implements Closeable, IStreamWriter {
    private final MDSSchemaStreamWriter writer;

    public MDSSchemaFileWriter(final File file, final Configuration config) throws IOException {
        OutputStream out = new FileOutputStream(file);
        writer = new MDSSchemaStreamWriter(out, config);
    }

    @Override
    public void write( final PrimitiveObject obj ) throws IOException{
        writer.write(obj);
    }

    @Override
    public void write( final List<Object> array ) throws IOException{
        writer.write(array);
    }

    @Override
    public void write( final Map<Object,Object> map ) throws IOException{
        writer.write(map);
    }

    @Override
    public void write( final IParser parser ) throws IOException{
        writer.write(parser);
    }

    @Override
    public void close() throws IOException{
        writer.close();
    }

}
