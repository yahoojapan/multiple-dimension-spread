package jp.co.yahoo.dataplatform.mds.schema.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Closeable;
import jp.co.yahoo.dataplatform.config.Configuration;
import javax.xml.ws.WebServiceException;
import jp.co.yahoo.dataplatform.schema.parser.IStreamReader;
import jp.co.yahoo.dataplatform.schema.parser.IParser;


public class MDSSchemaFileReader implements Closeable, IStreamReader {
    private final MDSSchemaReader reader;
    public MDSSchemaFileReader(final File file, final Configuration config) throws IOException{
        reader = new MDSSchemaReader();
        InputStream in = new FileInputStream(file);
        reader.setNewStream(in, file.length(), config);
    }


    @Override
    public boolean hasNext() throws IOException{
        return reader.hasNext();
    }

    @Override
    public IParser next() throws IOException{
        return reader.next();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
