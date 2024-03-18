package org.baylight.redis;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.baylight.redis.rdb.OpCode;
import org.baylight.redis.rdb.RdbFileParser;

public class DatabaseReader {

    private final File dbFile;
    private final Map<String, StoredData> dataStoreMap;

    public DatabaseReader(File dbFile, Map<String, StoredData> dataStoreMap) {
        this.dbFile = dbFile;
        this.dataStoreMap = dataStoreMap;
    }

    public void readDatabase() throws IOException {
        // open file and read as input stream
        System.out.println("Reading database file: " + dbFile);
        System.out.println("File size: " + dbFile.length());
        InputStream dbFileInput = new FileInputStream(dbFile);
        try {
            RdbFileParser rdbFileParser = new RdbFileParser(new BufferedInputStream(dbFileInput));
            OpCode dbCode = rdbFileParser.initDB();
            // skip AUX section
            if (dbCode == OpCode.AUX) {
                dbCode = rdbFileParser.skipAux();
            }
            // select DB (0)
            if (dbCode == OpCode.SELECTDB) {
                rdbFileParser.selectDB(dataStoreMap);
            } else {
                throw new IOException(
                        String.format("Database unexpected OpCode: 0x%X not equal to 0x%X",
                                dbCode.getCode(), OpCode.SELECTDB.getCode()));
            }
        } finally {
            // close file
            dbFileInput.close();
        }
    }

}
