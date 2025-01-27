package com.file.batch.kafka.service;

import com.file.batch.kafka.util.FileUtil;
import com.file.batch.kafka.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;


@Service
public class StoreMessagesService {
    private boolean init = true;
    private final Logger logger = LoggerFactory.getLogger(StoreMessagesService.class);

    private static String DOCS_PATH_ROOT;

    private static String PATHLOG = "";
    private int msgNumber = 0;

    private PropertyUtil propertyUtil;

    public StoreMessagesService() {
        propertyUtil = new PropertyUtil();
        DOCS_PATH_ROOT = propertyUtil.getProperty("gzip.path.directory");
    }


    public void save(List<String> messages) throws IOException {
        logger.info("Transform and save the data into file");
        BufferedWriter bw;
        PATHLOG = DOCS_PATH_ROOT + "//PreStage//dataLog" + msgNumber + ".txt";
        try {
            if (init) {
                try {
                    FileUtil.createDirectory(DOCS_PATH_ROOT);
                    FileUtil.createDirectory(DOCS_PATH_ROOT + "//PreStage");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                bw = new BufferedWriter(new FileWriter(PATHLOG));
                init = false;
            } else {
                bw = new BufferedWriter(new FileWriter(PATHLOG, true));
            }
            for (String message : messages) {
                bw.write(message + "\n");
            }
            bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        FileUtil.addFilesToZipArchive();
        msgNumber++;
    }

}
