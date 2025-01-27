package com.file.batch.kafka.service;

import com.file.batch.kafka.util.FileUtil;
import com.file.batch.kafka.util.PropertyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes= {StoreMessagesService.class})
class StoreMessagesServiceTest {

    @InjectMocks
    private PropertyUtil propertyUtil;

    @InjectMocks
    private FileUtil fileUtil;

    @Autowired
    private StoreMessagesService storeMessagesService;

    @Test
    void testSave() throws Exception {
        assertEquals(propertyUtil.getProperty("gzip.path.directory"),"c://tmp//gzip");
        assertNotNull(fileUtil);
        assertEquals(fileUtil.PRESTAGE_WORKSPACE_DIRECTORY,"c://tmp//gzip//PreStage//");
        storeMessagesService.save(Arrays.asList("message1", "message2"));
        fileUtil.createDirectory("c://tmp//gzip");
        fileUtil.addFilesToZipArchive();
    }
}