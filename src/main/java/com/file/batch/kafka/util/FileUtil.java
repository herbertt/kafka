package com.file.batch.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
    private  static final String PRESTAGE_WORKSPACE_DIRECTORY = new PropertyUtil().getProperty("gzip.path.directory") + File.separator + "PreStage" + File.separator;
    private  static final String WORKSPACE_DIRECTORY = new PropertyUtil().getProperty("gzip.path.directory") + File.separator;
    private static final String DEFAULT_FILE_EXT = ".txt";
    private PropertyUtil prop;

    public FileUtil() throws Exception {
        prop = new PropertyUtil();
        createDirectory(prop.getProperty("gzip.path.directory"));
        createDirectory(prop.getProperty("gzip.path.directory") + "//PreStage");
    }
    public static void createDirectory(String path) throws Exception {
        File directory = new File(path);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new Exception("Directory not create in: " + path);
            }
        }
    }
    public static void excludeFilesPreStage(String nmFile) {

        try {
            String dirPreStages = PRESTAGE_WORKSPACE_DIRECTORY + File.separator + nmFile + File.separator;
            File dir = new File(dirPreStages);
            if (dir.exists() && dir.isDirectory()) {
                for (File file : dir.listFiles()) {
                    if (!file.isDirectory())
                        file.delete();
                }
                dir.delete();
            }
        } catch (Exception e) {
            logger.error("Directory not removed: ", e);
        }

    }

    public static void moveFile(File file, String origDir, String destDir) {

        try {
            if (!(new File(origDir + destDir)).exists()) {
                File temp = new File(origDir + destDir);
                temp.mkdir();
            }

            File dest = new File(origDir + destDir + File.separator + file.getName());
            boolean sucess = false;
            boolean removed = false;

            if (dest.exists()) {
                dest.delete();
                dest = new File(origDir + destDir + File.separator + file.getName());
            }

            sucess = dest.createNewFile();
            copyFileUsingChannel(file, dest);

            if (sucess) {
                logger.info("Sucess to create file " + dest.getAbsolutePath());
                removed = file.delete();
            } else {
                logger.error("Erro to create file" + dest.getAbsolutePath());
            }

            if (removed) {
                logger.info("Sucess to remove file " + file.getAbsolutePath());
            } else {
                logger.error("Erro to remove file  " + file.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.error("Fail to move file: ", e);
        }
    }
    private static void copyFileUsingChannel(File source, File dest) throws IOException {
        FileChannel sourceChannel = null;
        FileChannel destChannel = null;
        try {
            sourceChannel = new FileInputStream(source).getChannel();
            destChannel = new FileOutputStream(dest).getChannel();
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        } finally {
            sourceChannel.close();
            destChannel.close();
        }
    }
    public static String getPreStageFilePath(String nmArquivo, String preStageName) throws Exception {

        String path = PRESTAGE_WORKSPACE_DIRECTORY + File.separator + nmArquivo + File.separator;
        createDirectory(path);
        return path + preStageName + DEFAULT_FILE_EXT;
    }

    public static void delFilesPreStage(String nmFile) {

        try {
            String dirPreStages = PRESTAGE_WORKSPACE_DIRECTORY + File.separator + nmFile + File.separator;
            File dir = new File(dirPreStages);
            if (dir.exists() && dir.isDirectory()) {
                for (File file : dir.listFiles()) {
                    if (!file.isDirectory())
                        file.delete();
                }
                dir.delete();
            }
        } catch (Exception e) {
            logger.error("Files can not be removed: ", e);
        }

    }

    public static void excludeAllFilesPreStage(String path, boolean removeDir) {

        try {
            File dir = new File(path);
            if (dir.exists() && dir.isDirectory()) {
                for (File file : dir.listFiles()) {
                    if (!file.isDirectory()) {
                        file.delete();
                    } else {
                        excludeAllFilesPreStage(file.getPath(), true);
                    }
                }
                if (removeDir) {
                    dir.delete();
                }
            }
        } catch (Exception e) {
            logger.error("Files can not be removed: ", e);
        }

    }
    public static List<String> getAllFilesPreStage(String path) {
        List<String> filesNames = new ArrayList<>();
        try {
            File dir = new File(path);
            if (dir.exists() && dir.isDirectory()) {
                for (File file : dir.listFiles()) {
                    filesNames.add(file.getName());
                }
            }
        } catch (Exception e) {
            logger.error("Directory is empty: ", e);
        }
        return filesNames;
    }

    public static void createFolderPreStageIfNull() {
        try {
            File file = new File(PRESTAGE_WORKSPACE_DIRECTORY);
            if (!file.exists()) {
                createDirectory(file.getPath());
            }
        } catch (Exception e) {
            logger.error("PreStage not created ", e);
        }
    }

    public static void addFilesToZipArchive() throws IOException{
        createFolderPreStageIfNull();
        final List<String> srcFiles = getAllFilesPreStage(PRESTAGE_WORKSPACE_DIRECTORY);

        final FileOutputStream fos = new FileOutputStream(WORKSPACE_DIRECTORY + "compressed.zip");
        ZipOutputStream zipOut = new ZipOutputStream(fos);

        for (String srcFile : srcFiles) {
            File fileToZip = new File(srcFile);
            FileInputStream fis = new FileInputStream(PRESTAGE_WORKSPACE_DIRECTORY+ File.separator +fileToZip);
            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zipOut.putNextEntry(zipEntry);

            byte[] bytes = new byte[1024];
            int length;
            while((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            fis.close();
        }

        zipOut.close();
        fos.close();
    }

}
