package com.feeyo.redis.config.loader.zk;

import com.feeyo.redis.engine.RedisEngineCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * redis proxy's config file (on zk server) listener
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class ConfigFileListener implements ZkDataListener {
    private static Logger LOGGER = LoggerFactory.getLogger( ConfigFileListener.class );

    private final String locFilePath;
    private boolean autoActivation;

    public ConfigFileListener(String locFilePath, boolean autoActivation) {
        this.locFilePath = locFilePath;
        this.autoActivation = autoActivation;
    }

    @Override
    public void zkDataChanged(byte[] data) {
        LOGGER.info("zk data changed. file: {} autoActivation: {}",new Object[] {locFilePath, autoActivation});
        try {
            File tmpFile = new File(locFilePath+".tmp");

            FileWriter fw = new FileWriter(tmpFile);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(new String(data));
            bw.flush();
            bw.close();

            // backup the original configure file
            File targetFile =new File(locFilePath);
            File targetFileBak = new File(locFilePath+".bak");
            if (targetFileBak.exists()) {
                targetFileBak.delete();
            }
            targetFile.renameTo(targetFileBak);
            targetFile.delete();

            tmpFile.renameTo(targetFile);
            tmpFile.delete();

            if (autoActivation) {
                doAutoActivation();
            }

        } catch (IOException e) {
            LOGGER.error("",e);
        }
    }

    private void doAutoActivation() {
        String fileName = locFilePath.substring(locFilePath.lastIndexOf(File.separator)+1);
        switch (fileName) {
            case "user.xml":
                RedisEngineCtx.INSTANCE().reloadUser();
                break;
            case "pool.xml":
                RedisEngineCtx.INSTANCE().reloadPool();
                break;
            case "server.xml":
                RedisEngineCtx.INSTANCE().reloadServer();
                break;
            default:
                break;
        }
    }

    public void setAutoActivation(boolean autoActivation) {
        this.autoActivation = autoActivation;
    }
}
