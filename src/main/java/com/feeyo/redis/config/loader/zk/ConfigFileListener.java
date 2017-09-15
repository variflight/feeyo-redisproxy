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
    private final String backupPath;
    private boolean autoActivation;

    public ConfigFileListener(String locFilePath, String backupPath, boolean autoActivation) {
        this.locFilePath = locFilePath;
        this.backupPath = backupPath;
        this.autoActivation = autoActivation;
    }

    @Override
    public void zkDataChanged(byte[] data) {
        LOGGER.info("zk data changed. file: {} autoActivation: {}",new Object[] {locFilePath, autoActivation});

        ZkClient.saveAndBackupCfgData(data, locFilePath, backupPath);

        if (autoActivation) {
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
    }

    public void setAutoActivation(boolean autoActivation) {
        this.autoActivation = autoActivation;
    }
}
