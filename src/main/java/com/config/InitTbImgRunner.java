package com.config;

import com.common.db.BizDB;
import com.common.db.IDbOp;
import com.common.util.IDGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author wul
 * @Description 初始化TB_IMG表
 * @Date 2019/12/12 15:30
 */
@Component
@Slf4j
public class InitTbImgRunner implements ApplicationRunner {

    public static final String TABLE = "TB_IMG";

    @Autowired
    BizDB bizDb;

    @Value("${path.imgPath}")
    String imgPath;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        File file = new File(imgPath);
        final File[] files = file.listFiles();
        final Connection conn = bizDb.getConn(false);
        try {
            IDbOp base = bizDb.getOptimizeDbOp();
            base.delete(conn, TABLE, null, null);
            for(File file1 : files){
                if(file1.isDirectory()){
                    String directory = file1.getName();
                    final File[] files1 = file1.listFiles();
                    for(File file2 : files1){
                        String filePath = file2.getPath();
                        if(filePath.endsWith(".jpg") || filePath.endsWith(".JPG") || filePath.endsWith(".png") || filePath.endsWith(".JPG")){
                            int startIndex = filePath.indexOf(imgPath) + imgPath.length();
                            String path = file2.getPath().substring(startIndex, filePath.length());
                            Map<String, Object> dataMap = new HashMap<>();
                            dataMap.put("IMG_ID", IDGenerator.getId());
                            dataMap.put("IMG_DES", directory);
                            dataMap.put("IMG_PATH", "showImg"+path);
                            base.insert(conn, TABLE, dataMap);
                            conn.commit();
                        }
                    }
                }
            }
        }catch (Exception e){
            conn.rollback();
            log.error("", e);
        }finally {
            bizDb.closeConn(conn);
        }
    }


}
