package com.controller;

import com.common.db.BizDB;
import com.config.InitTbImgRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/12 11:45
 */
@Controller
@Slf4j
public class ImgController {

    @Autowired
    BizDB bizDb;

    @GetMapping("/showImg")
    public String showImg(){
        return "img";
    }

    @GetMapping("/listImg")
    @ResponseBody
    public List<Map<String, Object>> showImg(@RequestParam(value = "pageNo", defaultValue = "1") int pageNo, Model model){
        int pageSize = 9;
        log.info("当前第{}页", pageNo);
        int startIndex = (pageNo - 1) * pageSize;
        List<String> fields = new ArrayList<>();
        fields.add("IMG_PATH");
        List<Map<String, Object>> maps = new ArrayList<>();
        try {
            maps = bizDb.searchAsMapList(InitTbImgRunner.TABLE, fields, null, null, startIndex, pageSize, null, false);
        } catch (Exception e) {
            log.error("", e);
        }
        return maps;
    }

    @GetMapping("/getMaxPage")
    @ResponseBody
    public int getTotalPage(){
        try {
            final int count = bizDb.getCount(InitTbImgRunner.TABLE);
            if(0 != count){
                return count/9;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

}
