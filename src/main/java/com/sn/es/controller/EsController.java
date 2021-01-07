package com.sn.es.controller;

import com.sn.es.config.EsClientConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

/**
 * Created by nings on 2020/12/11.
 */
@Controller
@ResponseBody
@Slf4j
@Api(value = "es常用接口",description = "es常用接口")
public class EsController {


    @GetMapping(value = "checkIndex/{index}")
    @ApiOperation(value = "检测index索引是否存在")
    public String checkIndexExist(@PathVariable(value = "index",required = true) String index, HttpServletRequest request, HttpServletResponse response){
        boolean flag = EsClientConfig.checkIndexExist(index);
        if (flag){
            return "{code:0,msg:"+index+"存在}";
        }
        return "{code:1,msg:"+index+"不存在}";
    }


    @GetMapping(value = "createIndex/{index}")
    @ApiOperation(value = "创建index索引")
    public String createIndex(@PathVariable(value = "index") String index, HttpServletRequest request, HttpServletResponse response){
        boolean flag = EsClientConfig.createIndex(index);
        if (flag){
          return "{code:0,msg:"+index+"索引创建成功}";
        }
        return "{code:1,msg:"+index+"索引创建失败}";
    }


    @GetMapping(value= "addTextContent/{index}/{type}")
    @ApiOperation(value = "批量插入数据")
    public String addTextContent(@PathVariable(value = "index") String index,@PathVariable(value = "type") String type,
                                 HttpServletRequest request, HttpServletResponse response){

        // 模拟数据库获取数据
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        Map<String,Object> map1 = new HashMap<String,Object>();
        map1.put("userid","001");
        map1.put("name","tom");
        Map<String,Object> map2 = new HashMap<String,Object>();
        map2.put("userid","002");
        map2.put("name","jack");
        Map<String,Object> map3 = new HashMap<String,Object>();
        map3.put("userid","003");
        map3.put("name","james");
        Collections.addAll(list,map1,map2,map3);

        boolean flag = EsClientConfig.addTextContent(index,type,list);
        if (flag){
            return "{code:0,msg:"+index+"文档插入成功}";
        }
        return "{code:1,msg:"+index+"文档插入失败}";
    }

    // es 7.0以后，就移除这个type了
    //@GetMapping(value = "getTextContntList/{index}/{type}/{start}/{rows}")
    @GetMapping(value = "getTextContntList/{index}/{start}/{rows}")
    @ApiOperation(value = "批量获取数据")
    public String getTextContentList(@PathVariable(value = "index") String index,
                                     @PathVariable(value = "type",required = false) String type,
                                     @PathVariable(value = "start") Integer start,
                                     @PathVariable(value = "rows") Integer rows){
        List<Map<String, Object>> list = EsClientConfig.searchAll(index, type,start, rows);
        log.info("查询数据：{}",list);
        return "{code:1,msg:"+list+"}";
    }









}
