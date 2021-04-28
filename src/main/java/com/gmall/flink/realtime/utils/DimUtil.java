package com.gmall.flink.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;


/**
 * Desc: 用于维度查询工具类  底层调用的是PhoenixUtil
 * select * from DIM_BASE_TRADEMARK where id=10 and name=zs;
 **/
public class DimUtil {

    //从Phoenix中查询数据,没有使用缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        String whereSql = " where ";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> stringStringTuple2 = cloNameAndValue[i];
            String filedName = stringStringTuple2.f0;
            String filedVaule = stringStringTuple2.f1;
            if (i > 0) {
                whereSql += " and ";
            }
            whereSql += filedName + "='" + filedVaule + "'";
        }

        String sql = "select * from " + tableName + whereSql;
        System.out.println("查询维度的SQL:" + sql);
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject dimJsonObj = null;
        //对于维度查询来讲,一般都是根据主键进行查询,不可能返回多条记录,只会有一条
        if (dimList != null && dimList.size() > 0) {
            dimJsonObj = dimList.get(0);
        } else {
            System.out.println("维度数据没有找到" + sql);
        }
        return dimJsonObj;
    }

    //在做维度关联的时候,大部分场景都是通过id进行关联,所以提供一个方法,只需要id的值作为参数传进来即可
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    /*
    优化:从Phoenix中查询数据,加入了旁路缓存
    先从缓存查询,如果缓存没有查到数据,再到Phoenix查询,并将查询结果放到缓存中
    Redis:
        类型:   String   list   set  zset  hash
        key:    dim:表名:值    例如:dim:DIM_BASE_TRADEMARK:10_xxx
        value:  通过Phoenix到维度表中查询数据,取出一条并将其转换为json字符串
        失效时间:  24*3600
     */
    @SafeVarargs
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        String whereSql = " where ";
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> stringStringTuple2 = cloNameAndValue[i];
            String filedName = stringStringTuple2.f0;
            String filedVaule = stringStringTuple2.f1;
            if (i > 0) {
                whereSql += " and ";
                redisKey += "_";
            }
            whereSql += filedName + "='" + filedVaule + "'";
            redisKey += filedVaule;
        }

        //从Redis中获取数据
        Jedis jedis = null;
        //维度数据的json字符串形式
        String dimJsonStr = null;
        //维度数据的json对象形式
        JSONObject dimJsonObj = null;
        try {
            //获取jedis客户端
            jedis = RedisUtil.getJedis();
            //根据key到Redis中查询
            dimJsonStr = jedis.get(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Redis中查询维度失败");
        }

        //判断是否从Redis中查询到了数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //如果在Redis中没有查到数据,需要到Phoenix中查询
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的SQL:" + sql);

            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度查询来讲,一般都是根据主键进行查询,不可能返回多条记录,只会有一条
            if (dimList != null && dimList.size() > 0) {
                dimJsonObj = dimList.get(0);
                //将查询出来的数据放到Redis中缓存起来
                if (jedis != null) {
                    jedis.setex(redisKey, 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("维度数据没有找到" + sql);
            }
        }

        //关闭jedis
        if (jedis != null) {
            jedis.close();
        }
        return dimJsonObj;
    }

    //根据 key 让 Redis 中的缓存失效
    public static void deleteCached(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过 key 清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        JSONObject dimInfo = DimUtil.getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "14"));
//        System.out.println(dimInfo);

        JSONObject dimInfo = getDimInfo("DIM_BASE_TRADEMARK", "12");
        System.out.println(dimInfo);
    }
}
