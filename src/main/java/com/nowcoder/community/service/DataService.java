package com.nowcoder.community.service;

import com.nowcoder.community.util.RedisKeyUtil;
import org.elasticsearch.common.recycler.Recycler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.w3c.dom.stylesheets.LinkStyle;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * DataService
 *
 * @author Jimmy
 * @date 2023/5/23 16:03
 * @description: 使用HyperLogLog 和 Bitmap进行数据统计的业务层
 * 因为是使用redis，所以不需要单独开发一个数据访问层
 * 本service做的事情：
 *      提供记录数据的方法、提供查询（包括区间查询）的方法
 *
 * （至于什么时候拦截数据记录、什么时候查询，交由表现层去做）
 */
@Service
public class DataService {

    @Autowired
    private RedisTemplate redisTemplate;

    private SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

    // 将指定IP计入UV
    public void recordUV(String ip) {
        // 先拼一个redis的key
        String redisKey = RedisKeyUtil.getUVKey(df.format(new Date()));
        // 记录进去就是了
        redisTemplate.opsForHyperLogLog().add(redisKey, ip);
    }

    // 统计指定日期范围内的UV
    public long calculateUV(Date start, Date end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("参数不能为空");
        }
        if (start.after(end)) {
            throw new IllegalArgumentException("请填入正确的时间段");
        }
        // 整理该日期范围内的key
        List<String> keyList = new ArrayList<>();
        Calendar calendar = Calendar.getInstance(); // 实例化一个Calendar对象用于处理日期
        calendar.setTime(start);
        while (!calendar.getTime().after(end)) {    // 当日期不晚于end时，进行循环
            String key = RedisKeyUtil.getUVKey(df.format(calendar.getTime()));   // 拼出每一天的UV Key
            keyList.add(key);
            calendar.add(Calendar.DATE, 1); // 每次循环加一天，也就是按天遍历
        }

        // 合并这些数据
        String redisKey = RedisKeyUtil.getUVKey(df.format(start), df.format(end));
        redisTemplate.opsForHyperLogLog().union(redisKey, keyList.toArray());

        // 返回统计的结果
        return redisTemplate.opsForHyperLogLog().size(redisKey);
    }

    // 将指定用户计入DAU
    public void recordDAU(int userId) {
        String redisKey = RedisKeyUtil.getDAUKey(df.format(new Date()));
        redisTemplate.opsForValue().setBit(redisKey, userId, true);
    }

    // 统计指定日期范围内的DAU
    // 这里如果是统计7天的DAU，不是做合并，而是做OR运算，7天之内的任意一天只要登录了一次，就算活跃用户
    public long calculateDAU(Date start, Date end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("参数不能为空");
        }
        if (start.after(end)) {
            throw new IllegalArgumentException("请填入正确的时间段");
        }
        // 整理该日期范围内的key
        List<byte[]> keyList = new ArrayList<>();
        Calendar calendar = Calendar.getInstance(); // 实例化一个Calendar对象用于处理日期
        calendar.setTime(start);
        while (!calendar.getTime().after(end)) {    // 当日期不晚于end时，进行循环
            String key = RedisKeyUtil.getDAUKey(df.format(calendar.getTime()));   // 拼出每一天的UV Key
            keyList.add(key.getBytes());
            calendar.add(Calendar.DATE, 1); // 每次循环加一天，也就是按天遍历
        }
        // 进行OR运算，不能直接用String，而应该用redis底层的连接进行OIR运算
        return (long) redisTemplate.execute(new RedisCallback() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                String redisKey = RedisKeyUtil.getDAUKey(df.format(start), df.format(end));
                connection.bitOp(RedisStringCommands.BitOperation.OR,
                        redisKey.getBytes(), keyList.toArray(new byte[0][0]));
                return connection.bitCount(redisKey.getBytes());
            }
        });
    }
}
