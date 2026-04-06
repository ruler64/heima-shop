package com.hmall.trade.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmall.trade.domain.po.LocalEventOutbox;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface LocalEventOutboxMapper extends BaseMapper<LocalEventOutbox> {

    /**
     * 高效更新状态，供 afterCommit 或 XXL-JOB 投递成功后调用
     */
    @Update("UPDATE local_event_outbox SET status = #{status}, update_time = NOW() WHERE id = #{id}")
    int updateStatus(@Param("id") Long id, @Param("status") Integer status);
}