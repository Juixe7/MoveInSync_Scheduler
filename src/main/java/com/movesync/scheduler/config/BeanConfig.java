package com.movesync.scheduler.config;

import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.domain.service.PartitionAssigner;
import com.movesync.scheduler.domain.service.RetryCalculator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanConfig {

    @Bean
    public CronResolver cronResolver() {
        return new CronResolver();
    }

    @Bean
    public PartitionAssigner partitionAssigner(SchedulerProperties props) {
        return new PartitionAssigner(props.getPartitionCount());
    }

    @Bean
    public RetryCalculator retryCalculator(SchedulerProperties props) {
        return new RetryCalculator(
            props.getInitialBackoffMs(),
            props.getMaxBackoffMs(),
            props.getBackoffMultiplier(),
            props.getJitterFactor()
        );
    }
}

