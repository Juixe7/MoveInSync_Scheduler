package com.movesync.scheduler.handler;

import com.movesync.scheduler.domain.model.JobExecution;
import com.movesync.scheduler.domain.model.JobType;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class BillingCloseHandler implements JobHandler {

    private final JobHandlerRegistry registry;

    @PostConstruct
    void init() {
        registry.register(JobType.BILLING_CLOSE, this);
    }

    @Override
    public void handle(JobExecution execution) throws Exception {
        log.info("Processing billing close for job={} tenant={}",
            execution.getJobId(), execution.getTenantId());
    }
}

