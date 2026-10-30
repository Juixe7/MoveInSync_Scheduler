package com.movesync.scheduler.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class SeedTokenPrinter implements CommandLineRunner {

    private final JwtTokenProvider jwtTokenProvider;

    public static final UUID TENANT_A_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    public static final UUID TENANT_B_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Override
    public void run(String... args) {
        String adminToken = jwtTokenProvider.generateToken("admin_a", TENANT_A_ID, "ADMIN");
        String operatorToken = jwtTokenProvider.generateToken("operator_a", TENANT_A_ID, "OPERATOR");
        String employeeToken = jwtTokenProvider.generateToken("employee_a", TENANT_A_ID, "EMPLOYEE");
        String tenantBToken = jwtTokenProvider.generateToken("admin_b", TENANT_B_ID, "ADMIN");

        log.info("\n" +
            "=========================================================================================\n" +
            "                  TEST JWT TOKENS FOR DEMO / EVALUATION (SWAGGER & CURL)                 \n" +
            "=========================================================================================\n" +
            " Tenant A ID : {}\n" +
            "   [ADMIN]    : Bearer {}\n" +
            "   [OPERATOR] : Bearer {}\n" +
            "   [EMPLOYEE] : Bearer {}\n" +
            "-----------------------------------------------------------------------------------------\n" +
            " Tenant B ID : {}\n" +
            "   [ADMIN]    : Bearer {}\n" +
            "=========================================================================================\n" +
            " Swagger UI Documentation available at: http://localhost:8080/swagger-ui.html\n" +
            "=========================================================================================",
            TENANT_A_ID, adminToken, operatorToken, employeeToken, TENANT_B_ID, tenantBToken);
    }
}

