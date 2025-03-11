package com.example.demo;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.demo.service.impl.AwardCalculationService;

class AwardCalculationServiceTest {

    private AwardCalculationService awardCalculationService;

    @BeforeEach
    void setUp() {
        awardCalculationService = new AwardCalculationService(null, "testDatabase");
    }

    @Test
    void testToSnakeCase() throws Exception {
        Method method = AwardCalculationService.class.getDeclaredMethod("toSnakeCase", String.class);
        method.setAccessible(true);
        System.out.printf("in the testToSnakeCaes function\n");
        assertEquals("prize_code", method.invoke(awardCalculationService, "PrizeCode"));
        assertEquals("prize_name", method.invoke(awardCalculationService, "PrizeName"));
        assertEquals("award_code", method.invoke(awardCalculationService, "AwardCode"));
        assertEquals("promotion_msg_id", method.invoke(awardCalculationService, "PromotionMsgId"));
        assertEquals("created_dtm", method.invoke(awardCalculationService, "CreatedDtm"));
        assertEquals("modified_by", method.invoke(awardCalculationService, "ModifiedBy"));
        assertEquals("is_max_prizes_per_trip", method.invoke(awardCalculationService, "IsMaxPrizesPerTrip"));
        assertEquals("_ods_replay_switch", method.invoke(awardCalculationService, "_ODS_replay_switch"));
        assertEquals("hub_promotion_id", method.invoke(awardCalculationService, "HUBPromotionID"));
        System.out.printf("the test case result: %s\n", method.invoke(awardCalculationService, "HUBPromotionID"));
    }
}
