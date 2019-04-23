package com.puming.equipment;

import lombok.Data;

/**
 * @author pengcheng
 * @version V1.0
 * @description
 * @date 2019/04/02 20:35
 */
@Data
public class Entity {

    /**
     * stats : {"count":4510,"min":0,"max":999,"avg":503.4911308203991,"sum":2270745}
     */

    private StatsBean stats;
    @Data
    public static class StatsBean {
        /**
         * count : 4510
         * min : 0.0
         * max : 999.0
         * avg : 503.4911308203991
         * sum : 2270745.0
         */

        private int count;
        private double min;
        private double max;
        private double avg;
        private double sum;
    }
}
