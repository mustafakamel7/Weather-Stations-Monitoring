package com.weather;

public class WeatherStatus {
    public long station_id;
    public long s_no;
    public String battery_status;
    public long status_timestamp;
    public Weather weather;

    public WeatherStatus(long station_id, long s_no,
                         String battery_status, Weather weather) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = System.currentTimeMillis() / 1000L;
        this.weather = weather;
    }
}
