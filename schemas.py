weather_enriched_schema = ["startDate","cloud","pressure","humidity","percip","show_depth","temp","dewpoint","visib","wind_dir","gust_speed","wind_speed","measurmentId","counterId","seqNumber","endDate","visits","counterIdPave","installationDate","parkCode","cordNorth","coordSouth"]
weather_enriched_schema_holidays = weather_enriched_schema[:]
weather_enriched_schema_holidays.append("holiday")