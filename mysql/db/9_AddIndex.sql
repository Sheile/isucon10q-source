ALTER TABLE isuumo.estate ADD INDEX index_estate_rent(rent);
ALTER TABLE isuumo.estate ADD INDEX index_estate_popularity(popularity);
ALTER TABLE isuumo.estate ADD INDEX index_estate_latitude_longitude(latitude, longitude);
ALTER TABLE isuumo.estate ADD INDEX index_estate_door_height_range_id(door_height_range_id);
ALTER TABLE isuumo.estate ADD INDEX index_estate_door_width_range_id(door_width_range_id);
ALTER TABLE isuumo.estate ADD INDEX index_estate_rent_range_id(rent_range_id);

ALTER TABLE isuumo.chair ADD INDEX index_chair_stock(stock);
ALTER TABLE isuumo.chair ADD INDEX index_chair_price(price);
ALTER TABLE isuumo.chair ADD INDEX index_estate_height_range_id(height_range_id);
ALTER TABLE isuumo.chair ADD INDEX index_estate_width_range_id(width_range_id);
ALTER TABLE isuumo.chair ADD INDEX index_estate_depth_range_id(depth_range_id);
ALTER TABLE isuumo.chair ADD INDEX index_estate_price_range_id(price_range_id);
