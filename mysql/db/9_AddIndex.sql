ALTER TABLE isuumo.estate ADD INDEX index_estate_rent(rent);
ALTER TABLE isuumo.estate ADD INDEX index_estate_latitude_longitude(latitude, longitude, sort_key);
ALTER TABLE isuumo.estate ADD INDEX index_estate_door_height_range_id(door_height_range_id, sort_key);
ALTER TABLE isuumo.estate ADD INDEX index_estate_door_width_range_id(door_width_range_id, sort_key);
ALTER TABLE isuumo.estate ADD INDEX index_estate_rent_range_id(rent_range_id, sort_key);
ALTER TABLE isuumo.estate ADD INDEX index_estate_sort_key(sort_key);

ALTER TABLE isuumo.chair ADD INDEX index_chair_stock(stock, sort_key);
ALTER TABLE isuumo.chair ADD INDEX index_chair_price(price);
ALTER TABLE isuumo.chair ADD INDEX index_estate_height_range_id(height_range_id, sort_key);
ALTER TABLE isuumo.chair ADD INDEX index_estate_width_range_id(width_range_id, sort_key);
ALTER TABLE isuumo.chair ADD INDEX index_estate_depth_range_id(depth_range_id, sort_key);
ALTER TABLE isuumo.chair ADD INDEX index_estate_price_range_id(price_range_id, sort_key);
ALTER TABLE isuumo.chair ADD INDEX index_chair_sort_key(sort_key);
