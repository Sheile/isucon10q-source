ALTER TABLE isuumo.estate ADD INDEX index_estate_rent(rent);
ALTER TABLE isuumo.estate ADD INDEX index_estate_popularity(popularity);

ALTER TABLE isuumo.chair ADD INDEX index_chair_stock(stock);
ALTER TABLE isuumo.chair ADD INDEX index_chair_price(price);
