from os import getenv, path
import json
import subprocess
from io import StringIO
import csv
import flask
from werkzeug.exceptions import BadRequest, NotFound
import mysql.connector
from sqlalchemy.pool import QueuePool
from humps import camelize
import numpy as np
import cv2
import netifaces
import requests

LIMIT = 20
NAZOTTE_LIMIT = 50

chair_search_condition = json.load(open("../fixture/chair_condition.json", "r"))
estate_search_condition = json.load(open("../fixture/estate_condition.json", "r"))

app = flask.Flask(__name__)

mysql_connection_env = {
    "host": getenv("MYSQL_HOST", "10.162.24.103"),
    "port": getenv("MYSQL_PORT", 3306),
    "user": getenv("MYSQL_USER", "isucon"),
    "password": getenv("MYSQL_PASS", "isucon"),
    "database": getenv("MYSQL_DBNAME", "isuumo"),
}

servers = set(['10.162.24.101', '10.162.24.102'])
servers = servers - set([netifaces.ifaddresses('ens5')[netifaces.AF_INET][0]['addr']])

cnxpool = QueuePool(lambda: mysql.connector.connect(**mysql_connection_env), pool_size=10)


class CachedResult:
    estates = []
    chairs = []

    @staticmethod
    def refresh_estates(propagation=True):
        rows = select_all("SELECT id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity FROM estate ORDER BY rent ASC, id ASC LIMIT %s", (LIMIT,))
        CachedResult.estates = camelize(rows)

        if propagation:
            for ip in servers:
                requests.post(f'http://{ip}/api/update_estates_cache')

    @staticmethod
    def refresh_chairs(propagation=True):
        rows = select_all("SELECT id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock FROM chair WHERE stock > 0 ORDER BY price ASC, id ASC LIMIT %s", (LIMIT,))
        CachedResult.chairs = camelize(rows)

        if propagation:
            for ip in servers:
                requests.post(f'http://{ip}/api/update_chairs_cache')


class Fixture:
    # estate
    def get_door_height_range_id(value):
        if value < 80:
            return 0
        if value < 110:
            return 1
        if value < 150:
            return 2
        return 3

    def get_door_width_range_id(value):
        if value < 80:
            return 0
        if value < 110:
            return 1
        if value < 150:
            return 2
        return 3

    def get_rent_range_id(value):
        if value < 50000:
            return 0
        if value < 100000:
            return 1
        if value < 150000:
            return 2
        return 3

    def get_height_range_id(value):
        if value < 80:
            return 0
        if value < 110:
            return 1
        if value < 150:
            return 2
        return 3

    def get_width_range_id(value):
        if value < 80:
            return 0
        if value < 110:
            return 1
        if value < 150:
            return 2
        return 3

    def get_depth_range_id(value):
        if value < 80:
            return 0
        if value < 110:
            return 1
        if value < 150:
            return 2
        return 3

    def get_price_range_id(value):
        if value < 3000:
            return 0
        if value < 6000:
            return 1
        if value < 9000:
            return 2
        if value < 12000:
            return 3
        if value < 15000:
            return 4
        return 5


@app.route("/api/update_estates_cache", methods=["POST"])
def update_estates_cache():
    CachedResult.refresh_estates(propagation=False)
    return {}


@app.route("/api/update_chairs_cache", methods=["POST"])
def update_chairs_cache():
    CachedResult.refresh_chairs(propagation=False)
    return {}


def select_all(query, *args, dictionary=True):
    cnx = cnxpool.connect()
    try:
        cur = cnx.cursor(dictionary=dictionary)
        cur.execute(query, *args)
        return cur.fetchall()
    finally:
        cnx.close()


def select_row(*args, **kwargs):
    rows = select_all(*args, **kwargs)
    return rows[0] if len(rows) > 0 else None


@app.route("/initialize", methods=["POST"])
def post_initialize():
    sql_dir = "../mysql/db"
    sql_files = [
        "0_Schema.sql",
        "1_DummyEstateData.sql",
        "2_DummyChairData.sql",
        "4_UpdateRangeId.sql",
        "9_AddIndex.sql",
    ]

    for sql_file in sql_files:
        command = f"mysql -h {mysql_connection_env['host']} -u {mysql_connection_env['user']} -p{mysql_connection_env['password']} -P {mysql_connection_env['port']} {mysql_connection_env['database']} < {path.join(sql_dir, sql_file)}"
        subprocess.run(["bash", "-c", command])

    CachedResult.refresh_estates()
    CachedResult.refresh_chairs()

    return {"language": "python"}


@app.route("/api/estate/low_priced", methods=["GET"])
def get_estate_low_priced():
    return {"estates": CachedResult.estates}


@app.route("/api/chair/low_priced", methods=["GET"])
def get_chair_low_priced():
    return {"chairs": CachedResult.chairs}


@app.route("/api/chair/search", methods=["GET"])
def get_chair_search():
    args = flask.request.args

    conditions = []
    params = []

    if args.get("priceRangeId"):
        if args.get("priceRangeId") not in ['0', '1', '2', '3', '4', '5']:
            raise BadRequest("priceRangeId invalid")

        conditions.append("price_range_id = %s")
        params.append(args.get("priceRangeId"))

    if args.get("heightRangeId"):
        if args.get("heightRangeId") not in ['0', '1', '2', '3']:
            raise BadRequest("heightRangeId invalid")

        conditions.append("height_range_id = %s")
        params.append(args.get("heightRangeId"))

    if args.get("widthRangeId"):
        if args.get("widthRangeId") not in ['0', '1', '2', '3']:
            raise BadRequest("widthRangeId invalid")

        conditions.append("width_range_id = %s")
        params.append(args.get("widthRangeId"))

    if args.get("depthRangeId"):
        if args.get("depthRangeId") not in ['0', '1', '2', '3']:
            raise BadRequest("depthRangeId invalid")

        conditions.append("depth_range_id = %s")
        params.append(args.get("depthRangeId"))

    if args.get("kind"):
        conditions.append("kind = %s")
        params.append(args.get("kind"))

    if args.get("color"):
        conditions.append("color = %s")
        params.append(args.get("color"))

    if args.get("features"):
        for feature_confition in args.get("features").split(","):
            conditions.append("features LIKE CONCAT('%', %s, '%')")
            params.append(feature_confition)

    if len(conditions) == 0:
        raise BadRequest("Search condition not found")

    conditions.append("stock > 0")

    try:
        page = int(args.get("page"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format page parameter")

    try:
        per_page = int(args.get("perPage"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format perPage parameter")

    search_condition = " AND ".join(conditions)

    query = f"SELECT COUNT(id) as count FROM chair WHERE {search_condition}"
    count = select_row(query, params)["count"]

    query = f"SELECT id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock FROM chair WHERE {search_condition} ORDER BY popularity DESC, id ASC LIMIT %s OFFSET %s"
    chairs = select_all(query, params + [per_page, per_page * page])

    return {"count": count, "chairs": camelize(chairs)}


@app.route("/api/chair/search/condition", methods=["GET"])
def get_chair_search_condition():
    return chair_search_condition


@app.route("/api/chair/<int:chair_id>", methods=["GET"])
def get_chair(chair_id):
    chair = select_row("SELECT id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock FROM chair WHERE id = %s", (chair_id,))
    if chair is None or chair["stock"] <= 0:
        raise NotFound()
    return camelize(chair)


@app.route("/api/chair/buy/<int:chair_id>", methods=["POST"])
def post_chair_buy(chair_id):
    cnx = cnxpool.connect()
    try:
        cnx.start_transaction()
        cur = cnx.cursor(dictionary=True)
        cur.execute("SELECT id FROM chair WHERE id = %s AND stock > 0 FOR UPDATE", (chair_id,))
        chair = cur.fetchone()
        if chair is None:
            raise NotFound()
        cur.execute("UPDATE chair SET stock = stock - 1 WHERE id = %s", (chair_id,))

        # Refresh cache before unlock by commit
        CachedResult.refresh_chairs()

        cnx.commit()
        return {"ok": True}
    except Exception as e:
        cnx.rollback()
        raise e
    finally:
        cnx.close()


@app.route("/api/estate/search", methods=["GET"])
def get_estate_search():
    args = flask.request.args

    conditions = []
    params = []

    if args.get("doorHeightRangeId"):
        if args.get("doorHeightRangeId") not in ['0', '1', '2', '3']:
            raise BadRequest("doorHeightRangeId invalid")

        conditions.append("door_height_range_id = %s")
        params.append(args.get("doorHeightRangeId"))

    if args.get("doorWidthRangeId"):
        if args.get("doorWidthRangeId") not in ['0', '1', '2', '3']:
            raise BadRequest("doorWidthRangeId invalid")

        conditions.append("door_width_range_id = %s")
        params.append(args.get("doorWidthRangeId"))

    if args.get("rentRangeId"):
        if args.get("rentRangeId") not in ['0', '1', '2', '3']:
            raise BadRequest("rentRangeId invalid")

        conditions.append("rent_range_id = %s")
        params.append(args.get("rentRangeId"))

    if args.get("features"):
        for feature_confition in args.get("features").split(","):
            conditions.append("features LIKE CONCAT('%', %s, '%')")
            params.append(feature_confition)

    if len(conditions) == 0:
        raise BadRequest("Search condition not found")

    try:
        page = int(args.get("page"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format page parameter")

    try:
        per_page = int(args.get("perPage"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format perPage parameter")

    search_condition = " AND ".join(conditions)

    query = f"SELECT COUNT(id) as count FROM estate WHERE {search_condition}"
    count = select_row(query, params)["count"]

    query = f"SELECT id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity FROM estate WHERE {search_condition} ORDER BY popularity DESC, id ASC LIMIT %s OFFSET %s"
    chairs = select_all(query, params + [per_page, per_page * page])

    return {"count": count, "estates": camelize(chairs)}


@app.route("/api/estate/search/condition", methods=["GET"])
def get_estate_search_condition():
    return estate_search_condition


@app.route("/api/estate/req_doc/<int:estate_id>", methods=["POST"])
def post_estate_req_doc(estate_id):
    estate = select_row("SELECT id FROM estate WHERE id = %s", (estate_id,))
    if estate is None:
        raise NotFound()
    return {"ok": True}


@app.route("/api/estate/nazotte", methods=["POST"])
def post_estate_nazotte():
    if "coordinates" not in flask.request.json:
        raise BadRequest()
    coordinates = flask.request.json["coordinates"]
    if len(coordinates) == 0:
        raise BadRequest()
    longitudes = [c["longitude"] for c in coordinates]
    latitudes = [c["latitude"] for c in coordinates]
    bounding_box = {
        "top_left_corner": {"longitude": min(longitudes), "latitude": min(latitudes)},
        "bottom_right_corner": {"longitude": max(longitudes), "latitude": max(latitudes)},
    }

    points = np.array(list(zip(latitudes, longitudes)), dtype='float32')
    convex = cv2.convexHull(points)

    cnx = cnxpool.connect()
    try:
        cur = cnx.cursor(dictionary=True)
        cur.execute(
            (
                "SELECT id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity FROM estate"
                " WHERE latitude <= %s AND latitude >= %s AND longitude <= %s AND longitude >= %s"
                " ORDER BY popularity DESC, id ASC"
            ),
            (
                bounding_box["bottom_right_corner"]["latitude"],
                bounding_box["top_left_corner"]["latitude"],
                bounding_box["bottom_right_corner"]["longitude"],
                bounding_box["top_left_corner"]["longitude"],
            ),
        )
        estates = cur.fetchall()
        estates_in_polygon = []
        for estate in estates:
            if cv2.pointPolygonTest(convex, (estate['latitude'], estate['longitude']), measureDist=False) > 0:
                estates_in_polygon.append(camelize(estate))
                if len(estates_in_polygon) >= NAZOTTE_LIMIT:
                    break
    finally:
        cnx.close()

    results = {}
    results["estates"] = estates_in_polygon
    results["count"] = len(results["estates"])
    return results


@app.route("/api/estate/<int:estate_id>", methods=["GET"])
def get_estate(estate_id):
    estate = select_row("SELECT id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity FROM estate WHERE id = %s", (estate_id,))
    if estate is None:
        raise NotFound()
    return camelize(estate)


@app.route("/api/recommended_estate/<int:chair_id>", methods=["GET"])
def get_recommended_estate(chair_id):
    chair = select_row("SELECT width, height, depth FROM chair WHERE id = %s", (chair_id,))
    if chair is None:
        raise BadRequest(f"Invalid format searchRecommendedEstateWithChair id : {chair_id}")
    w, h, d = chair["width"], chair["height"], chair["depth"]
    query = (
        "SELECT id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity FROM estate"
        " WHERE (door_width >= %s AND door_height >= %s)"
        "    OR (door_width >= %s AND door_height >= %s)"
        "    OR (door_width >= %s AND door_height >= %s)"
        "    OR (door_width >= %s AND door_height >= %s)"
        "    OR (door_width >= %s AND door_height >= %s)"
        "    OR (door_width >= %s AND door_height >= %s)"
        " ORDER BY popularity DESC, id ASC"
        " LIMIT %s"
    )
    estates = select_all(query, (w, h, w, d, h, w, h, d, d, w, d, h, LIMIT))
    return {"estates": camelize(estates)}


@app.route("/api/chair", methods=["POST"])
def post_chair():
    if "chairs" not in flask.request.files:
        raise BadRequest()
    records = csv.reader(StringIO(flask.request.files["chairs"].read().decode()))
    cnx = cnxpool.connect()
    try:
        cnx.start_transaction()
        cur = cnx.cursor()
        for record in records:
            record.append(Fixture.get_height_range_id(int(record[5])))
            record.append(Fixture.get_width_range_id(int(record[6])))
            record.append(Fixture.get_depth_range_id(int(record[7])))
            record.append(Fixture.get_price_range_id(int(record[4])))

            query = "INSERT INTO chair(id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock, height_range_id, width_range_id, depth_range_id, price_range_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(query, record)

        # Refresh cache before unlock by commit
        CachedResult.refresh_chairs()

        cnx.commit()
        return {"ok": True}, 201
    except Exception as e:
        cnx.rollback()
        raise e
    finally:
        cnx.close()


@app.route("/api/estate", methods=["POST"])
def post_estate():
    if "estates" not in flask.request.files:
        raise BadRequest()
    records = csv.reader(StringIO(flask.request.files["estates"].read().decode()))
    cnx = cnxpool.connect()
    try:
        cnx.start_transaction()
        cur = cnx.cursor()
        for record in records:
            record.append(Fixture.get_door_height_range_id(int(record[8])))
            record.append(Fixture.get_door_width_range_id(int(record[9])))
            record.append(Fixture.get_rent_range_id(int(record[7])))

            query = "INSERT INTO estate(id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity, door_height_range_id, door_width_range_id, rent_range_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(query, record)

        # Refresh cache before unlock by commit
        CachedResult.refresh_estates()

        cnx.commit()
        return {"ok": True}, 201
    except Exception as e:
        cnx.rollback()
        raise e
    finally:
        cnx.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=getenv("SERVER_PORT", 1323), debug=True, threaded=True)
