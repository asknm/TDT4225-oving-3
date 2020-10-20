from MyDataReader import MyDataReader
from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit


class Program:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(50) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id BIGINT UNSIGNED NOT NULL,
                user_id VARCHAR(50) NOT NULL references user(id),
                transportation_mode VARCHAR(20),
                start_date_time DATETIME,
                end_date_time DATETIME,
                PRIMARY KEY (id, user_id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id BIGINT UNSIGNED NOT NULL references activity(id),
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_user_data(self, data):
        query = "INSERT INTO user VALUES (%(id)s, %(has_labels)s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def insert_activity_data(self, data):
        query = """INSERT INTO activity VALUES (%(id)s, %(user_id)s, %(transportation_mode)s, 
                %(start_date_time)s, %(end_date_time)s)
                """
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def insert_trackpoint_data(self, data):
        query = """INSERT INTO trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%(activity_id)s, %(lat)s, %(lon)s,
                %(altitude)s, %(date_days)s, %(date_time)s)
        """
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE IF EXISTS %s "
        self.cursor.execute(query % table_name)
        query = """ALTER TABLE %s
                        ADD """
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()

    ### task 2
    # 1
    def count_all(self, table_name):
        query = """ SELECT COUNT(*) FROM %s;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Count of " % table_name)
        print(count)

    # 2
    def agerage_activities(self):
        query = """ SELECT (SELECT COUNT(*) FROM activity)/(SELECT COUNT(*) FROM user);"""
        # query = """ SELECT COUNT(activity.id)/COUNT(user.id) FROM activity, user;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Average activities for each user:")
        print(count)

    # 3
    def top_twenty_users(self):
        query = """SELECT user_id FROM activity GROUP BY user_id ORDER BY COUNT(*) DESC limit 20;"""
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Top twenty users:")
        print(count)

    # 4
    def taxi_users(self):
        query = """SELECT user.id FROM activity WHERE transportation_mode = 'taxi' GROUP BY user.id;"""  # hvis id finnes i labeled_ids.txt, hvordan er dette lagt inn i tabellen?
        self.cursor.execute(query)
        count = self.cursor.fetchall()
        print("Top taxi users:")
        print(count)

    # 5
    def all_transportations(self):
        query = """
        SELECT transportation_mode, count(transportation_mode)
        FROM activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Transportation modes and counts:")
        print(res)

    # 6a)
    def most_active_year_by_activity_count(self):
        query = """SELECT YEAR(start_date_time) as year
                FROM activity
                GROUP BY year
                ORDER BY COUNT(id) DESC
                LIMIT 1;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Most active year by activity count:")
        print(res)

    # 6b)
    def most_active_year_by_hours(self):
        query = """SELECT YEAR(start_date_time) as year, SUM(HOUR(TIMEDIFF(start_date_time, end_date_time))) AS hours
                FROM activity
                GROUP BY year
                ORDER BY hours DESC;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Most active year by hours logged:")
        print(res)

    # 7
    def user_112_distance_walked_2008(self):
        query = """
        SELECT t.activity_id, t.lat, t.lon, t.altitude
        FROM trackpoint AS t
        INNER JOIN activity AS a ON t.activity_id=a.id
        WHERE YEAR(a.start_date_time) = 2008
        AND a.user_id = '112'
        AND a.transportation_mode = 'walk'
        ORDER BY t.activity_id;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        distance = 0.0
        for i, l in enumerate(res):
            if i > 0 and l[0] == res[i - 1][0]:
                distance += haversine((l[1], l[2]), (res[i - 1][1], res[i - 1][2]))
        print("Total distance:")
        print(distance)

    # 8
    def top_20_altitude(self):
        query = """SET @last_altitude = 99999"""
        self.cursor.execute(query)
        query = """SET @last_activity_id = -1"""
        self.cursor.execute(query)
        query = """
        SELECT *
        FROM (
            SELECT a.user_id, SUM(t.altitude_gain) AS total_gain
            FROM (
                SELECT
                activity_id,
                IF(altitude > @last_altitude, altitude-@last_altitude, 0.0) AS altitude_gain,
                activity_id = @last_activity_id as same_activity,
                @last_altitude := altitude,
                @last_activity_id := activity_id
                FROM trackpoint
                WHERE altitude != -777
                AND @last_altitude != -777
            ) AS t
            INNER JOIN activity AS a ON t.activity_id = a.id
            WHERE t.same_activity
            GROUP BY a.user_id
        ) AS u
        ORDER BY u.total_gain DESC
        LIMIT 20
        ;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Top 20 altitude gainers:")
        print(res)

    # 9
    def find_invalid_activities(self):
        query = """SELECT user_id, COUNT(activity_id)
                FROM (activity as a JOIN 
                (SELECT DISTINCT tp1.activity_id
                FROM trackpoint AS tp1 JOIN trackpoint AS tp2 ON tp1.activity_id = tp2.activity_id 
                WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) >= 5 AND (tp1.id + 1) = tp2.id)
                AS invalids ON a.id = invalids.activity_id)
                GROUP BY a.user_id
                ORDER BY COUNT(activity_id) DESC;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Users with invalid activities:")
        print(res)

    # 10
    def activities_in_forbidden_city(self):
        query = """SELECT DISTINCT user.id 
                FROM user 
                INNER JOIN activity on activity.user_id=user.id 
                INNER JOIN trackpoint on trackpoint.activity_id=activity.id
                WHERE ROUND(trackpoint.lat, 2)=39.92 AND ROUND(trackpoint.lon, 2)=116.40;"""
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Users that have activities in The Forbidden City:")
        print(res)
        
    # 11
    def transportation_mode_users(self):
        query = """
        SELECT b.user_id, MAX(c.transportation_mode) AS most_used_tranportation_mode
        FROM (
            SELECT user_id, MAX(count) AS max
            FROM (
                SELECT user_id, transportation_mode, COUNT(*) AS count
                FROM activity
                WHERE transportation_mode IS NOT NULL
                GROUP BY user_id, transportation_mode
                ORDER BY user_id
            ) AS a
            GROUP BY user_id
        ) AS b
        INNER JOIN (
            SELECT user_id, transportation_mode, COUNT(*) AS count
            FROM activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
            ORDER BY user_id
        ) AS c ON b.user_id = c.user_id AND b.max = c.count
        GROUP BY user_id;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        print("Most user transportation modes:")
        print(res)


def build_database():
    try:
        program = Program()
        program.drop_table(table_name="user")
        program.drop_table(table_name="activity")
        program.drop_table(table_name="trackpoint")
        program.create_user_table(table_name="user")
        program.create_activity_table(table_name="activity")
        program.create_trackpoint_table(table_name="trackpoint")
        data_reader = MyDataReader()
        users, activities, trackpoints = data_reader.read()
        print(activities)
        program.insert_user_data(users)
        program.insert_activity_data(activities)
        # program.insert_trackpoint_data(trackpoints)
        for i in range(20000, len(trackpoints), 20000):
            print(str(100 * i / len(trackpoints)) + " %")
            program.insert_trackpoint_data(trackpoints[i - 20000:i])
        program.insert_trackpoint_data(trackpoints[i:])
        _ = program.fetch_data(table_name="user")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


def main():
    # build_database()
    program = Program()
    program.transportation_mode_users()


if __name__ == '__main__':
    main()
