from pprint import pprint

import pymongo
from haversine import haversine

from DbConnector import DbConnector
from MyDataReader import MyDataReader


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def insert_documents(self, docs, collection_name):
        collection = self.db[collection_name]
        collection.insert_many(docs)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def count_documents(self, collection_name):
        collection = self.db[collection_name]
        print(collection.count_documents(filter={}))

    def update_date_times(self):
        collection = self.db['activity']
        collection.update_many({}, [{
            '$set':
                {
                    'start_date_time':
                        {
                            '$dateFromString':
                                {
                                    'dateString': '$start_date_time'
                                }
                        },
                    'end_date_time':
                        {
                            '$dateFromString':
                                {
                                    'dateString': '$end_date_time'
                                }
                        }
                }
        }])

        collection = self.db['trackpoint']
        collection.update_many({}, [{
            '$set':
                {
                    'date_time':
                        {
                            '$dateFromString':
                                {
                                    'dateString': '$date_time'
                                }
                        }
                }
        }])

    # TASK 2
    # 2.1
    def count_all(self, collection_name):
        collection = self.db[collection_name]
        count = collection.find({}).count()
        print(count)

    # 2.2
    def average_activities(self):
        collection_activity = self.db['activity']
        collection_user = self.db['user']
        count_activity = collection_activity.find({}).count()
        count_user = collection_user.find({}).count()
        count = count_activity / count_user
        print("Average amount of activities per user:")
        print(count)

    # 2.3
    def top_twenty_users(self):
        collection_activity = self.db['activity']
        collection = collection_activity.aggregate([
            {
                '$group':
                    {
                        '_id': '$user_id',
                        'count': {'$sum': 1}
                    }
            },
            {
                '$sort': {"count": -1}
            }
        ])
        result = collection.find({}).limit(20)
        print(result)

    # 2.4
    def taxi_users(self):
        collection_activity = self.db['activity']
        collection = collection_activity.aggregate([
            {
                '$match':
                    {
                        'transportation_mode': 'taxi'
                    }
            },
            {
                '$group':
                    {
                        '_id': '$user_id'
                    }
            }
        ])
        for user in collection:
            print(user)

    # 2.5
    def all_transportation_modes(self):
        collection = self.db['activity']
        groups = collection.aggregate([
            {
                '$match':
                    {
                        'transportation_mode':
                            {
                                '$ne': None
                            }
                    }
            },
            {
                '$group':
                    {
                        '_id': "$transportation_mode",
                        'count': {'$sum': 1}
                    }
            }
        ])
        for group in groups:
            pprint(group)

    # 2.6a
    def most_active_year_by_activity_count(self):
        collection = self.db['activity']
        groups = collection.aggregate([
            {
                '$group':
                    {
                        '_id': {
                            '$year': '$start_date_time'
                        },
                        'count': {'$sum': 1}
                    }
            },
            {
                '$sort':
                    {
                        'count': -1
                    }
            },
            {
                '$limit': 1
            },
            {
                '$project':
                    {
                        '_id': '$_id'
                    }
            }
        ])
        for group in groups:
            pprint(group)

    # 2.6b
    def most_active_year_by_hours(self):
        collection = self.db['activity']
        groups = collection.aggregate([
            {
                '$addFields':
                    {
                        'hours':
                            {
                                '$divide':
                                    [
                                        {
                                            '$subtract': ['$end_date_time', '$start_date_time']
                                        },
                                        3600000
                                    ]
                            }
                    }
            },
            {
                '$group':
                    {
                        '_id': {
                            '$year': '$start_date_time'
                        },
                        'sum_hours':
                            {
                                '$sum': '$hours'
                            }
                    }
            },

            {
                '$sort':
                    {
                        'sum_hours': -1
                    }
            },
        ])
        for group in groups:
            pprint(group)

    # 2.7
    def user_112_distance_walked_2008(self):
        collection_activity = self.db['activity']
        # collection_activity.create_index([('activity_id', pymongo.ASCENDING)])
        collection_trackpoint = self.db['trackpoint']
        # collection_trackpoint.create_index([('activity_id', pymongo.ASCENDING)])
        groups = collection_activity.aggregate([
            {
                '$project':
                    {
                        'user_id': '$user_id',
                        'transportation_mode': '$transportation_mode',
                        'activity_id': '$activity_id',
                        'year':
                            {
                                '$year': '$start_date_time'
                            }
                    }
            },
            {
                '$match':
                    {
                        'user_id': '112',
                        'transportation_mode': 'walk',
                        'year': 2008
                    }
            },
            {
                '$lookup':
                    {
                        'from': 'trackpoint',
                        'localField': 'activity_id',
                        'foreignField': 'activity_id',
                        'as': 'trackpoints'
                    }
            },
        ])
        distance = 0.0
        for group in groups:
            for i, trackpoint in enumerate(group['trackpoints']):
                if i > 0:
                    distance += haversine((trackpoint['lat'], trackpoint['lon']),
                                          (group['trackpoints'][i - 1]['lat'], group['trackpoints'][i - 1]['lon']))
        print("Total distance:")
        print(distance)

    # 2.8
    def top_20_altitude(self):
        collection = self.db['trackpoint']
        groups = collection.aggregate([
            {
                '$group':
                    {
                        '_id': '$activity_id',
                        'altitudes':
                            {
                                '$push': '$altitude'
                            }
                    }
            },
            {
                '$project':
                    {
                        '_id': '$_id',
                        'temp':
                            {
                                '$reduce':
                                    {
                                        'input': '$altitudes',
                                        'initialValue':
                                            {
                                                'prev': -777,
                                                'altitude_gain': 0
                                            },
                                        'in':
                                            {
                                                'prev': '$$this',
                                                'altitude_gain':
                                                    {
                                                        '$add': ['$$value.altitude_gain', {
                                                            '$cond': [
                                                                {
                                                                    '$and':
                                                                        [
                                                                            {
                                                                                '$gt': ['$$this', '$$value.prev']
                                                                            },
                                                                            {
                                                                                '$ne': ['$$this', -777]
                                                                            },
                                                                            {
                                                                                '$ne': ['$$value.prev', -777]
                                                                            }
                                                                        ]
                                                                },
                                                                {
                                                                    '$subtract': ['$$this', '$$value.prev']
                                                                },
                                                                0
                                                            ]
                                                        }]
                                                    }
                                            }
                                    }
                            }
                    }
            },
            {
                '$project':
                    {
                        '_id': '$_id',
                        'gain': '$temp.altitude_gain'
                    }
            },
            {
                '$lookup':
                    {
                        'from': 'activity',
                        'localField': '_id',
                        'foreignField': 'activity_id',
                        'as': 'activity'
                    }
            },
            {
                '$unwind': '$activity'
            },
            {
                '$group':
                    {
                        '_id': '$activity.user_id',
                        'total_gain':
                            {
                                '$sum': '$gain'
                            }
                    }
            },
            {
                '$sort':
                    {
                        'total_gain': -1
                    }
            },
            {
                '$limit': 20
            }
        ], allowDiskUse=True)
        for group in groups:
            pprint(group)

    # 2.9

    # 2.10
    def forbidden_city(self):
        collection = self.db['trackpoint']
        groups = collection.aggregate([
            {
                '$project': {
                    'activity_id': '$activity_id',
                    'rounded_lat':
                        {
                            '$round': ['$lat', 2]},
                    'rounded_lon':
                        {
                            '$round': ['$lon', 2]}
                }
            },
            {
                '$match':
                    {
                        'rounded_lat': 39.92,
                        'rounded_lon': 116.40
                    }
            },
            {
                '$group':
                    {
                        '_id': '$activity_id'
                    }
            },
            {
                '$lookup':
                    {
                        'from': 'activity',
                        'localField': '_id',
                        'foreignField': 'activity_id',
                        'as': 'activity'
                    }
            },
            {
                '$unwind': '$activity'
            },
            {
                '$group':
                    {
                        '_id': '$activity.user_id'
                    }
            }
        ])
        for group in groups:
            pprint(group)

    # 2.11
    def most_transportation_modes(self):
        collection = self.db['activity']
        groups = collection.aggregate([
            {
                '$match':
                    {
                        'transportation_mode':
                            {
                                '$ne': None
                            }
                    }
            },
            {
                '$group':
                    {
                        '_id': {
                            'user_id': '$user_id',
                            'transportation_mode': '$transportation_mode'
                        },
                        'count': {'$sum': 1}
                    }
            },
            {
                '$group':
                    {
                        '_id': '$_id.user_id',
                        'transportation_mode': {'$first': '$_id.transportation_mode'},
                        'max_c': {'$max': '$count'}
                    }
            },
            {
                '$project':
                    {
                        '_id': '$_id',
                        'most_used_transportation_mode': '$transportation_mode',
                    }
            },
            {
                '$sort':
                    {
                        '_id': 1
                    }
            }])
        for group in groups:
            pprint(group)


def build_db():
    program = None
    try:
        program = Program()
        program.drop_coll(collection_name="user")
        program.drop_coll(collection_name="activity")
        program.drop_coll(collection_name="trackpoint")
        program.create_coll(collection_name="user")
        program.create_coll(collection_name="activity")
        program.create_coll(collection_name="trackpoint")
        data_reader = MyDataReader()
        users, activities, trackpoints = data_reader.read()
        program.insert_documents(users, "user")
        program.insert_documents(activities, "activity")
        program.insert_documents(trackpoints, "trackpoint")
    except Exception as e:
        print("ERROR: Failed to build database:", e)
    finally:
        if program:
            program.connection.close_connection()


def count_all():
    program = None
    try:
        program = Program()
        program.count_documents(collection_name="user")
        program.count_documents(collection_name="activity")
        program.count_documents(collection_name="trackpoint")
    except Exception as e:
        print("ERROR: Failed to count documents:", e)
    finally:
        if program:
            program.connection.close_connection()


def test():
    program = None
    try:
        program = Program()
        # program.fetch_documents('trackpoint')
        # program.update_date_times()

        # 2.1
        # program.count_all('user')
        # program.count_all('activity')
        # program.count_all('trackpoint')
        # 2.2
        # program.average_activities()
        # 2.3
        # program.top_twenty_users()
        # 2.4
        # program.taxi_users()
        # 2.5
        # program.all_transportation_modes()
        # 2.6a
        # program.most_active_year_by_activity_count()
        # 2.6b
        # program.most_active_year_by_hours()
        # 2.7
        program.user_112_distance_walked_2008()
        # 2.8
        # program.top_20_altitude()
        # 2.9

        # 2.10
        # program.forbidden_city()
        # 2.11
        # program.most_transportation_modes()

    except Exception as e:
        print("ERROR: Failed test:", e)
    finally:
        if program:
            program.connection.close_connection()


def main():
    # build_db()
    # count_all()
    test()


if __name__ == '__main__':
    main()
