from pprint import pprint
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
        pass


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

        # program.all_transportation_modes()
        # program.most_active_year_by_activity_count()
        program.most_active_year_by_hours()
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
