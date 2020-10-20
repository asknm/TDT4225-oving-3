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
        print("ERROR: Failed to build database:", e)
    finally:
        if program:
            program.connection.close_connection()


def main():
    # build_db()
    count_all()


if __name__ == '__main__':
    main()
