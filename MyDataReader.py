import os

from numpy import double
from numpy.core import long


def find_labels(path):
    try:
        labels = []
        with open(path + "\\labels.txt") as file:
            for i, l in enumerate(file):
                if i > 0:
                    start_date, start_time, end_date, end_time, transportation_mode = l.split()
                    label_dict = {
                        "start_date_time": start_date.replace("/", "-") + " " + start_time,
                        "end_date_time": end_date.replace("/", "-") + " " + end_time,
                        "transportation_mode": transportation_mode,
                    }
                    labels.append(label_dict)
        return True, labels
    except Exception:
        return False, []


class MyDataReader:

    def read(self):
        users = []
        activities = []
        trackpoints = []
        for dirname, dirnames, _ in os.walk('.\\dataset\\dataset\\Data'):
            for subdirname in dirnames:
                if subdirname != "Trajectory":
                    print("UserID:" + subdirname)
                    has_labels, labels = find_labels(os.path.join(dirname, subdirname))

                    complete_subdir = os.path.join(dirname, subdirname) + "\\Trajectory"
                    for _, _, filenames in os.walk(complete_subdir):
                        for i, filename in enumerate(filenames):

                            j = 0
                            # Enumerates through the file ones to check number of lines
                            with open(os.path.join(complete_subdir, filename), "r") as file:
                                for j, l in enumerate(file):
                                    pass
                            # Reads data from file if it is not too long
                            start_date_time = None
                            end_date_time = None
                            transportation_mode = None
                            if j < 2506:
                                with open(os.path.join(complete_subdir, filename), "r") as file:
                                    for k, l in enumerate(file):
                                        # Changed j to k
                                        if k > 5:
                                            lat, lon, _, altitude, date_days, date, time = l.split(",")
                                            trackpoint_dict = {
                                                "activity_id": int(filename.split(".")[0]),
                                                "lat": float(lat),
                                                "lon": float(lon),
                                                "altitude": round(float(altitude)),
                                                "date_days": date_days,
                                                "date_time": (date + " " + time).strip(),
                                            }
                                            trackpoints.append(trackpoint_dict)

                                            if k == 6:
                                                start_date_time = (date + " " + time).strip()
                                            elif k == j:
                                                end_date_time = (date + " " + time).strip()
                                for label in labels:
                                    if label["start_date_time"] == start_date_time and \
                                            label["end_date_time"] == end_date_time:
                                        transportation_mode = label["transportation_mode"]
                                activity_dict = {
                                    "activity_id": int(filename.split(".")[0]),
                                    "user_id": subdirname,
                                    "transportation_mode": transportation_mode,
                                    "start_date_time": start_date_time,
                                    "end_date_time": end_date_time,
                                }
                                activities.append(activity_dict)

                    user_dict = {
                        "_id": subdirname,
                        "has_labels": has_labels,
                    }
                    users.append(user_dict)
        print(len(activities))
        print(len(trackpoints))
        return users, activities, trackpoints


if __name__ == "__main__":
    MyDataReader()
