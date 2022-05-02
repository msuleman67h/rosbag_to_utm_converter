#!/usr/bin/env python
from datetime import datetime

import rospy
import utm
from rosbag import Bag
from rosbag_to_utm_converter.srv import RosbagToUTM


def rosbag_to_utm(req):
    source_file = req.source_file
    gps_topic_name = req.gps_topic_name
    gps_topic_type = req.gps_topic_type

    with Bag(source_file) as bag:
        if gps_topic_name not in bag.get_type_and_topic_info().topics:
            return "\"{}\" topic not found in the rosbag. " \
                   "Please rename the topic in launch file if gps coordinates are published under a different topic name.".format(gps_topic_name)

        if gps_topic_type not in ["NavSatFix", "Vector3Stamped", "Vector3"]:
            return "Incorrect GPS message type. Supported types are: NavSatFix, Vector3Stamped, and Vector3."

        rospy.loginfo("Converting Bag file")
        utm_file_name = "{}_UTM_{}.csv".format(source_file, datetime.now().isoformat())
        with open(utm_file_name, 'w') as file_pointer:
            for topic, msg, t in bag.read_messages(topics=[gps_topic_name]):
                if gps_topic_type == "NavSatFix":
                    file_pointer.write(",".join(map(str, utm.from_latlon(latitude=msg.latitude, longitude=msg.longitude)[:2])))
                    file_pointer.write(",{}\n".format(msg.altitude))
                elif gps_topic_type == "Vector3Stamped":
                    file_pointer.write(",".join(map(str, utm.from_latlon(latitude=msg.vector.x, longitude=msg.vector.y)[:2])))
                    file_pointer.write(",{}\n".format(msg.vector.z))
                elif gps_topic_type == "Vector3":
                    file_pointer.write(",".join(map(str, utm.from_latlon(latitude=msg.x, longitude=msg.y)[:2])))
                    file_pointer.write(",{}\n".format(msg.z))
    return "Conversion complete. File save at {}".format(utm_file_name)


def main():
    rospy.init_node('rosbag_to_utm_converter')
    rospy.Service("convert_rosbag_to_utm", RosbagToUTM, rosbag_to_utm)
    rospy.spin()


if __name__ == '__main__':
    main()
