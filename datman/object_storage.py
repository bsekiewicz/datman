# -*- coding: utf-8 -*-

import json
from minio import Minio
from io import BytesIO


class ObjectStorage(object):
    """Object Storage based S3 Utilities
    """

    connection = None

    def connect(self, connection_params):
        """Create connection to S3
        """
        if isinstance(connection_params, str):
            connection_params_dict = json.loads(connection_params)
        elif isinstance(connection_params, dict):
            connection_params_dict = connection_params
        else:
            raise TypeError(f"invalid connection_params type: {type(connection_params)}")

        self.connection = Minio(**connection_params_dict)

    def put_object(self, bucket_name, object_name, data, content_type=None):
        if content_type is None:
            content_type = "application/octet-stream"
        try:
            if isinstance(data, BytesIO):
                f = data
                f.seek(0)
            else:
                f = BytesIO(data)
            self.connection.put_object(bucket_name=bucket_name, object_name=object_name, content_type=content_type,
                                       data=f, length=f.getbuffer().nbytes)
            return True
        except Exception as e:
            print(e)
            return False

    def get_object(self, bucket_name, object_name):
        try:
            return self.connection.get_object(bucket_name=bucket_name, object_name=object_name)
        except Exception as e:
            print(e)
            return None
