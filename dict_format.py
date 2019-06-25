"""Library to convert a protobuf message to and from Python built-in dictionary."""

import datetime

from google.protobuf import descriptor
from google.protobuf import timestamp_pb2

PRIMITIVE_TYPES = [
    descriptor.FieldDescriptor.TYPE_DOUBLE,
    descriptor.FieldDescriptor.TYPE_FLOAT,
    descriptor.FieldDescriptor.TYPE_INT64,
    descriptor.FieldDescriptor.TYPE_UINT64,
    descriptor.FieldDescriptor.TYPE_INT32,
    descriptor.FieldDescriptor.TYPE_UINT32,
    descriptor.FieldDescriptor.TYPE_FIXED64,
    descriptor.FieldDescriptor.TYPE_FIXED32,
    descriptor.FieldDescriptor.TYPE_BOOL,
    descriptor.FieldDescriptor.TYPE_STRING,
    descriptor.FieldDescriptor.TYPE_SFIXED32,
    descriptor.FieldDescriptor.TYPE_SFIXED64,
    descriptor.FieldDescriptor.TYPE_SINT32,
    descriptor.FieldDescriptor.TYPE_SINT64,
    descriptor.FieldDescriptor.TYPE_BYTES,
]


def Parse(dict_obj, proto):
  """Parses a dictionary object into protobuf.

  Args:
    dict_obj: a Python built-in dictionary object.
    proto: protobuf message to parse data into.
  Returns:
    The same instance as "proto" argument.
  """
  proto.Clear()

  if isinstance(dict_obj, datetime.datetime):
    assert isinstance(proto, timestamp_pb2.Timestamp)
    proto.FromDatetime(dict_obj)
    return proto

  for field in proto.DESCRIPTOR.fields:
    if field.name not in dict_obj:
      continue

    if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
      value = getattr(proto, field.name)
      if field.type in PRIMITIVE_TYPES:
        value.extend(dict_obj[field.name])
      elif field.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
        for child in dict_obj[field.name]:
          Parse(child, value.add())
    else:
      if field.type in PRIMITIVE_TYPES:
        setattr(proto, field.name, dict_obj[field.name])
      elif field.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
        Parse(dict_obj[field.name], getattr(proto, field.name))

  return proto


def MessageToDict(proto):
  """Converts protobuf message to Python built-in dictionary object.

  Args:
    proto: a protobuf message.
  Returns:
    A Python built-in dictionary object.
  """
  if isinstance(proto, timestamp_pb2.Timestamp):
    return proto.ToDatetime()

  dict_obj = {}
  for field in proto.DESCRIPTOR.fields:
    if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
      value = getattr(proto, field.name)
      dict_obj_list = []
      if field.type in PRIMITIVE_TYPES:
        for child in value:
          dict_obj_list.append(child)
      elif field.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
        for child in value:
          dict_obj_list.append(MessageToDict(child))
      if dict_obj_list:
        dict_obj[field.name] = dict_obj_list
    else:
      if field.type in PRIMITIVE_TYPES:
        value = getattr(proto, field.name)
        dict_obj[field.name] = value
      elif field.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
        if proto.HasField(field.name):
          value = getattr(proto, field.name)
          dict_obj[field.name] = MessageToDict(value)

  return dict_obj
