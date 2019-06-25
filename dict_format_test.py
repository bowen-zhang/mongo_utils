import datetime
import unittest

import dict_format
import test_pb2


class DictFormatTests(unittest.TestCase):
  def setUp(self):
    self._proto = test_pb2.Test(
        int_value=123,
        float_value=123.456,
        string_value='Hello World!',
        int_list=[1, 2, 3, 4, 5],
        sub_test=test_pb2.SubTest(value=100),
        sub_test_list=[
            test_pb2.SubTest(value=1),
            test_pb2.SubTest(value=2),
            test_pb2.SubTest(value=3),
        ],
    )
    self._proto.datetime_value.FromDatetime(datetime.datetime.utcnow())

  def testMessageToDict(self):
    bson = dict_format.MessageToDict(self._proto)
    self.assertIn('int_value', bson)
    self.assertTrue(isinstance(bson['int_value'], int))
    self.assertEqual(bson['int_value'], self._proto.int_value)
    self.assertIn('float_value', bson)
    self.assertTrue(isinstance(bson['float_value'], float))
    self.assertEqual(bson['float_value'], self._proto.float_value)
    self.assertIn('string_value', bson)
    self.assertTrue(isinstance(bson['string_value'], unicode))
    self.assertEqual(bson['string_value'], self._proto.string_value)
    self.assertIn('int_list', bson)
    self.assertTrue(isinstance(bson['int_list'], list), type(bson['int_list']))
    self.assertEqual(bson['int_list'], self._proto.int_list)
    self.assertIn('sub_test', bson)
    self.assertTrue(isinstance(bson['sub_test'], dict))
    self.assertEqual(bson['sub_test']['value'], self._proto.sub_test.value)
    self.assertIn('sub_test_list', bson)
    self.assertTrue(isinstance(bson['sub_test_list'], list))
    self.assertEqual(len(bson['sub_test_list']), 3)
    for i in range(len(self._proto.sub_test_list)):
      self.assertTrue(isinstance(bson['sub_test_list'][i], dict))
      self.assertEqual(bson['sub_test_list'][i]['value'],
                       self._proto.sub_test_list[i].value)
    self.assertIn('datetime_value', bson)
    self.assertTrue(isinstance(bson['datetime_value'], datetime.datetime))
    self.assertEqual(bson['datetime_value'],
                     self._proto.datetime_value.ToDatetime())

  def testParse(self):
    bson = {
        'int_value': self._proto.int_value,
        'float_value': self._proto.float_value,
        'string_value': self._proto.string_value,
        'int_list': self._proto.int_list,
        'sub_test': {
            'value': self._proto.sub_test.value,
        },
        'sub_test_list': [],
        'datetime_value': self._proto.datetime_value.ToDatetime(),
    }

    for sub_test in self._proto.sub_test_list:
      bson['sub_test_list'].append({'value': sub_test.value})

    proto = dict_format.Parse(bson, test_pb2.Test())

    self.assertEqual(proto, self._proto)


if __name__ == '__main__':
  unittest.main()