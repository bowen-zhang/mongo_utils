import logging
import pymongo

from bson import objectid

from . import dict_format


class ProtobufMongoStorage(object):
  def __init__(self, proto_cls, database, collection, id_field, client=None, indexes=None):
    self._proto_cls = proto_cls
    self._database_name = database
    self._collection_name = collection
    self._client = client or pymongo.MongoClient()
    self._indexes = indexes or {}

    self._coll = self._client[database][collection]
    self._id_field = id_field

    self._logger = logging.getLogger(self.__class__.__name__)

  @property
  def proto_cls(self):
    return self._proto_cls
  
  @property
  def name(self):
    return self._collection_name

  @property
  def collection(self):
    return self._coll

  def create_indexes(self):
    new_count = 0
    update_count = 0
    remove_count = 0

    old_indexes = list(self._coll.list_indexes())
    old_indexes = {x['name']: list(x['key'].items()) for x in old_indexes}

    for name, keys in self._indexes.items():
      if name in old_indexes:
        if self._same_keys(keys, self._indexes[name]):
          continue
        self._coll.drop_index(name)
        self._coll.create_index(name=name, keys=keys, background=True)
        update_count += 1
      else:
        self._coll.create_index(name=name, keys=keys, background=True)
        new_count += 1

    for name, keys in old_indexes.items():
      if name not in self._indexes and name != '_id_':
        self._coll.drop_index(name)
        remove_count += 1

    self._logger.info('[{0}.{1}] Created {2} indexes, updated {3} indexes, removed {4} indexes.'.format(
        self._database_name, self._collection_name, new_count, update_count, remove_count))

  def _same_keys(self, keys1, keys2):
    return keys1.sort(key=lambda x: x[0]) == keys2.sort(key=lambda x: x[0])

  def find_one(self, *args, **kwargs):
    filter = args[0] if args else kwargs.get('filter', {})
    if self._id_field in filter:
        filter['_id'] = objectid.ObjectId(filter[self._id_field])
        del filter[self._id_field]

    doc = self._coll.find_one(*args, **kwargs)
    if doc:
      doc[self._id_field] = str(doc['_id'])
      return dict_format.Parse(doc, self._proto_cls())
    else:
      return None

  def find(self, *args, **kwargs):
    filter = args[0] if args else kwargs.get('filter', {})
    if self._id_field in filter:
        filter['_id'] = objectid.ObjectId(filter[self._id_field])
        del filter[self._id_field]

    for doc in self._coll.find(*args, **kwargs):
      doc[self._id_field] = str(doc['_id'])
      yield dict_format.Parse(doc, self._proto_cls())

  def insert(self, proto):
    """Inserts a protobuf.

    Args:
      doc: protobuf to save.
    Returns:
      Saved protobuf
    """
    doc = dict_format.MessageToDict(proto)
    assert self._id_field not in doc
    
    oid = self._coll.insert_one(doc).inserted_id

    doc[self._id_field] = str(oid)
    return dict_format.Parse(doc, self._proto_cls())

  def replace(self, proto):
    doc = dict_format.MessageToDict(proto)
    assert doc[self._id_field]

    oid = objectid.ObjectId(doc[self._id_field])
    del doc[self._id_field]
    self._coll.replace_one({'_id': oid}, doc)  

  def upsert_by(self, proto, filter):
    doc = dict_format.MessageToDict(proto)
    assert self._id_field not in doc

    oid = self._coll.replace_one(filter, doc, upsert=True).upserted_id
    doc[self._id_field] = str(oid)
    return dict_format.Parse(doc, self._proto_cls())
  
  def delete_by_id(self, id):
    self._coll.delete_one({'_id': objectid.ObjectId(id)})
 
  def delete_by(self, filter) -> int:
    result = self._coll.delete_many(filter)
    return result.deleted_count
  