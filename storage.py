import pymongo

from . import dict_format


class AutoId(object):
  _COLLECTION_NAME = '__ID__'

  def __init__(self, name, database, client=None):
    client = client or pymongo.MongoClient()
    self._coll = client[database][self._COLLECTION_NAME]
    self._name = name

    self._coll.update_one(
        {
            'name': self._name
        }, {'$setOnInsert': {
            'name': self._name,
            'next_id': 0
        }},
        upsert=True)

  def next_id(self):
    doc = self._coll.find_one_and_update({
        'name': self._name
    }, {'$inc': {
        'next_id': 1
    }})
    return doc['next_id']


class MongoStorage(object):
  def __init__(self, database, collection, client=None, auto_id_field=None):
    self._database = database
    self._collection = collection
    self._client = client or pymongo.MongoClient()

    self._coll = self._client[database][collection]
    self._auto_id = None
    self._auto_id_field = auto_id_field

  @property
  def collection(self):
    return self._coll

  @property
  def auto_id(self):
    if not self._auto_id:
      self._auto_id = AutoId(self._collection, self._database, self._client)
    return self._auto_id

  def find_one(self, filter):
    return self._coll.find_one(filter)

  def find(self, filter=None, limit=0, sort=None):
    return self._coll.find(filter=filter, limit=limit, sort=sort)

  def find_or_create(self, filter, new_doc):
    doc = self._coll.find_one(filter)
    if not doc:
      if self._auto_id_field:
        new_doc[self._auto_id_field] = self.auto_id.next_id()

      self._coll.update_one(filter, {'$setOnInsert': new_doc}, upsert=True)
      doc = self._coll.find_one(filter)

    return doc

  def save(self, doc, filter=None):
    """Inserts or updates a document.

    Args:
      doc: document (as a dictionary) to save.
      filter: if given, updates matching document, otherwise insert new document.
    Returns:
      id of document.
    """
    if filter:
      self._coll.update_one(filter, {'$set': doc}, upsert=True)
      doc = self._coll.find_one(filter)
      return str(doc['_id'])
    else:
      insert_one_result = self._coll.insert_one(doc)
      return str(insert_one_result.inserted_id)


class ProtobufMongoStorage(MongoStorage):
  def __init__(self, proto_cls, *args, **kwargs):
    super(ProtobufMongoStorage, self).__init__(*args, **kwargs)
    self._proto_cls = proto_cls

  def find_one(self, filter):
    doc = super(ProtobufMongoStorage, self).find_one(filter)
    if doc:
      return dict_format.Parse(doc, self._proto_cls())
    else:
      return None

  def find(self, filter=None, limit=0, sort=None):
    for doc in super(ProtobufMongoStorage, self).find(filter, limit, sort):
      yield dict_format.Parse(doc, self._proto_cls())

  def find_or_create(self, filter, proto):
    doc = dict_format.MessageToDict(proto)
    doc = super(ProtobufMongoStorage, self).find_or_create(filter, doc)
    return dict_format.Parse(doc, proto)

  def save(self, proto, filter=None):
    doc = dict_format.MessageToDict(proto)
    return super(ProtobufMongoStorage, self).save(doc, filter)