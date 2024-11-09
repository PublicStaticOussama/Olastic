from elasticsearch import AsyncElasticsearch, Elasticsearch, helpers
# import aiohttp

from helpers import functions
from helpers.colors import C


import asyncio
import traceback
import uuid
import json

class Olastic:
    elastic: Elasticsearch = None

    def __init__(self, hosts, port):
        Olastic.hosts = hosts
        Olastic.port = port
        # Olastic.session = aiohttp.ClientSession()

        Olastic.elastic = AsyncElasticsearch(hosts=Olastic.hosts)
        Olastic.sync = Elasticsearch(hosts=Olastic.hosts)

    async def getConnection(hosts, port):
        # Olastic.session = aiohttp.ClientSession()
        
        Olastic.elastic = AsyncElasticsearch(hosts=Olastic.hosts)
        Olastic.sync = Elasticsearch(hosts=Olastic.hosts)

        return Olastic.elastic
    
    async def close(self):
        await Olastic.elastic.close()
    
def OlasticSchema(cls):
    if not hasattr(cls, 'elastic'):
        raise Exception("Error: a connection to elasticsearch has to be established before using an Olastic schema")
    if cls.elastic is None:
        raise Exception("Error: the connection to elasticsearch has to be established before defining an Olastic schema")
    cls.subclass_name: str = cls.__name__
    cls.index_name = cls.subclass_name.lower()
    if cls.index_name[-1] == "y":
        cls.index_name = cls.index_name[:-1]
        cls.index_name += "ies"
    else:
        cls.index_name += "s"

    instance = cls()

    mapping = {
        'mappings': {
            'properties': {
            }
        }
    }

    # mapping["mappings"]["properties"]["_id"] = {"type": "keyword"}
    cls.field_names = vars(instance)
    for fieldname, value in cls.field_names.items():
        if fieldname == "created_at" or fieldname == "updated_at" or fieldname == "_id":
            continue
        if fieldname in ["$not"]:
            raise Exception(f"TODO Error: cannot use {fieldname} as a field name at the moment")
        if issubclass(type(value), bool):
            mapping["mappings"]["properties"][fieldname] = {"type": "boolean"}
        elif issubclass(type(value), int):
            mapping["mappings"]["properties"][fieldname] = {"type": "long"}
        elif issubclass(type(value), float):
            mapping["mappings"]["properties"][fieldname] = {"type": "double"}
        elif issubclass(type(value), str):
            mapping["mappings"]["properties"][fieldname] = {"type": "keyword"}
        else:
            mapping["mappings"]["properties"][fieldname] = {"type": "long"}

    mapping["mappings"]["properties"]["created_at"] = {"type": "date"}
    mapping["mappings"]["properties"]["updated_at"] = {"type": "date"}

    doesExists = bool(cls.sync.indices.exists(index=cls.index_name))
    if doesExists:
        print(f"{C.warn}[Olastic] Warning:{C.end} Index already exists")
    else:
        cls.sync.indices.create(index=cls.index_name, body=mapping)

    if not doesExists:
        if cls.sync.indices.exists(index=cls.index_name):
            print(f"{C.ok}[Olastic] Info:{C.end} Index {cls.index_name} created successfully !")
        else:
            raise Exception(f"{C.err}[Olastic] Error:{C.end} Failed to create index '{cls.index_name}'.")
        
    original_init = cls.__init__

    def new(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        for fieldname, default_val in cls.field_names.items():
            val = getattr(self, fieldname)
            cast_val = val
            if issubclass(type(default_val), bool): # this if HAS to come before the int case, because
                cast_val = functions.resolve_bool(val)
            elif issubclass(type(default_val), float):
                cast_val = float(val) 
            elif issubclass(type(default_val), int):
                cast_val = int(val)
            else:
                cast_val = str(val)
            setattr(self, fieldname, cast_val)

        setattr(self, "_id", str(functions.uuid_hex()))
        setattr(self, "created_at", functions.get_current_date())
        setattr(self, "updated_at", functions.get_current_date())

    cls.__init__ = new

    def toString(self):
        return json.dumps(self.__dict__, indent=4)
    
    cls.__str__ = toString
    cls.__repr__ = toString

    def create(doc_dict, exists=False):
        inst = cls()
        for fieldname, default_val in cls.field_names.items():
            if fieldname in doc_dict:
                val = doc_dict[fieldname]
                cast_val = val
                if issubclass(type(default_val), bool): # this if HAS to come before the int case, because
                    cast_val = functions.resolve_bool(val)
                elif issubclass(type(default_val), float):
                    cast_val = float(val) 
                elif issubclass(type(default_val), int):
                    cast_val = int(val) 
                else:
                    cast_val = str(val)
                setattr(inst, fieldname, cast_val)
        
        setattr(inst, "_id", doc_dict["_id"])
        if "created_at" in doc_dict:
            setattr(inst, "created_at", doc_dict["created_at"])
        if "updated_at" in doc_dict:
            setattr(inst, "updated_at", doc_dict["updated_at"])
        # print(doc_dict["_id"])
        if not exists:
            setattr(inst, "_id", str(functions.uuid_hex()))
            setattr(inst, "created_at", functions.get_current_date())
            setattr(inst, "updated_at", functions.get_current_date())
        
        return inst
    
    cls.create = create

    def _resolve_value_by_fieldname(field, value, elastic_query, clause="must"):
        default_type = type(cls.field_names[field])

        if issubclass(default_type, bool):
            elastic_query["query"]["bool"][clause].append({
                "term": {
                    f"{field}": functions.resolve_bool(value)
                }
            })
        elif issubclass(default_type, float) or issubclass(default_type, int):
            if type(value) == dict:
                if "$lt" in value or "$gt" in value or "$lte" in value or "$gte" in value:
                    range_query = {
                        "range": {
                            f"{field}": {}
                        }
                    }
                    if "$lt" in value:
                        range_query["range"][field]["$lt"] = default_type(value["$lt"])
                    if "$gt" in value:
                        range_query["range"][field]["$gt"] = default_type(value["$gt"])
                    if "$lte" in value:
                        range_query["range"][field]["$lte"] = default_type(value["$lte"])
                    if "$gte" in value:
                        range_query["range"][field]["$gte"] = default_type(value["$gte"])
                else: 
                    raise Exception(f"Error: Invalid value ({str(value)}) given to field: '{field}'")
            else: 
                elastic_query["query"]["bool"][clause].append({
                    "term": {
                        f"{field}": default_type(value)
                    }
                })
        elif issubclass(default_type, str):
            if type(value) == list:
                elastic_query["query"]["bool"][clause].append({
                    "terms": {
                        f"{field}": value
                    }
                })
            else:
                elastic_query["query"]["bool"][clause].append({
                    "term": {
                        f"{field}": str(value)
                    }
                })
        else:
            if type(value) == list:
                elastic_query["query"]["bool"][clause].append({
                    "terms": {
                        f"{field}": value
                    }
                })
            else:
                elastic_query["query"]["bool"][clause].append({
                    "term": {
                        f"{field}": str(value)
                    }
                })

        return elastic_query
                

    def find(query):
        elastic_query = {
            "size": 10_000,
            "query": {
                "bool": {
                    "must_not": [],
                    "must": []
                }
            },
            "sort": [
                {"created_at": {"order": "desc"}}
            ]
            
        }

        for field, value in query.items():
            # =========================================================;
            # =========================================================;
            # =========================================================;
            if field in ["_id", "created_at", "updated_at"]:
                clause = "must"
                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"
                if type(value) == list:
                    elastic_query["query"]["bool"][clause].append({
                        "terms": value
                    })
                else:
                    elastic_query["query"]["bool"][clause].append({
                        "term": value
                    })
            elif field in cls.field_names:
                clause = "must"

                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"

                elastic_query = _resolve_value_by_fieldname(field=field, value=value, elastic_query=elastic_query, clause=clause)

        

        return OlasticQuery(search_query=elastic_query, cls=cls)
    
    cls.find = find

    async def findOne(query, sort_by="created_at", asc=False) -> cls:
        elastic_query = {
            "size": 1,
            "query": {
                "bool": {
                    "should": [],
                    "must_not": [],
                    "must": []
                }
            },
            "sort": [
                {"created_at": {"order": "desc"}}
            ]  
        }

        for field, value in query.items():
            # =========================================================;
            # =========================================================;
            # =========================================================;
            if field in ["_id", "created_at", "updated_at"]:
                clause = "must"
                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"
                if type(value) == list:
                    elastic_query["query"]["bool"][clause].append({
                        "terms": value
                    })
                else:
                    elastic_query["query"]["bool"][clause].append({
                        "term": value
                    })
            elif field in cls.field_names:
                clause = "must"

                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"

                elastic_query = _resolve_value_by_fieldname(field=field, value=value, elastic_query=elastic_query, clause=clause)

        if sort_by is not None:
            elastic_query["sort"] = [
                {
                    f"{sort_by}": {"order": "desc" if not bool(asc) else "asc"}
                }
            ]

        res = await cls.elastic.search(index=cls.index_name, body=elastic_query)
        hits = []

        if "hits" in res:
            if "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    hit["_source"]["_id"] = hit["_id"]
                    hits.append(hit["_source"])

        one = None
        for doc in hits:
            one = cls.create(doc, exists=True)
        
        return one

    cls.findOne = findOne

    async def insert(bulk):
        print(cls.index_name)
        documents = []
        for doc in bulk:
            try:
                new_doc = {}
                new_doc['_id'] = str(functions.uuid_hex())
                new_doc['_index'] = cls.index_name
                new_doc["_source"] = {}
                for field in doc:
                    val = doc[field]
                    cast_val = val
                    if field in cls.field_names:
                        if issubclass(type(cls.field_names[field]), bool): # this if HAS to come before the int case, because
                            cast_val = functions.resolve_bool(val)
                        elif issubclass(type(cls.field_names[field]), float):
                            cast_val = float(val) 
                        elif issubclass(type(cls.field_names[field]), int):
                            cast_val = int(val)
                        else:
                            cast_val = str(val)

                        new_doc["_source"][field] = cast_val
            
                new_doc["_source"]['created_at'] = functions.get_current_timestamp()
                new_doc["_source"]['updated_at'] = functions.get_current_timestamp()

                documents.append(new_doc)

            except Exception as e:
                traceback_info = traceback.format_exc()
                print(" +", traceback_info)
                print(e)

        success, failed = await helpers.async_bulk(cls.elastic, actions=documents)

        await asyncio.sleep(0.5)

        return success, failed

    cls.insert = insert


    async def deleteWhere(query):
        elastic_query = {
            "query": {
                "bool": {
                    "should": [],
                    "must_not": [],
                    "must": []
                }
            },
            "sort": [
                {"created_at": {"order": "desc"}}
            ]
            
        }
        for field, value in query.items():
            # =========================================================;
            # =========================================================;
            # =========================================================;
            if field in ["_id", "created_at", "updated_at"]:
                clause = "must"
                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"
                if type(value) == list:
                    elastic_query["query"]["bool"][clause].append({
                        "terms": value
                    })
                else:
                    elastic_query["query"]["bool"][clause].append({
                        "term": value
                    })
            elif field in cls.field_names:
                clause = "must"

                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"

                elastic_query = _resolve_value_by_fieldname(field=field, value=value, elastic_query=elastic_query, clause=clause)

        response = await cls.elastic.delete_by_query(index=cls.index_name, body=elastic_query)

        await asyncio.sleep(0.5)

        deleted = None

        if 'deleted' in response:
            print(f"{C.cyan}[Olastic] Info:{C.end} Deleted {response['deleted']} documents.")
            deleted = response['deleted']
        else:
            print(f"{C.err}[Olastic] Error:{C.end} Failed to perform delete-by-query operation.")
        
        return deleted

    cls.deleteWhere = deleteWhere

    async def updateWhere(values, query):
        elastic_query = {
            "query": {
                "bool": {
                    "should": [],
                    "must_not": [],
                    "must": []
                }
            }
        }

        for field, value in query.items():
            # =========================================================;
            # =========================================================;
            # =========================================================;
            if field in ["_id", "created_at", "updated_at"]:
                clause = "must"
                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"
                if type(value) == list:
                    elastic_query["query"]["bool"][clause].append({
                        "terms": value
                    })
                else:
                    elastic_query["query"]["bool"][clause].append({
                        "term": value
                    })
            elif field in cls.field_names:
                clause = "must"

                if type(value) == dict:
                    if "$not" in value:
                        value = value["$not"]
                        clause = "must_not"

                elastic_query = _resolve_value_by_fieldname(field=field, value=value, elastic_query=elastic_query, clause=clause)

        params = {}
        source = ""

        for field, value in values.items():
            if field in cls.field_names:
                cast_val = value
                if issubclass(type(cls.field_names[field]), bool): # this if HAS to come before the int case, because
                    cast_val = functions.resolve_bool(value)
                elif issubclass(type(cls.field_names[field]), float):
                    cast_val = float(value) 
                elif issubclass(type(cls.field_names[field]), int):
                    cast_val = int(value)
                else:
                    cast_val = str(value)

                if field != "_id":
                    params[field] = cast_val
                    source += f"ctx._source.{field} = params.{field};"

        params["updated_at"] = functions.get_current_timestamp()
        source += f"ctx._source.updated_at = params.updated_at;"

        elastic_query["script"] = {
            "source": source,
            "lang": "painless",
            "params": params
        }

        response = await cls.elastic.update_by_query(index=cls.index_name, body=elastic_query)

        await asyncio.sleep(0.4)

        updated = None

        if 'updated' in response:
            print(f"{C.ok}[Olastic] Info:{C.end} Updated {response['updated']} documents.")
            updated = response['updated']
        else:
            print(f"{C.err}[Olastic] Error:{C.end} Failed to perform update-by-query operation.")
        
        return updated
    
    cls.updateWhere = updateWhere
    
    async def deleteAll():
        if (await cls.elastic.indices.exists(index=cls.index_name)):
            response = await cls.elastic.delete_by_query(
                index=cls.index_name,
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )
            await asyncio.sleep(0.5)

            if response.get('deleted', 0) > 0:
                return True
            else:
                return False
            
        return False

    cls.deleteAll = deleteAll

    async def termsAgg(fieldname: str, query: dict = {}):
        buckets = []
        if fieldname in cls.field_names:
            elastic_query = {
                "query": {
                    "bool": {
                        "should": [],
                        "must_not": [],
                        "must": []
                    }
                },
                "aggs": {
                    f"{fieldname}_terms_agg": {
                        "terms": {
                            "field": fieldname,
                            "size": 10_000
                        }
                    }
                }
            }

            for field, value in query.items():
                # =========================================================;
                # =========================================================;
                # =========================================================;
                if field in ["_id", "created_at", "updated_at"]:
                    clause = "must"
                    if type(value) == dict:
                        if "$not" in value:
                            value = value["$not"]
                            clause = "must_not"
                    if type(value) == list:
                        elastic_query["query"]["bool"][clause].append({
                            "terms": value
                        })
                    else:
                        elastic_query["query"]["bool"][clause].append({
                            "term": value
                        })
                elif field in cls.field_names:
                    clause = "must"

                    if type(value) == dict:
                        if "$not" in value:
                            value = value["$not"]
                            clause = "must_not"

                    elastic_query = _resolve_value_by_fieldname(field=field, value=value, elastic_query=elastic_query, clause=clause)

            result = await cls.elastic.search(index=cls.index_name, body=elastic_query)
            buckets = result['aggregations'][f"{fieldname}_terms_agg"]['buckets']

            for i, bucket in enumerate(buckets):
                buckets[i] = {
                    f"{fieldname}": bucket["key"],
                    "doc_count": bucket["doc_count"]
                }

    cls.termsAgg = termsAgg  

    async def save(self):
        params = {}
        source = ""
        for field, value in self.__dict__.items():
            if field in cls.field_names:
                cast_val = value
                if issubclass(type(cls.field_names[field]), bool): # this if HAS to come before the int case, because
                    cast_val = functions.resolve_bool(value)
                elif issubclass(type(cls.field_names[field]), float):
                    cast_val = float(value) 
                elif issubclass(type(cls.field_names[field]), int):
                    cast_val = int(value)
                else:
                    cast_val = str(value)

                params[field] = cast_val
                source += f"ctx._source.{field} = params.{field};"

        # print(f"{C.cyan}[Olastic] Info:{C.end} checking if exists ...")
        doc_exists = await self.elastic.exists(index=cls.index_name, id=self._id)
        # print(f"{C.cyan}[Olastic] Info:{C.end} check result: [{bool(doc_exists)}]")
        if doc_exists:
            # print(f"{C.cyan}[Olastic] Info:{C.end} Updating ...")
            params["updated_at"] = functions.get_current_timestamp()
            source += f"ctx._source.updated_at = params.updated_at;"

            if "_id" in params:
                del params["_id"]
                source = source.replace(f"ctx._source._id = params._id;", "")

            # print(f"{C.cyan}[Olastic] Info:{C.end} removed _id override")

            update_script = {
                "script": {
                    "source": source,
                    "lang": "painless",
                    "params": params
                }
            }

            try:
                response = await cls.elastic.update(index=cls.index_name, id=self._id, body=update_script, request_timeout=200)
            except Exception as e:
                print(f"{C.err}[Olastic] Error:{C.end} Blocked request !!!!")
                print(e)
            if response['result'] == "updated":
                print(f"{C.ok}[Olastic] Info:{C.end} Updated existing document.")
                return self
            else:
                print(f"{C.err}[Olastic] Error:{C.end} Updated existing document.")
                return self
        else:
            response = await cls.elastic.index(index=cls.index_name, id=self._id, body=params)

            # Check the response
            if response['result'] == 'created':
                print(f"{C.ok}[Olastic] Info:{C.end} Document created.")
                return self
            else:
                print(f"{C.err}[Olastic] Error:{C.end} Failed to save document")
                return self

    cls.save = save
    
    return cls
    
class OQueryInterface:
    def sortBy(self, field, asc=True):
        pass
        
    def limit(self, start=0, size=10_000):
        pass

    async def exec(self, asDicts=False):
        pass

    async def count(self):
        pass

    async def update(self, values):
        pass

    async def delete(self):
        pass

    async def aggregate(self, aggs):
        pass

class Schema(Olastic):
    # Schema.__init__ has to be defined in Schema because it has to replace Olastic.__init__
    def __init__(self):
        # self._id = str(functions.uuid_hex())
        pass

    def save(self):
        pass

    @classmethod
    def create(cls, doc_dict, exists=False):
        pass

    @classmethod
    def find(cls, query, start=0, end=10_000) -> OQueryInterface:
        pass

    @classmethod
    def findOne(cls, query, sort_by, asc):
        pass

    @classmethod
    def insert(cls, bulk):
        pass

    @classmethod
    def updateWhere(cls, values, query):
        pass

    @classmethod
    def deleteWhere(cls, query):
        pass

    @classmethod
    def deleteAll(cls):
        pass

    @classmethod
    def termsAgg(cls, fieldname: str):
        pass

class OlasticQuery(OQueryInterface):
    
    def __init__(self, search_query: dict, cls: Schema):
        self.search_query: dict = search_query
        self.schema: Schema = cls

    def sortBy(self, field, asc=True) -> OQueryInterface:
        self.search_query["sort"] = [
            {f"{field}": {"order": "asc" if asc else "desc"}}
        ]
        return self
        
    def limit(self, start=0, size=10_000) -> OQueryInterface:
        self.search_query["from"] = start
        self.search_query["size"] = size
        return self

    async def exec(self, asDicts=False):
        res = await self.schema.elastic.search(index=self.schema.index_name, body=self.search_query)
        hits = []

        # print(json.dumps(res["hits"]["hits"], indent=4))

        if "hits" in res:
            if "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    # print(hit["_id"])
                    # print(json.dumps(hit, indent=4))
                    hit["_source"]["_id"] = hit["_id"]
                    hits.append(hit["_source"])

        arr = []
        if not asDicts:
            for doc in hits:
                arr.append(self.schema.create(doc, True))
        else:
            for doc in hits:
                arr.append(doc)

        return arr
    
    async def count(self):
        del self.search_query["sort"]
        del self.search_query["size"]
        if "aggs" in self.search_query:
            del self.search_query["aggs"]
        if "from" in self.search_query:
            del self.search_query["from"]
        result = await self.schema.elastic.count(index=self.schema.index_name, body=self.search_query)
        return result['count']
    
    async def update(self, values):
        params = {}
        source = ""

        for field, value in values.items():
            if field in self.schema.field_names:
                cast_val = value
                if issubclass(type(self.schema.field_names[field]), bool): # this if HAS to come before the int case, because int is a superset of bool in python
                    cast_val = functions.resolve_bool(value)
                elif issubclass(type(self.schema.field_names[field]), float):
                    cast_val = float(value) 
                elif issubclass(type(self.schema.field_names[field]), int):
                    cast_val = int(value)
                else:
                    cast_val = str(value)

                if field != "_id" and field != "created_at":
                    params[field] = cast_val
                    source += f"ctx._source.{field} = params.{field};"

        params["updated_at"] = functions.get_current_timestamp()
        source += f"ctx._source.updated_at = params.updated_at;"

        self.search_query["script"] = {
            "source": source,
            "lang": "painless",
            "params": params
        }

        response = await self.schema.elastic.update_by_query(index=self.schema.index_name, body=self.search_query)

        await asyncio.sleep(0.4)

        updated = None
        
        if 'updated' in response:
            print(f"{C.ok}[Olastic] Info:{C.end} Updated {response['updated']} documents.")
            updated = response['updated']
        else:
            print(f"{C.err}[Olastic] Error:{C.end} Failed to perform update-by-query operation.")
        
        return updated
    
    async def delete(self):
        response = await self.schema.elastic.delete_by_query(index=self.schema.index_name, body=self.search_query)

        await asyncio.sleep(0.4)

        deleted = None

        if 'deleted' in response:
            print(f"{C.cyan}[Olastic] Info:{C.end} Deleted {response['deleted']} documents.")
            deleted = response['deleted']
        else:
            print(f"{C.err}[Olastic] Error:{C.end} Failed to perform delete-by-query operation.")
        
        return deleted
    
    async def aggregate(self, aggs):
        self.search_query["size"] = 0
        self.search_query["aggs"] = aggs
        result = await self.schema.elastic.search(index=self.schema.index_name, body=self.search_query)
        return result