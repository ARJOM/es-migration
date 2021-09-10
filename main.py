from elasticsearch import Elasticsearch


class Migration:

    origin: Elasticsearch
    destiny: Elasticsearch

    def set_origin(self, origin):
        self.origin = origin

    def set_destiny(self, destiny):
        self.destiny = destiny

    def run(self):
        print("Starting")
        aliases = self.origin.indices.get_alias()
        for alias in aliases:
            self.origin.indices.refresh(index=alias)
            res = self.origin.search(index=alias, body={"query": {"match_all": {}}})
            for hit in res['hits']['hits']:
                self.destiny.index(index=alias, body=hit["_source"])
        print("Finished")


if __name__ == '__main__':
    es = Elasticsearch()
    es2 = Elasticsearch(['localhost:9201'])

    migration = Migration()
    migration.set_origin(es)
    migration.set_destiny(es2)
    migration.run()
